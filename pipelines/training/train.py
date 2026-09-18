"""Phase 6 -- Train, Validate, Attribute. pipelines.md steps 6.1-6.8.

Runs as a SageMaker script-mode job on one ml.m5.4xlarge (~$0.25/run).

Trains both targets:
  * net_flow regressor   -- Tier 1, the real target. Predicting CHANGE rather
    than level keeps reconstruction error out of the learned weights.
  * is_empty classifier  -- a calibrated probability rather than a point
    estimate, so a downstream consumer can threshold it themselves.

The Tier-2 occupancy labels are NOT externally validated. Checking them would
need a window of recorded live dock counts to compare against, and this project
records none -- it reads historical archives only. Tier 2 therefore ships
diagnostically consistent but without a numeric error bar, and metrics.json
says so explicitly rather than omitting it.

Exports the bundle step 6.8 specifies: model text, features.json, metrics.json,
attribution.json. The bundle is the deliverable -- it is registered in the
SageMaker Model Registry and nothing serves it. A future inference path would
load exactly these; that path is drawn in docs/live_inference.drawio and is not
built.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

TRAIN_DIR = Path(os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train"))
VAL_DIR = Path(os.environ.get("SM_CHANNEL_VALIDATION", "/opt/ml/input/data/validation"))
TEST_DIR = Path(os.environ.get("SM_CHANNEL_TEST", "/opt/ml/input/data/test"))
MODEL_DIR = Path(os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
SOURCE_DIR = Path(os.environ.get("SM_MODULE_DIR", ".")).parent


def load_split(path: Path) -> pd.DataFrame:
    files = sorted(path.glob("**/*.parquet"))
    if not files:
        raise SystemExit(f"no parquet under {path}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def feature_columns(cfg: dict) -> list[str]:
    """Feature ORDER comes from features.yaml and nothing else.

    The scorer in the inference Lambda reads its order from the model file,
    which LightGBM writes from the order it was trained on -- so this list is
    the one place the order is decided, and a mismatch is impossible rather
    than merely unlikely.
    """
    return [name for group in cfg["feature_groups"].values() for name in group]


def baseline_mae(train: pd.DataFrame, test: pd.DataFrame) -> float:
    """6.1 -- mean by (station, hour, weekday).

    A strong, cheap baseline. If the full model cannot beat it, weather and
    closures are contributing nothing and that needs to be known EARLY -- the
    training flow routes this failure back to phase 4, not forward to 6.5.
    """
    key = ["station_id", "hour", "dow"]
    tr = train.assign(hour=train.hour_ts.dt.hour, dow=train.hour_ts.dt.dayofweek)
    te = test.assign(hour=test.hour_ts.dt.hour, dow=test.hour_ts.dt.dayofweek)

    means = tr.groupby(key).net_flow.mean().rename("pred").reset_index()
    joined = te.merge(means, on=key, how="left")
    joined["pred"] = joined["pred"].fillna(tr.net_flow.mean())

    return float(np.abs(joined.net_flow - joined.pred).mean())


def train_regressor(train, val, features, params) -> lgb.Booster:
    """6.2 -- the Tier-1 target."""
    ds_train = lgb.Dataset(train[features], label=train.net_flow)
    ds_val = lgb.Dataset(val[features], label=val.net_flow, reference=ds_train)

    return lgb.train(
        {**params, "objective": "regression_l1", "metric": "l1"},
        ds_train,
        num_boost_round=2000,
        valid_sets=[ds_val],
        # 6.4 -- tuned on VAL only. Test stays sealed until 6.5.
        callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(200)],
    )


def train_classifier(train, val, features, params, scale_pos_weight) -> lgb.Booster:
    """6.3 -- calibrated probability of an empty dock."""
    ds_train = lgb.Dataset(train[features], label=train.is_empty.astype(int))
    ds_val = lgb.Dataset(val[features], label=val.is_empty.astype(int),
                         reference=ds_train)

    return lgb.train(
        {**params, "objective": "binary", "metric": ["binary_logloss", "auc"],
         "scale_pos_weight": scale_pos_weight},
        ds_train,
        num_boost_round=2000,
        valid_sets=[ds_val],
        callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(200)],
    )


def attribute(model, sample: pd.DataFrame, features: list[str], cfg: dict) -> dict:
    """6.7 -- SHAP per FACTOR GROUP.

    "The research question is about the effect of weather, time, and closures --
    not only prediction accuracy. Attribution is a deliverable, not a
    nice-to-have" (pipelines.md 6.7). Reported by group because the groups are
    the hypothesis; per-feature values are kept underneath for inspection.
    """
    import shap

    explainer = shap.TreeExplainer(model)
    values = explainer.shap_values(sample[features])
    if isinstance(values, list):
        values = values[-1]

    per_feature = dict(zip(features, np.abs(values).mean(axis=0)))
    by_group = {
        group: float(sum(per_feature.get(f, 0.0) for f in names))
        for group, names in cfg["feature_groups"].items()
    }
    total = sum(by_group.values()) or 1.0

    return {
        "mean_abs_shap_by_group": {k: round(v, 6) for k, v in by_group.items()},
        "share_of_total_effect": {k: round(v / total, 4) for k, v in by_group.items()},
        "mean_abs_shap_by_feature": {
            k: round(float(v), 6)
            for k, v in sorted(per_feature.items(), key=lambda kv: -kv[1])
        },
        "n_sampled_rows": int(len(sample)),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--num-leaves", type=int, default=63)
    ap.add_argument("--min-data-in-leaf", type=int, default=200)
    ap.add_argument("--shap-sample", type=int, default=20000)
    args, _ = ap.parse_known_args()

    cfg = json.loads((SOURCE_DIR / "features.json").read_text())
    features = feature_columns(cfg)

    train = load_split(TRAIN_DIR)
    val = load_split(VAL_DIR)
    test = load_split(TEST_DIR)
    print(f"rows: train={len(train):,} val={len(val):,} test={len(test):,}")

    missing = [f for f in features if f not in train.columns]
    if missing:
        raise SystemExit(f"features.yaml declares columns absent from the table: {missing}")

    params = {
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "min_data_in_leaf": args.min_data_in_leaf,
        "verbosity": -1,
        "num_threads": os.cpu_count(),
        # No categorical_feature anywhere: the inference scorer rejects
        # categorical splits by design, so this is the contract that keeps the
        # exported model loadable.
        "feature_pre_filter": False,
    }

    pos = int(train.is_empty.sum())
    scale_pos_weight = (len(train) - pos) / pos if pos else 1.0

    print("\n=== 6.1 baseline ===")
    base_mae = baseline_mae(train, test)
    print(f"  (station, hour, weekday) mean MAE on test: {base_mae:.4f}")

    print("\n=== 6.2 net_flow regressor ===")
    reg = train_regressor(train, val, features, params)

    print("\n=== 6.3 is_empty classifier ===")
    clf = train_classifier(train, val, features, params, scale_pos_weight)

    # 6.5 -- evaluate on TEST, once, at the end. These are the only numbers
    # reported.
    print("\n=== 6.5 test evaluation (once) ===")
    from sklearn.metrics import (average_precision_score, brier_score_loss,
                                 mean_absolute_error, mean_squared_error)

    pred_flow = reg.predict(test[features], num_iteration=reg.best_iteration)
    pred_empty = clf.predict(test[features], num_iteration=clf.best_iteration)

    mae = float(mean_absolute_error(test.net_flow, pred_flow))
    rmse = float(np.sqrt(mean_squared_error(test.net_flow, pred_flow)))
    pr_auc = float(average_precision_score(test.is_empty.astype(int), pred_empty))
    brier = float(brier_score_loss(test.is_empty.astype(int), pred_empty))

    beats_baseline = mae < base_mae
    print(f"  net_flow  MAE={mae:.4f}  RMSE={rmse:.4f}  (baseline MAE {base_mae:.4f})")
    print(f"  is_empty  PR-AUC={pr_auc:.4f}  Brier={brier:.4f}")
    print(f"  beats baseline: {beats_baseline}")

    if not beats_baseline:
        # Not a crash: the run is still informative and the artifacts are still
        # worth inspecting. But this is the 6.1 gate failing, and it routes back
        # to phase 4 -- the weather and closure arms may be contributing
        # nothing.
        print("  !! GATE FAILED (pipelines.md 6.1). Revisit phase 4 feature "
              "engineering before publishing this model.")

    print("\n=== 6.7 SHAP attribution ===")
    sample = test.sample(min(args.shap_sample, len(test)), random_state=0)
    attribution = attribute(reg, sample, features, cfg)
    for group, share in attribution["share_of_total_effect"].items():
        print(f"  {group:<15} {100 * share:5.1f}% of total effect")

    # 6.8 -- export the bundle. Model, config and transformers versioned
    # together, since a model and its preprocessing are one unit.
    print("\n=== 6.8 export ===")
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    reg.save_model(str(MODEL_DIR / "model_net_flow.txt"),
                   num_iteration=reg.best_iteration)
    clf.save_model(str(MODEL_DIR / "model_is_empty.txt"),
                   num_iteration=clf.best_iteration)
    shutil.copy(SOURCE_DIR / "features.json", MODEL_DIR / "features.json")


    metrics = {
        "trained_at": pd.Timestamp.utcnow().isoformat(),
        "rows": {"train": len(train), "val": len(val), "test": len(test)},
        "baseline": {"station_hour_weekday_mae": round(base_mae, 6)},
        "net_flow": {"mae": round(mae, 6), "rmse": round(rmse, 6),
                     "best_iteration": reg.best_iteration},
        "is_empty": {"pr_auc": round(pr_auc, 6), "brier": round(brier, 6),
                     "scale_pos_weight": round(scale_pos_weight, 2),
                     "best_iteration": clf.best_iteration},
        "gates": {"beats_baseline_6_1": bool(beats_baseline)},
        "caveats": [
            "Tier-2 occupancy is diagnostically consistent but NOT externally "
            "validated: no recorded dock counts exist to check it against. It "
            "ships without a numeric error bar.",
            "Gap thresholds 6h / 72h / 30d are stated priors, not findings.",
            "The closure arm is ABSENT, not zero: PGW publishes closures as "
            "text addresses with no coordinates and the resolver does not "
            "exist. This model measures weather and time only.",
            "Occupancy level is a lower bound -- O(s,0) is the minimum value "
            "keeping occupancy non-negative, since no historical capacity is "
            "published. The series shape, which the features predict, is "
            "unaffected.",
        ],
    }
    (MODEL_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2))
    (MODEL_DIR / "attribution.json").write_text(json.dumps({
        **attribution,
        "trained_at": metrics["trained_at"],
        "note": "Mean absolute SHAP per factor group on a random test sample. "
                "This is the research deliverable (pipelines.md 6.7).",
    }, indent=2))

    print(f"  bundle written to {MODEL_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
