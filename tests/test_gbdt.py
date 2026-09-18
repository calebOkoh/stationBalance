"""Scorer tests.

lightgbm is not installed here (and is not a runtime dependency of anything
deployed), so these assert against hand-written model text whose expected
output is derivable by hand. That is the point: the scorer's contract is the
LightGBM text FORMAT, not the lightgbm package.
"""

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "lambdas" / "inference"))

import gbdt  # noqa: E402

# Two features, two trees. Tree 0 splits on feature 0 at 1.5; tree 1 splits on
# feature 1 at 0.5. decision_type=2 is default_left with missing_type None.
SIMPLE_MODEL = """tree
version=v3
num_class=1
num_tree_per_iteration=1
label_index=0
max_feature_idx=1
objective=regression
feature_names=occupancy_lag_1h temperature_2m

Tree=0
num_leaves=2
num_cat=0
split_feature=0
threshold=1.5
decision_type=2
left_child=-1
right_child=-2
leaf_value=0.25 -0.75

Tree=1
num_leaves=2
num_cat=0
split_feature=1
threshold=0.5
decision_type=2
left_child=-1
right_child=-2
leaf_value=1 2

end of trees
"""


def test_parses_metadata():
    booster = gbdt.load(SIMPLE_MODEL)
    assert booster.feature_names == ["occupancy_lag_1h", "temperature_2m"]
    assert booster.objective == "regression"
    assert len(booster.trees) == 2


def test_traversal_takes_left_on_less_or_equal():
    booster = gbdt.load(SIMPLE_MODEL)
    # f0=1.0 <= 1.5 -> 0.25 ; f1=0.0 <= 0.5 -> 1.0
    assert booster.predict([1.0, 0.0]) == 1.25


def test_traversal_takes_right_above_threshold():
    booster = gbdt.load(SIMPLE_MODEL)
    # f0=9.0 > 1.5 -> -0.75 ; f1=9.0 > 0.5 -> 2.0
    assert booster.predict([9.0, 9.0]) == 1.25
    # and the two branches are genuinely different
    assert booster.predict([1.0, 9.0]) == 2.25


def test_threshold_boundary_goes_left():
    booster = gbdt.load(SIMPLE_MODEL)
    # LightGBM's numerical split is `value <= threshold`, so the boundary is
    # inclusive on the left. Getting this backwards shifts every prediction at
    # an exact threshold and is invisible in aggregate metrics.
    assert booster.predict([1.5, 0.5]) == 1.25


def test_missing_follows_default_left():
    booster = gbdt.load(SIMPLE_MODEL)
    nan = float("nan")
    # default_left is set (decision_type=2), so NaN goes left on both trees.
    assert booster.predict([nan, nan]) == 1.25


def test_binary_objective_applies_sigmoid():
    text = SIMPLE_MODEL.replace("objective=regression", "objective=binary sigmoid:1")
    booster = gbdt.load(text)
    raw = booster.raw_score([1.0, 0.0])
    assert math.isclose(booster.predict([1.0, 0.0]), 1 / (1 + math.exp(-raw)))
    assert 0.0 < booster.predict([1.0, 0.0]) < 1.0


def test_vectorize_uses_model_feature_order():
    booster = gbdt.load(SIMPLE_MODEL)
    # Deliberately supplied in the wrong order -- the model's order must win.
    row = booster.vectorize({"temperature_2m": 9.0, "occupancy_lag_1h": 1.0})
    assert row == [1.0, 9.0]


def test_vectorize_raises_on_missing_feature():
    booster = gbdt.load(SIMPLE_MODEL)
    try:
        booster.vectorize({"temperature_2m": 9.0})
    except KeyError:
        return
    raise AssertionError("a feature missing at serve time must raise, not default")


def test_categorical_split_is_rejected():
    text = SIMPLE_MODEL.replace("decision_type=2\nleft_child=-1\nright_child=-2\nleaf_value=0.25 -0.75",
                                "decision_type=1\nleft_child=-1\nright_child=-2\nleaf_value=0.25 -0.75")
    try:
        gbdt.load(text)
    except gbdt.UnsupportedModelError:
        return
    raise AssertionError("categorical splits must be rejected, not mis-scored")


def test_deeper_tree_traversal():
    # One internal root plus one internal child, to exercise a non-trivial walk.
    text = """tree
num_class=1
objective=regression
feature_names=a b

Tree=0
num_leaves=3
num_cat=0
split_feature=0 1
threshold=1.5 0.5
decision_type=2 2
left_child=1 -1
right_child=-3 -2
leaf_value=10 20 30

end of trees
"""
    booster = gbdt.load(text)
    assert booster.predict([1.0, 0.0]) == 10.0   # root left, child left  -> leaf 0
    assert booster.predict([1.0, 9.0]) == 20.0   # root left, child right -> leaf 1
    assert booster.predict([9.0, 0.0]) == 30.0   # root right             -> leaf 2


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {exc}")
    raise SystemExit(1 if failures else 0)
