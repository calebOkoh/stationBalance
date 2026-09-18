"""A pure-standard-library scorer for LightGBM's text model format.

Why this exists
---------------
The inference Lambda is drawn as a plain Lambda (drawio `inf`), not a container
image and not a SageMaker endpoint. Honouring that means the function cannot
import lightgbm or numpy: doing so needs either an ECR image or a dependency
layer, and neither appears in the architecture.

It does not need to. `Booster.save_model()` emits a complete, documented text
description of every tree, and scoring a GBDT is tree traversal plus a sum --
a few hundred lines of arithmetic. At ~250 stations x 48 hours = ~12 K rows per
refresh, with the model off the request path entirely (README section 2), plain
Python is comfortably fast enough.

Contract with training
----------------------
pipelines/training/train.py must export with `save_model()` and must NOT use
LightGBM categorical features -- categorical splits are rejected below rather
than silently mis-scored. Categoricals are one-hot or ordinal encoded in
phase 5 instead, which is also what keeps the fitted transformers portable.
"""

from __future__ import annotations

import math

# decision_type is a bitfield in LightGBM's text format.
_MASK_CATEGORICAL = 1
_MASK_DEFAULT_LEFT = 2
_MISSING_TYPE_SHIFT = 2
_MISSING_TYPE_MASK = 3

_MISSING_NONE = 0
_MISSING_ZERO = 1
_MISSING_NAN = 2


class UnsupportedModelError(ValueError):
    """The model uses a feature this scorer deliberately does not implement."""


class Tree:
    __slots__ = ("split_feature", "threshold", "decision_type",
                 "left_child", "right_child", "leaf_value")

    def __init__(self, block: dict[str, list]):
        self.split_feature = [int(x) for x in block.get("split_feature", [])]
        self.threshold = [float(x) for x in block.get("threshold", [])]
        self.decision_type = [int(x) for x in block.get("decision_type", [])]
        self.left_child = [int(x) for x in block.get("left_child", [])]
        self.right_child = [int(x) for x in block.get("right_child", [])]
        self.leaf_value = [float(x) for x in block.get("leaf_value", [])]

        for dt in self.decision_type:
            if dt & _MASK_CATEGORICAL:
                raise UnsupportedModelError(
                    "model contains a categorical split; train with encoded "
                    "features instead (see pipelines.md 5.3)"
                )

    def predict(self, row: list[float]) -> float:
        # A stump has no internal nodes at all -- its single leaf is the whole
        # tree, and traversal would index an empty list.
        if not self.split_feature:
            return self.leaf_value[0] if self.leaf_value else 0.0

        node = 0
        while node >= 0:
            value = row[self.split_feature[node]]
            dt = self.decision_type[node]
            missing_type = (dt >> _MISSING_TYPE_SHIFT) & _MISSING_TYPE_MASK

            if _is_missing(value, missing_type):
                go_left = bool(dt & _MASK_DEFAULT_LEFT)
            else:
                # LightGBM treats a zero as missing when missing_type is Zero,
                # which is why this is not simply `value <= threshold`.
                if missing_type == _MISSING_ZERO and value == 0.0:
                    go_left = bool(dt & _MASK_DEFAULT_LEFT)
                else:
                    go_left = value <= self.threshold[node]

            node = self.left_child[node] if go_left else self.right_child[node]

        # A negative index encodes a leaf: leaf_index = -node - 1.
        return self.leaf_value[-node - 1]


def _is_missing(value: float, missing_type: int) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return missing_type in (_MISSING_NAN, _MISSING_NONE)
    return False


class Booster:
    """A parsed LightGBM text model."""

    def __init__(self, text: str):
        self.feature_names: list[str] = []
        self.objective = "regression"
        self.sigmoid = 1.0
        self.trees: list[Tree] = []
        self._parse(text)

    # -- parsing ----------------------------------------------------------
    def _parse(self, text: str) -> None:
        block: dict[str, list] = {}
        in_tree = False

        for raw in text.splitlines():
            line = raw.strip()

            if line.startswith("Tree="):
                if in_tree and block:
                    self.trees.append(Tree(block))
                block, in_tree = {}, True
                continue

            if line == "end of trees":
                if in_tree and block:
                    self.trees.append(Tree(block))
                in_tree, block = False, {}
                continue

            if not line or "=" not in line:
                continue

            key, _, value = line.partition("=")
            parts = value.split()

            if in_tree:
                block[key] = parts
                continue

            if key == "feature_names":
                self.feature_names = parts
            elif key == "objective":
                # e.g. "binary sigmoid:1" -- the sigmoid parameter matters for
                # turning a raw score into the calibrated probability the web
                # tool displays.
                self.objective = parts[0] if parts else "regression"
                for token in parts[1:]:
                    if token.startswith("sigmoid:"):
                        self.sigmoid = float(token.split(":", 1)[1])

        if in_tree and block:
            self.trees.append(Tree(block))

        if not self.trees:
            raise UnsupportedModelError("no trees parsed from model text")

    # -- scoring ----------------------------------------------------------
    def raw_score(self, row: list[float]) -> float:
        # Leaf values in the text format already carry the learning rate, and
        # boost_from_average is folded into the first tree, so the raw score is
        # a plain sum with no bias term to add.
        return math.fsum(tree.predict(row) for tree in self.trees)

    def predict(self, row: list[float]) -> float:
        score = self.raw_score(row)
        if self.objective.startswith("binary"):
            return 1.0 / (1.0 + math.exp(-self.sigmoid * score))
        return score

    def predict_batch(self, rows: list[list[float]]) -> list[float]:
        return [self.predict(row) for row in rows]

    def vectorize(self, record: dict) -> list[float]:
        """Order a feature dict into the model's own feature order.

        The ordering comes from the model file, not from features.yaml, so a
        feature list that drifts between training and inference produces a
        KeyError here rather than a silently mis-ordered vector -- which is the
        failure mode that looks like a mediocre model instead of a bug.
        """
        return [_as_float(record[name]) for name in self.feature_names]


def _as_float(value) -> float:
    if value is None:
        return float("nan")
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    return float(value)


def load(text: str) -> Booster:
    return Booster(text)
