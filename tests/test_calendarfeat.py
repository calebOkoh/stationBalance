import math
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines"))

import calendarfeat as cf  # noqa: E402


def test_fixed_holidays_2026():
    hol = cf.us_federal_holidays(2026)
    assert date(2026, 7, 4) in hol          # Independence Day (a Saturday)
    assert date(2026, 7, 3) in hol          # ...observed on the Friday
    assert date(2026, 12, 25) in hol
    assert date(2026, 6, 19) in hol         # Juneteenth


def test_floating_holidays_2026():
    hol = cf.us_federal_holidays(2026)
    assert date(2026, 1, 19) in hol         # MLK, 3rd Monday
    assert date(2026, 5, 25) in hol         # Memorial, last Monday of May
    assert date(2026, 9, 7) in hol          # Labor Day
    assert date(2026, 11, 26) in hol        # Thanksgiving, 4th Thursday


def test_last_monday_of_may_when_may_ends_on_a_monday():
    # 2021-05-31 was a Monday -- the "last Monday" edge case.
    assert date(2021, 5, 31) in cf.us_federal_holidays(2021)


def test_ordinary_day_is_not_a_holiday():
    assert not cf.is_holiday(date(2026, 3, 17))


def test_cyclic_encoding_wraps():
    h23 = cf.calendar_features(datetime(2026, 3, 17, 23))
    h00 = cf.calendar_features(datetime(2026, 3, 18, 0))
    # Adjacent hours must be adjacent in the encoding. Raw integers would put
    # these 23 apart, which is the whole reason for sin/cos.
    dist = math.hypot(h23["hour_sin"] - h00["hour_sin"],
                      h23["hour_cos"] - h00["hour_cos"])
    assert dist < 0.3


def test_weekend_flag():
    assert cf.calendar_features(datetime(2026, 3, 21, 12))["is_weekend"] == 1.0   # Saturday
    assert cf.calendar_features(datetime(2026, 3, 17, 12))["is_weekend"] == 0.0   # Tuesday


def test_every_declared_feature_is_produced():
    feats = cf.calendar_features(datetime(2026, 3, 17, 9))
    assert set(feats) == set(cf.FEATURE_NAMES)


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
