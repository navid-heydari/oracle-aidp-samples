import pytest

from ocidi2aidp.compile.schedule import convert


def sched(fd, tz="UTC", **kw):
    return {"name": "S", "timezone": tz, "frequencyDetails": fd, **kw}


@pytest.mark.parametrize("fd,cron", [
    ({"modelType": "DAILY", "interval": 1, "time": {"hour": 2, "minute": 30}}, "0 30 2 * * ?"),
    ({"modelType": "HOURLY", "interval": 1, "time": {"minute": 5}}, "0 5 * * * ?"),
    ({"modelType": "HOURLY", "interval": 6, "time": {"minute": 0}}, "0 0 0/6 * * ?"),
    ({"modelType": "WEEKLY", "days": ["MONDAY", "FRIDAY"], "time": {"hour": 7}}, "0 0 7 ? * MON,FRI"),
    ({"modelType": "MONTHLY", "interval": 1, "days": [1, 15], "time": {"hour": 1}}, "0 0 1 1,15 * ?"),
    ({"modelType": "MONTHLY", "interval": 1, "days": ["LAST"], "time": {"hour": 1}}, "0 0 1 L * ?"),
    ({"modelType": "MONTHLY_RULE", "interval": 1, "weekOfMonth": "SECOND", "dayOfWeek": "TUESDAY",
      "time": {"hour": 9}}, "0 0 9 ? * TUE#2"),
    ({"modelType": "MONTHLY_RULE", "interval": 1, "weekOfMonth": "LAST", "dayOfWeek": "FRIDAY",
      "time": {"hour": 23}}, "0 0 23 ? * 6L"),
])
def test_exact_conversions(fd, cron):
    res = convert(sched(fd))
    assert res.schedule == {"quartzCronExpression": cron, "timezoneId": "UTC",
                            "pauseStatus": "PAUSED"}


@pytest.mark.parametrize("expr,cron", [
    ("0 6 * * 1", "0 0 6 ? * MON"),          # OCI-DI 1 = Monday, not Sunday
    ("30 1 * * 7", "0 30 1 ? * SUN"),
    ("0 6 * * 1-5", "0 0 6 ? * MON-FRI"),
    ("0 6 * * 6-1", "0 0 6 ? * SAT,SUN,MON"),  # wrap-around range
    ("15 */2 1 * *", "0 15 */2 1 * ?"),
])
def test_custom_cron_day_of_week_is_monday_first(expr, cron):
    assert convert(sched({"modelType": "CUSTOM", "customExpression": expr})).schedule[
        "quartzCronExpression"] == cron


@pytest.mark.parametrize("fd", [
    {"modelType": "DAILY", "interval": 3, "time": {"hour": 1}},
    {"modelType": "HOURLY", "interval": 7},
    {"modelType": "MONTHLY", "interval": 2, "days": [1]},
    {"modelType": "CUSTOM", "customExpression": "0 6 1 * 1"},       # dom AND dow
    {"modelType": "CUSTOM", "customExpression": "0 6 * * */2"},     # dow step
    {"modelType": "CUSTOM", "customExpression": "0 0 6 * * ?"},     # not 5 fields
    {"modelType": "SOMETHING"},
])
def test_no_exact_form_means_no_schedule(fd):
    res = convert(sched(fd))
    assert res.schedule is None
    assert any(f.code == "SC01_NO_EXACT_CRON" for f in res.findings)


def test_timezone_validated_and_dst_flagged():
    res = convert(sched({"modelType": "DAILY", "interval": 1, "time": {"hour": 1}},
                        tz="Mars/Olympus", isDaylightAdjustmentEnabled=False),
                  default_timezone="Europe/Paris")
    assert res.schedule["timezoneId"] == "Europe/Paris"
    assert {f.code for f in res.findings} >= {"SC05_TIMEZONE", "SC06_DST"}
