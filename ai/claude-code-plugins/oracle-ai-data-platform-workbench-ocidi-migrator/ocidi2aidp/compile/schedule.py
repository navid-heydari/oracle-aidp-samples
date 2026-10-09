"""OCI-DI schedule -> AIDP job schedule (Quartz cron + IANA timezone).

The AIDP job body takes ``schedule: {quartzCronExpression, timezoneId,
pauseStatus}`` (keys verified live by the Informatica migrator, 2026-09-25).

Rules, in the order they bite:

* Only a schedule with an *exact* Quartz form is emitted. "Every 5 days" or
  "every 7 hours" has none (Quartz steps restart each month / day), and a
  plausible approximation is worse than no schedule: an unscheduled job is
  obviously unscheduled, a wrong cron is not. Those return ``None`` and a
  finding that quotes the source.
* OCI-DI's CUSTOM cron is five fields with day-of-week **1-7 = Monday-Sunday**
  (docs: create-schedule-cron.htm). Quartz numbers 1-7 from **Sunday**. Days
  are therefore always emitted as names (``MON``), never numbers.
* Every schedule is created ``PAUSED``. A migrated job starting to fire the
  moment it is published is an outward-facing side effect nobody asked for.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..report import Finding

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # pragma: no cover - Python < 3.9
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

_DAY = {"SUNDAY": "SUN", "MONDAY": "MON", "TUESDAY": "TUE", "WEDNESDAY": "WED",
        "THURSDAY": "THU", "FRIDAY": "FRI", "SATURDAY": "SAT"}
_QUARTZ_DOW_NUM = {"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}
# OCI-DI custom cron day-of-week numbers (Monday first).
_DI_DOW = {1: "MON", 2: "TUE", 3: "WED", 4: "THU", 5: "FRI", 6: "SAT", 7: "SUN", 0: "SUN"}
_WEEK = {"FIRST": "#1", "SECOND": "#2", "THIRD": "#3", "FOURTH": "#4", "FIFTH": "#5"}


@dataclass
class ScheduleResult:
    schedule: Optional[dict]
    findings: list = field(default_factory=list)
    described: str = ""


def _time(fd: dict):
    t = fd.get("time") or {}
    return int(t.get("second") or 0), int(t.get("minute") or 0), int(t.get("hour") or 0)


def _tz(name: str, default: str, findings: list, label: str) -> str:
    if name and ZoneInfo is not None:
        try:
            ZoneInfo(name)
            return name
        except (ZoneInfoNotFoundError, ValueError):
            findings.append(Finding("SC05_TIMEZONE", f"{label}: timezone {name!r} is not an IANA "
                                    f"zone AIDP accepts; using {default or 'UTC'}", "review"))
            return default or "UTC"
    if not name:
        findings.append(Finding("SC05_TIMEZONE", f"{label}: no timezone on the schedule; using "
                                f"{default or 'UTC'}", "assume"))
    return name or default or "UTC"


def describe(schedule: dict) -> str:
    fd = schedule.get("frequencyDetails") or {}
    bits = [fd.get("modelType") or fd.get("frequency") or "?"]
    for k in ("interval", "days", "weekOfMonth", "dayOfWeek", "customExpression"):
        if fd.get(k) not in (None, "", []):
            bits.append(f"{k}={fd[k]}")
    if fd.get("time"):
        s, m, h = _time(fd)
        bits.append(f"at {h:02d}:{m:02d}:{s:02d}")
    bits.append(f"tz={schedule.get('timezone') or '?'}")
    return " ".join(str(b) for b in bits)


def convert(schedule: dict, *, default_timezone: str = "") -> ScheduleResult:
    label = schedule.get("name") or schedule.get("key") or "schedule"
    findings: list = []
    fd = schedule.get("frequencyDetails") or {}
    kind = (fd.get("modelType") or fd.get("frequency") or "").upper()
    described = describe(schedule)
    cron = None
    try:
        cron = _cron(kind, fd, label, findings)
    except ValueError as exc:
        findings.append(Finding("SC01_NO_EXACT_CRON", f"{label} ({described}): {exc}; the job is "
                                f"created with no schedule -- set it by hand", "review"))
    if cron is None:
        return ScheduleResult(None, findings, described)
    tz = _tz(schedule.get("timezone") or "", default_timezone, findings, label)
    if schedule.get("isDaylightAdjustmentEnabled") is False:
        findings.append(Finding("SC06_DST", f"{label}: OCI-DI had daylight-saving adjustment "
                                f"off; AIDP follows the IANA zone {tz}, so runs move by an hour "
                                f"across DST changes -- use a fixed-offset zone if that matters",
                                "review"))
    return ScheduleResult({"quartzCronExpression": cron, "timezoneId": tz,
                           "pauseStatus": "PAUSED"}, findings, described)


def _cron(kind: str, fd: dict, label: str, findings: list) -> Optional[str]:
    s, m, h = _time(fd)
    interval = int(fd.get("interval") or 1)
    if kind == "HOURLY":
        if interval == 1:
            return f"{s} {m} * * * ?"
        if interval < 1 or 24 % interval:
            raise ValueError(f"every {interval} hours has no exact Quartz form")
        findings.append(Finding("SC02_ANCHOR", f"{label}: every {interval} hours is anchored at "
                                f"00:{m:02d} on AIDP; confirm OCI-DI used the same anchor",
                                "assume"))
        return f"{s} {m} 0/{interval} * * ?"
    if kind == "DAILY":
        if interval != 1:
            raise ValueError(f"every {interval} days has no exact Quartz form")
        return f"{s} {m} {h} * * ?"
    if kind == "WEEKLY":
        days = [_DAY.get(str(d).upper()) for d in fd.get("days") or []]
        if not days or None in days:
            raise ValueError(f"weekly days {fd.get('days')!r} are not day names")
        return f"{s} {m} {h} ? * {','.join(days)}"
    if kind == "MONTHLY_RULE" or (kind == "MONTHLY" and fd.get("weekOfMonth")):
        if interval != 1:
            raise ValueError(f"every {interval} months has no exact Quartz form")
        day = _DAY.get(str(fd.get("dayOfWeek") or "").upper())
        week = str(fd.get("weekOfMonth") or "").upper()
        if not day or (week not in _WEEK and week != "LAST"):
            raise ValueError("monthly rule without a day and week")
        dow = f"{_QUARTZ_DOW_NUM[day]}L" if week == "LAST" else f"{day}{_WEEK[week]}"
        return f"{s} {m} {h} ? * {dow}"
    if kind == "MONTHLY":
        if interval != 1:
            raise ValueError(f"every {interval} months has no exact Quartz form")
        days = []
        for d in fd.get("days") or []:
            if str(d).upper() in ("LAST", "-1"):
                days.append("L")
            elif str(d).isdigit() and 1 <= int(d) <= 31:
                days.append(str(int(d)))
            else:
                raise ValueError(f"monthly day {d!r} is not a day of the month")
        if not days:
            raise ValueError("monthly schedule with no days")
        if "L" in days and len(days) > 1:
            raise ValueError("Quartz cannot combine the last day with other days")
        return f"{s} {m} {h} {','.join(days)} * ?"
    if kind == "CUSTOM":
        return _custom(str(fd.get("customExpression") or ""), label, findings)
    raise ValueError(f"frequency {kind or '(none)'} is not known")


def _dow_token(tok: str) -> str:
    """One OCI-DI day-of-week list item -> Quartz names. Raises on a step."""
    if "/" in tok:
        raise ValueError(f"day-of-week step {tok!r} counts from a different first day in Quartz")
    if "-" in tok:
        a, b = tok.split("-", 1)
        na, nb = _dow_num(a), _dow_num(b)
        if na <= nb:
            return f"{_DI_DOW[na]}-{_DI_DOW[nb]}"
        return ",".join(_DI_DOW[d] for d in list(range(na, 8)) + list(range(1, nb + 1)))
    return _DI_DOW[_dow_num(tok)]


def _dow_num(tok: str) -> int:
    t = tok.strip().upper()
    names = {v: k for k, v in _DI_DOW.items() if k}
    if t[:3] in names:
        return names[t[:3]]
    if t.isdigit() and 0 <= int(t) <= 7:
        return int(t) or 7
    raise ValueError(f"day-of-week {tok!r} is not a day")


def _custom(expr: str, label: str, findings: list) -> Optional[str]:
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError(f"custom expression {expr!r} is not the five-field OCI-DI cron")
    minute, hour, dom, month, dow = fields
    for name, value in (("minute", minute), ("hour", hour), ("day-of-month", dom),
                        ("month", month)):
        if any(c not in "0123456789*,-/LW?" for c in value.upper()):
            raise ValueError(f"{name} field {value!r} is not plain cron")
    dom_any = dom in ("*", "?")
    dow_any = dow in ("*", "?")
    if not dom_any and not dow_any:
        raise ValueError("cron day-of-month and day-of-week are both set (OR semantics); "
                         "Quartz needs one of them to be '?'")
    q_dow = "?" if dow_any else ",".join(_dow_token(t) for t in dow.split(","))
    q_dom = "?" if not dow_any else ("*" if dom_any else dom)
    if not dow_any:
        findings.append(Finding("SC03_DOW", f"{label}: OCI-DI day-of-week {dow!r} (1 = Monday) "
                                f"-> Quartz {q_dow}", "info"))
    return f"0 {minute} {hour} {q_dom} {month} {q_dow}"
