from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..core.events import EventEmitter


logger = logging.getLogger(__name__)


@dataclass
class CronSchedule:
    cron_expression: str
    event: Optional[str] = None
    options: Optional[dict[str, Any]] = None


CronScheduleInput = str | list[str] | list[CronSchedule | dict[str, Any]]


@dataclass
class CronProxyOptions:
    event: Optional[str] = None
    timezone: Optional[str] = None


class UnsCronProxy:
    def __init__(self, cron_input: CronScheduleInput, options: Optional[CronProxyOptions | dict[str, Any]] = None, *, active: Callable[[], bool] = lambda: True) -> None:
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                'Cron support requires the "cron" extra. Install with `pip install "uns-kit[cron]"`.'
            ) from exc

        self.event = EventEmitter()
        self._scheduler = AsyncIOScheduler()
        self._CronTrigger = CronTrigger
        self._started = False
        self._active = active
        normalized_options = self._normalize_options(options)
        for schedule in self._normalize_schedules(cron_input, normalized_options):
            trigger = self._build_trigger(schedule.cron_expression, schedule.options)
            self._scheduler.add_job(
                self._emit_event,
                trigger=trigger,
                kwargs={
                    "event_name": schedule.event,
                    "cron_expression": schedule.cron_expression,
                },
            )
            logger.info(
                "Created cron job with expression %s%s",
                schedule.cron_expression,
                f" (event: {schedule.event})" if schedule.event else "",
            )

    async def start(self) -> None:
        if self._started:
            return
        self._scheduler.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        self._scheduler.shutdown(wait=False)
        self._started = False

    async def _emit_event(self, *, event_name: Optional[str], cron_expression: str) -> None:
        if not self._active():
            return
        await self.event.emit(
            "cronEvent",
            {
                "event": event_name,
                "cronExpression": cron_expression,
            },
        )

    def _normalize_options(self, options: Optional[CronProxyOptions | dict[str, Any]]) -> CronProxyOptions:
        if isinstance(options, CronProxyOptions):
            return options
        if isinstance(options, dict):
            return CronProxyOptions(
                event=options.get("event"),
                timezone=options.get("timezone"),
            )
        return CronProxyOptions()

    def _normalize_schedules(self, cron_input: CronScheduleInput, options: CronProxyOptions) -> list[CronSchedule]:
        if isinstance(cron_input, str):
            return [CronSchedule(cron_expression=cron_input, event=options.event, options={"timezone": options.timezone})]

        schedules: list[CronSchedule] = []
        for entry in cron_input:
            if isinstance(entry, str):
                schedules.append(CronSchedule(cron_expression=entry, event=options.event, options={"timezone": options.timezone}))
                continue
            if isinstance(entry, CronSchedule):
                schedules.append(entry)
                continue
            schedules.append(
                CronSchedule(
                    cron_expression=str(entry["cron_expression"] if "cron_expression" in entry else entry["cronExpression"]),
                    event=entry.get("event", options.event),
                    options=entry.get("options") or {"timezone": options.timezone},
                )
            )
        return schedules

    def _build_trigger(self, cron_expression: str, options: Optional[dict[str, Any]]) -> Any:
        parts = cron_expression.split()
        if len(parts) not in (5, 6):
            raise ValueError("Cron expression must have 5 or 6 fields.")

        timezone = (options or {}).get("timezone")
        if len(parts) == 5:
            minute, hour, day, month, day_of_week = parts
            return self._CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=timezone,
            )

        second, minute, hour, day, month, day_of_week = parts
        return self._CronTrigger(
            second=second,
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=timezone,
        )
