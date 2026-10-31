import cron, { ScheduledTask, TaskOptions } from "node-cron";
import UnsProxy from "@uns-kit/core/uns/uns-proxy.js";

export type CronSchedule = {
  cronExpression: string;
  event?: string;
  options?: TaskOptions;
};

export type CronScheduleInput = string | string[] | CronSchedule[];

export type CronProxyOptions = TaskOptions & {
  event?: string;
};

type NormalizedCronSchedule = {
  cronExpression: string;
  event?: string;
  options?: TaskOptions;
};

const normalizeTaskOptions = (options?: CronProxyOptions): TaskOptions | undefined => {
  if (!options) {
    return undefined;
  }
  const { event: _event, ...taskOptions } = options;
  return Object.keys(taskOptions).length > 0 ? (taskOptions as TaskOptions) : undefined;
};

const normalizeCronSchedules = (
  input: CronScheduleInput,
  options?: CronProxyOptions,
): NormalizedCronSchedule[] => {
  const defaultEvent = options?.event;
  const defaultOptions = normalizeTaskOptions(options);

  const toSchedule = (
    cronExpression: string,
    event?: string,
    scheduleOptions?: TaskOptions,
  ): NormalizedCronSchedule => ({
    cronExpression,
    event,
    options: scheduleOptions,
  });

  if (typeof input === "string") {
    return [toSchedule(input, defaultEvent, defaultOptions)];
  }

  return input.map((entry) => {
    if (typeof entry === "string") {
      return toSchedule(entry, defaultEvent, defaultOptions);
    }

    return toSchedule(
      entry.cronExpression,
      entry.event ?? defaultEvent,
      entry.options ?? defaultOptions,
    );
  });
};

export default class UnsCronProxy extends UnsProxy {
  private readonly tasks: ScheduledTask[];

  constructor(cronInput: CronScheduleInput, options?: CronProxyOptions, private readonly active: () => boolean = () => true) {
    super();
    const schedules = normalizeCronSchedules(cronInput, options);
    this.tasks = schedules.map((schedule) =>
      cron.schedule(
        schedule.cronExpression,
        () => {
          if (!this.active()) return;
          this.event.emit("cronEvent", {
            event: schedule.event,
            cronExpression: schedule.cronExpression,
          });
        },
        schedule.options,
      ),
    );
  }

  public override async stop(): Promise<void> {
    await super.stop();
    this.tasks.forEach((task) => task.stop());
  }
}
