/** Accepted application work is separate from transport queues. Errors stay latched. */
export class ProcessWork {
  private pending = new Set<Promise<unknown>>();
  private failed = false;
  private paused = false;

  constructor(private readonly active: () => boolean = () => true) {}

  /** Track work already admitted by a transport. Never drop it during release. */
  track<T>(work: Promise<T>): Promise<T> {
    this.pending.add(work);
    void work.then(() => this.pending.delete(work), () => {
      this.failed = true;
      this.pending.delete(work);
    });
    return work;
  }

  /** Timers/pollers must enter through this ownership gate. */
  run<T>(work: () => Promise<T>): Promise<T | undefined> {
    if (this.paused || !this.active()) return Promise.resolve(undefined);
    return this.track(Promise.resolve().then(work));
  }

  pause(): void { this.paused = true; }
  fail(): void { this.failed = true; }
  async drain(): Promise<void> {
    while (this.pending.size) await Promise.allSettled([...this.pending]);
    if (this.failed) throw new Error("Accepted application work is unconfirmed");
  }
}
