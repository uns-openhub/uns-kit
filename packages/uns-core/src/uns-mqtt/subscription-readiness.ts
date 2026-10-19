/** Transport evidence only. No extra subscriptions, topics or credentials are published. */
export class SubscriptionReadiness {
  private connected = false;
  private sequence = 0;
  private filters = new Map<string, {revision: number; acknowledged: boolean}>();
  private pending = new Map<number, {remove: boolean; filters: Array<{topic: string; revision: number}>}>();

  connection(connected: boolean): void {
    this.connected = connected;
    if (!connected) { this.filters.clear(); this.pending.clear(); }
  }

  sent(packet: {cmd: string; messageId?: number; subscriptions?: Array<{topic: string}>; unsubscriptions?: string[]}): void {
    if (!['subscribe', 'unsubscribe'].includes(packet.cmd) || !Number.isInteger(packet.messageId)) return;
    const topics = packet.cmd === 'subscribe' ? packet.subscriptions?.map(s => s.topic) : packet.unsubscriptions;
    if (!topics?.length) return;
    const filters = topics.map(topic => {
      const revision = ++this.sequence;
      this.filters.set(topic, {revision, acknowledged: false});
      return {topic, revision};
    });
    this.pending.set(packet.messageId!, {remove: packet.cmd === 'unsubscribe', filters});
  }

  received(packet: {cmd: string; messageId?: number; granted?: number[]}): void {
    if (!['suback', 'unsuback'].includes(packet.cmd) || packet.messageId === undefined) return;
    const pending = this.pending.get(packet.messageId);
    if (!pending || pending.remove !== (packet.cmd === 'unsuback')) return;
    this.pending.delete(packet.messageId);
    pending.filters.forEach(({topic, revision}, index) => {
      if (this.filters.get(topic)?.revision !== revision) return;
      const grant = packet.granted?.[index];
      const accepted = pending.remove ? grant === undefined || grant === 0 || grant === 17 : grant === 0 || grant === 1 || grant === 2;
      if (pending.remove && accepted) this.filters.delete(topic);
      else this.filters.set(topic, {revision, acknowledged: accepted && !pending.remove});
    });
  }

  ready(): boolean {
    return this.connected && this.filters.size > 0 && this.pending.size === 0
      && [...this.filters.values()].every(filter => filter.acknowledged);
  }
}
