import { describe, expect, it } from 'vitest';
import App from '../packages/uns-api/src/app.js';

describe('real HTTP ownership and accepted requests', () => {
  it('rejects standby requests and finishes an accepted request before shutdown', async () => {
    const app = new App(0, 'example', 'api');
    let owned = false, release!: () => void, entered!: () => void;
    const requestEntered = new Promise<void>(r => { entered = r; });
    const pending = new Promise<void>(r => { release = r; });
    app.setOwnershipGate(() => owned);
    app.router.get('/work', async (_req, res) => { entered(); await pending; res.json({ done: true }); });
    await app.start();
    if (!app.server.listening) await new Promise<void>(r => app.server.once('listening', r));
    const address = app.server.address() as { port: number };
    const url = `http://127.0.0.1:${address.port}/api/work`;
    try {
      expect((await fetch(url)).status).toBe(503);
      owned = true;
      const response = fetch(url); await requestEntered;
      owned = false;
      let drained = false;
      const drain = app.drainRequests().then(() => { drained = true; });
      expect((await fetch(url)).status).toBe(503);
      expect(drained).toBe(false);
      release(); expect(await (await response).json()).toEqual({ done: true });
      await drain; expect(drained).toBe(true);
    } finally { release(); await app.stop(); }
  });
});
