import { AuthClient, ConfigFile, ServiceTokenProvider, UnsClient } from "../index.js";

async function main(): Promise<void> {
  const config = await ConfigFile.loadConfig();
  const tokenProvider = new ServiceTokenProvider({
    configToken: typeof config.uns.token === "string" ? config.uns.token : undefined,
    fallback: await AuthClient.create(),
  });
  const client = new UnsClient(config.uns.rest, {
    tokenProvider,
  });

  const valueTopic = "sij/acroni/vv/hrm-furnace/equipment/pusher/output-quantity";
  const tableTopic = "sij/acroni/vv/hrm-furnace/process-segment/slab-001/trend-data";

  const to = new Date();
  const from = new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

  const lastValue = await client.lastValue(valueTopic);
  console.log("lastValue:");
  console.dir(lastValue, { depth: null });
  console.log();

  const data = await client.getAttributeData(tableTopic, {
    from: from.toISOString(),
    to: to.toISOString(),
    table: "uns_hrm_furnace_trend_data_table",
    dedupe: false,
    summaryOnly: false,
  });
  console.log("getData:");
  if (data === null) {
    console.log(null);
  } else {
    console.dir(data.toRecords().slice(0, 3), { depth: null });
  }
  console.log();

  const customData = await client.getData("/projects/project-name/path-to-data/data", {
    fromDate: "20260325",
  });
  console.log("getData:");
  console.dir(await customData.json(), { depth: null });
  console.log();

  const history = await client.history([valueTopic, tableTopic], {
    from: from.toISOString(),
    to: to.toISOString(),
    limit: 500,
    dedupe: false,
    summaryOnly: false,
  });
  console.log("history:");
  if (history === null) {
    console.log(null);
  } else {
    console.dir(history.byTopic, { depth: null });
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
