import { describe, expect, it } from "vitest";
import { UnsPacket } from "../packages/uns-core/src/uns/uns-packet.js";
import type { IUnsMessage } from "../packages/uns-core/src/uns/uns-interfaces.js";

const time = "2026-11-02T12:00:00.000Z" as const;

describe("explicit typed null scalar data", () => {
  it.each(["string", "number"] as const)("roundtrips a %s clear without inventing a value", async valueType => {
    const packet = await UnsPacket.unsPacketFromUnsMessage({
      data: { time, value: null, valueType, uom: "" }, createdAt: time,
    });
    expect(packet.message.data).toMatchObject({ time, value: null, valueType, uom: "" });
    const read = UnsPacket.parseMqttPacket(JSON.stringify(packet));
    expect(read?.message.data?.value).toBeNull();
    expect(read?.message.data?.valueType).toBe(valueType);
    expect(read?.message.data?.time).toBe(time);
  });
  it.each([undefined, "object", "boolean", ""])("refuses ambiguous null type %s", async valueType => {
    await expect(UnsPacket.unsPacketFromUnsMessage({
      data: { time, value: null, valueType },
    } as unknown as IUnsMessage)).rejects.toThrow("Null data requires explicit");
  });
  it("still distinguishes an omitted value from an explicit clear", async () => {
    await expect(UnsPacket.unsPacketFromUnsMessage({
      data: { time, valueType: "string" },
    } as unknown as IUnsMessage)).rejects.toThrow("Value is not defined");
  });
  it.each(["", "ID-001", 0, 12.5])("retains ordinary scalar %s behavior", async value => {
    const packet = await UnsPacket.unsPacketFromUnsMessage({ data: { time, value } });
    expect(packet.message.data?.value).toBe(value);
    expect(packet.message.data?.valueType).toBe(typeof value);
  });
  it("does not make arbitrary native object/array data a supported scalar", async () => {
    await expect(UnsPacket.unsPacketFromUnsMessage({
      data: { time, value: [1, 2] },
    })).rejects.toThrow("must be string or number");
  });
});
