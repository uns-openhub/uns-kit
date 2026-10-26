import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

import { inspectGenerationJournal, processGroupGenerationInputSchema } from "../../packages/uns-core/src/runtime/process-group-generation.js";
import { processGroupUpgradeInputSchema } from "../../packages/uns-core/src/runtime/process-group-upgrade.js";
import { generationFixture } from "../helpers/process-group-generation-fixture.js";

describe("shared reviewed generation transition", () => {
  it.each([
    ["quality-adapter", false],
    ["document-router", true],
  ] as const)("reviews %s without executing application code", (name, nested) => {
    const f = generationFixture(name, nested);
    try {
      const before = fs.readFileSync(path.join(f.journals.source.directory, "admission.sqlite"));
      const p = f.review();
      expect(p.review.protocolVersion).toBe(2);
      if (p.review.protocolVersion !== 2) throw Error("wrong review");
      expect(p.review.resourceChanges).toHaveLength(2);
      expect(p.review.sourcePolicyFile).not.toBe(p.review.policyFile);
      expect(inspectGenerationJournal(p.review.sourceJournal).records).toBe(1);
      expect(inspectGenerationJournal(p.review.targetJournal).records).toBe(0);
      expect(fs.readFileSync(path.join(f.journals.source.directory, "admission.sqlite"))).toEqual(before);
    } finally {
      f.close();
    }
  });
  it.each(["pending", "used", "partial", "contract", "profile", "policy", "resource", "channel", "outside-configuration"])(
    "refuses %s target/review without clearing old work",
    (mutation) => {
      const f = generationFixture();
      try {
        if (mutation === "pending") f.append("target");
        if (mutation === "used")
          f.databases.target.exec(
            "UPDATE usage SET admitted=1; UPDATE profiles SET completed=1,last_seq=1 WHERE name=(SELECT name FROM profiles LIMIT 1)",
          );
        if (mutation === "partial") f.databases.target.exec("DELETE FROM metadata WHERE key='id'");
        if (mutation === "contract") f.databases.target.exec("UPDATE metadata SET value='{}' WHERE key='contract'");
        if (mutation === "profile") f.databases.target.exec("UPDATE profiles SET contract='{}'");
        if (mutation === "policy") fs.appendFileSync(path.join(f.registry, "cell-rev-b.json"), "changed");
        if (mutation === "resource") (f.configs.target.publisher.raw["transform"] as any).document.profiles[0].resource.rows[0].unit = "different";
        if (mutation === "channel")
          f.configs.target.publisher.resolved = { ...f.configs.target.publisher.resolved, infra: { host: "other.invalid" } };
        if (mutation === "outside-configuration") f.configs.target.publisher.raw["extra"] = true;
        expect(() => f.review()).toThrow();
        expect(f.databases.source.prepare("SELECT count(*) n FROM tasks").get()?.["n"]).toBe(1);
      } finally {
        f.close();
      }
    },
  );
  it("requires explicit acknowledgement and permits same-version distinct placements", () => {
    const p = (id: string) => ({ controller: "local", rttNode: "generic-adapter", version: "1.0.0", instanceId: id });
    const input = {
      protocolVersion: 2,
      source: { publisher: p("a"), receiver: p("b") },
      target: { publisher: p("c"), receiver: p("d") },
      acknowledgeInputPause: true,
      acknowledgeResourceReview: true,
      acknowledgePossibleReplay: true,
    };
    expect(() => processGroupUpgradeInputSchema.parse(input)).not.toThrow();
    expect(() => processGroupGenerationInputSchema.parse({ ...input, acknowledgeResourceReview: false })).toThrow();
    expect(() => processGroupGenerationInputSchema.parse({ ...input, acknowledgePossibleReplay: false })).toThrow();
    expect(() => processGroupGenerationInputSchema.parse({ ...input, target: input.source })).toThrow();
  });
});

it("reviews a later configuration-owned generation through the same shared protocol", () => {
  const f = generationFixture("document-router", true, true);
  try {
    const p = f.review();
    if (p.review.protocolVersion !== 2) throw Error("wrong review");
    expect(p.review.sourceJournal.contractVersion).toBe(2);
    expect(p.review.sourceJournal.generationId).toBe("cell-rev-a");
    expect(p.review.targetJournal.generationId).toBe("cell-rev-b");
    expect(inspectGenerationJournal(p.review.sourceJournal).records).toBe(1);
  } finally {
    f.close();
  }
});

it("does not call missing admitted work an empty source generation", () => {
  const f = generationFixture();
  try {
    f.databases.source.exec("DELETE FROM tasks; UPDATE usage SET records=0,bytes=0");
    expect(() => f.review()).toThrow("usage is unconfirmed");
  } finally {
    f.close();
  }
});
