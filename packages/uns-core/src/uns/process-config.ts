// process-config.ts
import { readFileSync } from "fs";
import * as path from "path";

import { basePath } from "../base-path.js";

// Path to package.json to retrieve package name and version
export const PACKAGE_JSON_PATH = path.join(basePath, "package.json");

// Read package.json and export as an object
interface PackageInfo {
  name: string;
  version: string;
}

const rawPackageInfo: PackageInfo = JSON.parse(readFileSync(PACKAGE_JSON_PATH, "utf8"));

export const PACKAGE_INFO: PackageInfo = {
  ...rawPackageInfo,
  name: rawPackageInfo.name,
};

// Other configuration values (update intervals, timeouts, etc.)
export const MQTT_UPDATE_INTERVAL = 10000; // in milliseconds
/**
 * A fresh active heartbeat is published every 10 seconds. Leave enough time
 * for a just-subscribed process to receive a complete heartbeat interval.
 */
export const ACTIVE_TIMEOUT = 25000; // in milliseconds
/** Retained active heartbeats expire automatically when a process disappears. */
export const ACTIVE_STATUS_EXPIRY_SECONDS = 30;
/** A normal shutdown should briefly advertise passive state, then clear it. */
export const INACTIVE_STATUS_EXPIRY_SECONDS = 1;
