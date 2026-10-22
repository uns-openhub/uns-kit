import { legacyPilotEnvironment } from "./compatibility/energenti-descriptor-v1.js";
import type { ProcessGroupAuthorization } from "./process-group-authorization.js";

export function processGroupLaunchEnvironment(
  localPilot: boolean,
  authorization: ProcessGroupAuthorization | undefined,
  stateRoot: string | undefined,
  operationId: string | undefined,
): Record<string, string> {
  return {
    UNS_PROCESS_GROUP_LOCAL_PILOT: localPilot ? "1" : "0",
    ...legacyPilotEnvironment(localPilot),
    UNS_PROCESS_GROUP_AUTHORIZATION_FILE: authorization?.file ?? "",
    UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY: "",
    UNS_PROCESS_GROUP_OPERATION_ID: authorization?.role === "target" ? (operationId ?? "") : "",
    UNS_PROCESS_GROUP_STATE_ROOT: authorization || localPilot ? (stateRoot ?? "") : "",
  };
}
