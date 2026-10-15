import { redis } from "../redis";
import type { ApiCallInputMessage } from "common-db";
import { config } from "../config";
import { rootLogger } from "../logger";
import {
  createPersistedResponse,
  settlePersistedResponse,
  loadResponse,
  loadResponseMessages,
  deletePersistedResponse,
  isResponseDeleted,
  type ResponseOwner,
  type ResponseAttribution,
} from "./responses/persistence";
import type { ResponseObject } from "./responses/schemas";

const log = rootLogger.child({ name: "response-store" });

const responseKey = (orgId: string, id: string) => `response:${orgId}:${id}`;
const deletedKey = (orgId: string, id: string) => `${responseKey(orgId, id)}:deleted`;

/** Only the creating write can supply these; later writes are recognized by status. */
export type ResponseCreation = ResponseAttribution & { inputMessages: ApiCallInputMessage[] };

const GUARDED_CACHE_WRITE = `
if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
`;

const MARK_DELETED_AND_CLEAR = `
redis.call('SET', KEYS[2], '1')
redis.call('DEL', KEYS[1])
return 1
`;

async function cacheResponse(orgId: string, id: string, payload: unknown): Promise<boolean> {
  const result = await redis.send("EVAL", [
    GUARDED_CACHE_WRITE,
    "2",
    responseKey(orgId, id),
    deletedKey(orgId, id),
    JSON.stringify(payload),
    String(config.cache.responseTtlSeconds()),
  ]);
  return Number(result) === 1;
}

/**
 * Postgres failures are logged rather than thrown: Redis already holds the response, so
 * the request can still be served for the cache window.
 */
async function persist(orgId: string, response: ResponseObject, creation?: ResponseCreation): Promise<void> {
  const owner: ResponseOwner = {
    orgId,
    apiKeyId: creation?.apiKeyId ?? null,
    applicationId: creation?.applicationId ?? null,
    inferenceCallId: creation?.inferenceCallId ?? null,
  };
  try {
    if (response.status === "in_progress") {
      if (creation) {
        await createPersistedResponse({ response, ...owner, inputMessages: creation.inputMessages });
      }
      return;
    }
    if (!await settlePersistedResponse(orgId, response)) {
      log.warn({ responseId: response.id, status: response.status }, "No in-progress row to settle");
    }
  } catch (err) {
    log.error({ err, responseId: response.id }, "Failed to persist response");
  }
}

export async function saveResponse(
  orgId: string,
  id: string,
  response: ResponseObject,
  creation?: ResponseCreation,
): Promise<void> {
  if (!await cacheResponse(orgId, id, response)) {
    log.info({ responseId: id }, "Skipped caching deleted response");
    return;
  }
  if (response.store === false) {
    return;
  }
  await persist(orgId, response, creation);
}

export async function getResponse(orgId: string, id: string): Promise<unknown | null> {
  let deletionMarkerAvailable = true;
  try {
    if (await redis.get(deletedKey(orgId, id))) {
      return null;
    }
  } catch (err) {
    deletionMarkerAvailable = false;
    log.warn({ err, responseId: id }, "Redis error checking response deletion marker");
  }

  // The durable tombstone must be checked even on a Redis cache hit:
  // the Redis deletion marker may have expired or been evicted.
  try {
    if (await isResponseDeleted(orgId, id)) {
      return null;
    }
  } catch (err) {
    log.warn({ err, responseId: id }, "Error checking durable response deletion");
    return null;
  }

  if (deletionMarkerAvailable) {
    let payload: string | null;
    try {
      payload = await redis.get(responseKey(orgId, id));
    } catch (err) {
      log.warn({ err, responseId: id }, "Redis error in getResponse");
      payload = null;
    }

    if (payload) {
      try {
        return JSON.parse(payload);
      } catch (err) {
        log.error({ err, responseId: id }, "Corrupted response data in Redis");
      }
    }
  }

  const stored = await loadResponse(orgId, id);
  if (!stored) {
    return null;
  }
  const cached = await cacheResponse(orgId, id, stored)
    .catch((err) => {
      log.warn({ err, responseId: id }, "Failed to re-cache response");
      return false;
    });
  return cached ? stored : null;
}

/**
 * The messages a stored response was built from. Only Postgres holds these; the Redis entry is
 * the response object, which carries the answer but not the question. Empty for a response
 * created with `store: false`, which is never written to Postgres at all.
 */
export function getResponseMessages(orgId: string, id: string): Promise<ApiCallInputMessage[]> {
  return loadResponseMessages(orgId, id);
}

/** Returns false when the entry could not be removed, so callers do not report a delete that did not happen. */
export async function deleteResponse(orgId: string, id: string): Promise<boolean> {
  try {
    await redis.send("EVAL", [
      MARK_DELETED_AND_CLEAR,
      "2",
      responseKey(orgId, id),
      deletedKey(orgId, id),
    ]);
    await deletePersistedResponse(orgId, id);
    return true;
  } catch (err) {
    log.warn({ err }, "Error in deleteResponse");
    return false;
  }
}
