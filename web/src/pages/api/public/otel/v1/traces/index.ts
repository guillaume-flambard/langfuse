import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import {
  getCurrentSpan,
  logger,
  markProjectAsOtelUser,
  createIngestionAttribution,
  getLangfuseHeaderValue,
} from "@langfuse/shared/src/server";
import { z } from "zod";
import { ForbiddenError } from "@langfuse/shared";
import { env } from "@/src/env.mjs";
import {
  gunzipOtelRequestBody,
  handleOtelRequestBodyTooLarge,
  OtelRequestBodyTooLargeError,
  readOtelRequestBody,
} from "@/src/server/otel/otelRequestBody";
import { processOtelIngestion } from "@/src/server/otel/processOtelIngestion";

export const config = {
  api: {
    bodyParser: false,
  },
};

const OTEL_REQUEST_BODY_READ_TIMEOUT_MS = 300_000;

export default withMiddlewares({
  POST: createAuthedProjectAPIRoute({
    name: "OTel Traces",
    querySchema: z.any(),
    responseSchema: z.any(),
    rateLimitResource: "ingestion",
    fn: async ({ req, res, auth }) => {
      // Check if ingestion is suspended due to usage threshold
      if (auth.scope.isIngestionSuspended) {
        throw new ForbiddenError(
          "Ingestion suspended: Usage threshold exceeded. Please upgrade your plan.",
        );
      }

      // Mark project as using OTEL API
      await markProjectAsOtelUser(auth.scope.projectId);

      const useWorker = env.LANGFUSE_OTEL_INGESTION_USE_WORKER === "true";
      const workerLeaseResult = useWorker
        ? await (
            await import("@/src/server/otel/otelIngestionWorkerPool")
          ).createOtelIngestionWorkerLease(req, res)
        : undefined;
      if (workerLeaseResult?.kind === "busy") {
        res.setHeader("Retry-After", 1);
        res.setHeader("Connection", "close");
        res.status(503);
        return { error: "OTel ingestion worker is busy" };
      }
      if (workerLeaseResult?.kind === "aborted") {
        return {};
      }
      const workerLease =
        workerLeaseResult?.kind === "acquired"
          ? workerLeaseResult.lease
          : undefined;

      const maxBodyBytes = env.LANGFUSE_OTEL_INGESTION_MAX_BODY_BYTES;

      let body: Buffer;
      let encodedBodyBytes: number;
      let bodyFailureMessage = "Failed to read request body";
      const bodyReadAbortController = workerLease
        ? new AbortController()
        : undefined;
      const bodyReadTimeout = bodyReadAbortController
        ? setTimeout(
            () => bodyReadAbortController.abort(),
            OTEL_REQUEST_BODY_READ_TIMEOUT_MS,
          )
        : undefined;
      try {
        try {
          body = bodyReadAbortController
            ? await readOtelRequestBody(
                req,
                maxBodyBytes,
                bodyReadAbortController.signal,
              )
            : await readOtelRequestBody(req, maxBodyBytes);
        } finally {
          if (bodyReadTimeout !== undefined) clearTimeout(bodyReadTimeout);
        }
        encodedBodyBytes = body.byteLength;

        if (req.headers["content-encoding"]?.includes("gzip")) {
          bodyFailureMessage = "Failed to decompress request body";
          body = await gunzipOtelRequestBody(body, maxBodyBytes);
        }
      } catch (error) {
        if (bodyReadAbortController?.signal.aborted) {
          logger.warn("OTel request body read timed out", {
            projectId: auth.scope.projectId,
            timeoutMs: OTEL_REQUEST_BODY_READ_TIMEOUT_MS,
          });
          res.status(408);
          return { error: "Request body read timed out" };
        }
        if (error instanceof OtelRequestBodyTooLargeError) {
          return handleOtelRequestBodyTooLarge(
            error,
            req,
            res,
            auth.scope.projectId,
          );
        }

        logger.error(bodyFailureMessage, error);
        res.status(400);
        return { error: bodyFailureMessage };
      }

      const contentType = req.headers["content-type"]?.toLowerCase();

      // Extract SDK headers for write path decision (supports both hyphen and underscore formats)
      const attribution = createIngestionAttribution({
        headers: req.headers,
        authCheck: auth,
      });
      const ingestionVersion = getLangfuseHeaderValue(
        req.headers,
        "x-langfuse-ingestion-version",
      );
      if (workerLease && ingestionVersion) {
        getCurrentSpan()?.setAttribute(
          "langfuse.ingestion.version",
          ingestionVersion,
        );
      }

      // Extract headers to propagate for ingestion masking
      const propagatedHeaderNames =
        env.LANGFUSE_INGESTION_MASKING_PROPAGATED_HEADERS;
      const propagatedHeaders: Record<string, string> = {};
      for (const headerName of propagatedHeaderNames) {
        const value = req.headers[headerName];
        if (typeof value === "string") {
          propagatedHeaders[headerName] = value;
        }
      }

      const ingestionRequest = {
        body,
        contentType,
        encodedBodyBytes,
        config: {
          projectId: auth.scope.projectId,
          publicKey: auth.scope.publicKey,
          orgId: auth.scope.orgId,
          propagatedHeaders:
            Object.keys(propagatedHeaders).length > 0
              ? propagatedHeaders
              : undefined,
          sdkName: attribution.ingestionSdkName,
          sdkVersion: attribution.ingestionSdkVersion,
          rejectionSdkName: req.headers["x-langfuse-sdk-name"],
          ingestionVersion,
        },
      };
      const result = workerLease
        ? await workerLease.run(ingestionRequest)
        : await processOtelIngestion(ingestionRequest);
      if (!result) {
        return {};
      }
      if (result.kind === "http") {
        res.status(result.status);
        return result.body;
      }

      return result.body ?? {};
    },
  }),
});
