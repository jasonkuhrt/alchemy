import * as Axiom from "@alchemy.run/axiom";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Output from "alchemy/Output";
import * as Provider from "alchemy/Provider";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Redacted from "effect/Redacted";
import { MinimumLogLevel } from "effect/References";
import { testName } from "../names.ts";

const { test } = Test.make({ providers: Axiom.providers() });

const logLevel = Effect.provideService(MinimumLogLevel, process.env.DEBUG ? "Debug" : "Info");

// Deterministic token name (no Date.now) so reruns target the same token and
// the start/end destroy bookends keep the org clean.
const TOKEN_NAME = "alchemy-list-test-token";

test.provider(
  "list enumerates the deployed api token",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const deployed = yield* stack.deploy(
        Effect.gen(function* () {
          return yield* Axiom.ApiToken("ListApiToken", {
            name: TOKEN_NAME,
            description: "alchemy list() lifecycle op test",
            // Axiom rejects a token with no capabilities ("at least one
            // capability must be set"). An org-level read scope is the
            // simplest valid grant that needs no pre-existing dataset.
            orgCapabilities: { datasets: ["read"] },
          });
        }),
      );

      // The bearer Axiom returns at creation is the token's value, redacted once.
      expect(typeof Redacted.value(deployed.token)).toBe("string");
      expect(Redacted.value(deployed.token)).not.toBe("");

      const provider = yield* Provider.findProvider(Axiom.ApiToken);
      const all = yield* provider.list();

      // `getAPITokens` returns the full account-wide list in one shot; the
      // freshly created token must be present, hydrated into the exact `read`
      // Attributes shape (so `.id` / `.name` are directly usable by `delete`).
      expect(all.some((t) => t.id === deployed.id)).toBe(true);

      const row = all.find((t) => t.id === deployed.id)!;
      expect(row.name).toEqual(TOKEN_NAME);
      // The bearer secret is only returned at creation and is never echoed back
      // on enumeration, so `list()` surfaces an empty Redacted token (matching
      // the fact that `read` would only source it from cached state).
      expect(row.token).toBeDefined();

      yield* stack.destroy();
    }).pipe(logLevel),
  {
    tags: ["provider:axiom", "provider:axiom:apitoken", "live"],
    timeout: 120_000,
  },
);

const SCOPED_TOKEN_NAME = testName("alchemy-test-apitoken-scoped");
const SCOPED_DATASET_A = testName("alchemy-test-apitoken-scoped-a");
const SCOPED_DATASET_B = testName("alchemy-test-apitoken-scoped-b");

// Ingest one event with a bearer into a dataset on its edge; the status says
// whether Axiom accepted the bearer.
const ingest = (dataset: Axiom.Dataset["Attributes"], token: Redacted.Redacted<string>) =>
  HttpClient.execute(
    HttpClientRequest.post(
      `${dataset.edgeDeploymentUrl.replace(/\/$/, "")}/v1/ingest/${dataset.name}`,
    ).pipe(
      HttpClientRequest.bearerToken(token),
      HttpClientRequest.bodyJsonUnsafe([{ test: "apitoken" }]),
    ),
  ).pipe(Effect.map((response) => response.status));

test.provider(
  "a token scoped to a dataset by its name follows a rename of the dataset",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      // The token's capabilities come from the dataset's `name` output; its
      // other inputs are constant.
      const scopedToken = (datasetName: string) =>
        Effect.gen(function* () {
          const dataset = yield* Axiom.Dataset("ScopedDataset", { name: datasetName });
          const token = yield* Axiom.ApiToken("ScopedApiToken", {
            name: SCOPED_TOKEN_NAME,
            description: "alchemy scoped token test",
            datasetCapabilities: dataset.name.pipe(
              Output.map((name) => ({ [name]: { ingest: ["create"] } })),
            ),
          });
          return { dataset, token };
        });

      yield* stack.deploy(scopedToken(SCOPED_DATASET_A));
      const { dataset, token } = yield* stack.deploy(scopedToken(SCOPED_DATASET_B));

      const tokens = yield* (yield* Provider.findProvider(Axiom.ApiToken)).list();
      expect(tokens.find((t) => t.id === token.id)?.datasetCapabilities).toEqual({
        [SCOPED_DATASET_B]: { ingest: ["create"] },
      });
      expect(yield* ingest(dataset, token.token)).toBe(200);

      yield* stack.destroy();
    }).pipe(logLevel),
  {
    tags: ["provider:axiom", "provider:axiom:apitoken", "live"],
    timeout: 120_000,
  },
);
