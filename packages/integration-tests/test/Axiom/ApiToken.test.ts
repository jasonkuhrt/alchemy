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

const RENAME_TOKEN_NAME = testName("alchemy-test-apitoken-rename");
const DATASET_BEFORE = testName("alchemy-test-apitoken-rename-a");
const DATASET_AFTER = testName("alchemy-test-apitoken-rename-b");
const CONSUMER_DATASET = testName("alchemy-test-apitoken-rename-consumer");

test.provider(
  "a token scoped to a dataset that is replaced moves to the new dataset",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      // The token's capabilities name the dataset through its `name` output,
      // and its description reads the dataset's `created` output. Both are
      // unresolved at plan time while the dataset is replaced; an in-place
      // update keeps `name` known and leaves only `created` unresolved. A
      // second dataset stands in for the token's consumers: its description
      // holds the token's id.
      const scopedToken = (datasetName: string, datasetDescription: string) =>
        Effect.gen(function* () {
          const dataset = yield* Axiom.Dataset("RenameDataset", {
            name: datasetName,
            description: datasetDescription,
          });
          const token = yield* Axiom.ApiToken("RenameApiToken", {
            name: RENAME_TOKEN_NAME,
            description: dataset.created.pipe(Output.map((created) => `dataset of ${created}`)),
            datasetCapabilities: dataset.name.pipe(
              Output.map((name) => ({ [name]: { ingest: ["create"] } })),
            ),
          });
          yield* Axiom.Dataset("RenameConsumer", { name: CONSUMER_DATASET, description: token.id });
          return { dataset, token };
        });
      const tokenNamed = (id: string) =>
        Effect.gen(function* () {
          const tokens = yield* (yield* Provider.findProvider(Axiom.ApiToken)).list();
          return tokens.find((t) => t.id === id);
        });
      const consumerDescription = Effect.gen(function* () {
        const datasets = yield* (yield* Provider.findProvider(Axiom.Dataset)).list();
        return datasets.find((d) => d.name === CONSUMER_DATASET)?.description;
      });

      const before = yield* stack.deploy(scopedToken(DATASET_BEFORE, "first"));
      expect(yield* ingest(before.dataset, before.token.token)).toBe(200);

      // Renaming replaces the dataset. The token now ingests into the new
      // dataset, the old bearer is refused and its token is gone, and the
      // consumer holds the new token.
      const after = yield* stack.deploy(scopedToken(DATASET_AFTER, "first"));
      expect((yield* tokenNamed(after.token.id))?.datasetCapabilities).toEqual({
        [DATASET_AFTER]: { ingest: ["create"] },
      });
      expect(after.token.id).not.toEqual(before.token.id);
      expect(yield* tokenNamed(before.token.id)).toBeUndefined();
      expect(yield* ingest(after.dataset, after.token.token)).toBe(200);
      expect(yield* ingest(after.dataset, before.token.token)).toBe(401);
      expect(yield* consumerDescription).toEqual(after.token.id);

      // Redeploying unchanged keeps the token.
      const again = yield* stack.deploy(scopedToken(DATASET_AFTER, "first"));
      expect(again.token.id).toEqual(after.token.id);

      // Updating the dataset leaves the token's description unresolved at
      // plan time, but it resolves to the same value, so the token stays.
      const updated = yield* stack.deploy(scopedToken(DATASET_AFTER, "second"));
      expect(updated.token.id).toEqual(after.token.id);
      expect(yield* ingest(updated.dataset, updated.token.token)).toBe(200);
      expect(yield* consumerDescription).toEqual(after.token.id);

      yield* stack.destroy();

      const tokens = yield* (yield* Provider.findProvider(Axiom.ApiToken)).list();
      expect(tokens.filter((t) => t.name === RENAME_TOKEN_NAME)).toEqual([]);
      const datasets = yield* (yield* Provider.findProvider(Axiom.Dataset)).list();
      expect(
        datasets.filter((d) => [DATASET_BEFORE, DATASET_AFTER, CONSUMER_DATASET].includes(d.name)),
      ).toEqual([]);
    }).pipe(logLevel),
  {
    tags: ["provider:axiom", "provider:axiom:apitoken", "live"],
    timeout: 120_000,
  },
);
