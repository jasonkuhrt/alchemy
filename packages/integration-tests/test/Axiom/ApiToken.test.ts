import * as Axiom from "@alchemy.run/axiom";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Output from "alchemy/Output";
import * as Provider from "alchemy/Provider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { MinimumLogLevel } from "effect/References";

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

const RENAME_TOKEN_NAME = "alchemy-test-apitoken-rename";
const DATASET_BEFORE = "alchemy-test-apitoken-rename-a";
const DATASET_AFTER = "alchemy-test-apitoken-rename-b";
const CONSUMER_DATASET = "alchemy-test-apitoken-rename-consumer";

test.provider(
  "replaces the token when the dataset it is scoped to is replaced",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      // The token's capabilities name the dataset through its `name` output,
      // which is unresolved at plan time while the dataset is being replaced.
      // A second dataset stands in for the token's consumers: its description
      // holds the token's id.
      const scopedToken = (datasetName: string) =>
        Effect.gen(function* () {
          const dataset = yield* Axiom.Dataset("RenameDataset", { name: datasetName });
          const token = yield* Axiom.ApiToken("RenameApiToken", {
            name: RENAME_TOKEN_NAME,
            datasetCapabilities: dataset.name.pipe(
              Output.map((name) => ({ [name]: { ingest: ["create"] } })),
            ),
          });
          yield* Axiom.Dataset("RenameConsumer", { name: CONSUMER_DATASET, description: token.id });
          return token;
        });

      const before = yield* stack.deploy(scopedToken(DATASET_BEFORE));
      const after = yield* stack.deploy(scopedToken(DATASET_AFTER));

      const tokens = yield* (yield* Provider.findProvider(Axiom.ApiToken)).list();
      const datasets = yield* (yield* Provider.findProvider(Axiom.Dataset)).list();

      // The token in Axiom can ingest into the new dataset, not the deleted one.
      expect(tokens.find((t) => t.id === after.id)?.datasetCapabilities).toEqual({
        [DATASET_AFTER]: { ingest: ["create"] },
      });
      // Axiom has no token update, so the old token is gone.
      expect(tokens.some((t) => t.id === before.id)).toBe(false);
      // The consumer holds the token that exists.
      expect(datasets.find((d) => d.name === CONSUMER_DATASET)?.description).toEqual(after.id);

      yield* stack.destroy();
    }).pipe(logLevel),
  {
    tags: ["provider:axiom", "provider:axiom:apitoken", "live"],
    timeout: 120_000,
  },
);
