import * as Axiom from "@distilled.cloud/axiom";
import { deepEqual, isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import type { Providers } from "./Providers.ts";

export type ApiTokenProps = Omit<Axiom.CreateAPITokenRequest, never>;

export type ApiToken = Resource<
  "Axiom.ApiToken",
  ApiTokenProps,
  Omit<Axiom.CreateAPITokenResponse, "token"> & {
    /**
     * The bearer token. Returned only by `create` (and `regenerate`); Axiom
     * does not return it on subsequent reads. Persisted in resource state via
     * `Redacted` — handle with care.
     */
    token: Redacted.Redacted<string>;
  },
  never,
  Providers
>;

/**
 * An Axiom API token — a scoped bearer token used to authenticate API
 * requests (ingest, query, admin). Capabilities are pinned at creation time;
 * changing any field triggers a **replacement** because Axiom does not
 * expose an update endpoint.
 *
 * The raw token value is returned only by `create`. After that, Axiom
 * never echoes it back, so it is captured into `output.token` (as a
 * {@link Redacted}) on initial create and persisted in resource state.
 * Treat resource state as sensitive — anyone with read access can recover
 * the token. Pair with a secret store for downstream consumption.
 * @see https://axiom.co/docs/reference/tokens
 *
 * ### Creating an API Token
 * **Example:** Ingest-only token scoped to one dataset
 * ```typescript
 * const ingest = yield* Axiom.ApiToken("ingest", {
 *   name: "prod-ingest",
 *   description: "OTEL collector ingest",
 *   datasetCapabilities: {
 *     "my-app-traces": { ingest: ["create"] },
 *   },
 * });
 * ```
 *
 * **Example:** Read-only query token
 * ```typescript
 * yield* Axiom.ApiToken("query", {
 *   name: "grafana-reader",
 *   datasetCapabilities: {
 *     "my-app-traces": { query: ["read"] },
 *     "my-app-logs":   { query: ["read"] },
 *   },
 * });
 * ```
 *
 * ### Consuming the Token
 * **Example:** Forward the token via Cloudflare Secrets
 * ```typescript
 * const secret = yield* Cloudflare.SecretsStore.Secret("axiom-token", {
 *   value: ingest.token,
 * });
 * ```
 *
 * @resource
 * @product API Token
 */
export const ApiToken = Resource<ApiToken>("Axiom.ApiToken");

/** Axiom created a token but returned no bearer value. */
export class MissingApiTokenValue extends Data.TaggedError("MissingApiTokenValue")<{
  readonly id: string;
}> {
  override get message() {
    return `Axiom did not return a value for the new API token ${this.id}`;
  }
}

export const ApiTokenProvider = () =>
  Provider.effect(
    ApiToken,
    Effect.gen(function* () {
      const create = yield* Axiom.createAPIToken;
      const get = yield* Axiom.getAPIToken;
      const del = yield* Axiom.deleteAPIToken;
      const regenerate = yield* Axiom.regenerateAPIToken;
      const listTokens = yield* Axiom.getAPITokens;

      return {
        stables: ["id", "token"],
        // Enumerate every API token in the org. Axiom exposes a single
        // account-wide `GET /v2/tokens` collection op (no pagination), so we
        // fetch it once and hydrate each row into the exact `read` Attributes
        // shape. The bearer secret is returned by Axiom only at creation and is
        // never echoed back on enumeration, so — matching `read`, which sources
        // the secret from cached state — we surface an empty Redacted token
        // here rather than the real value.
        list: () =>
          Effect.gen(function* () {
            const tokens = yield* listTokens({});
            return tokens.map((token) => ({
              ...token,
              token: Redacted.make(""),
            }));
          }),
        diff: Effect.fn(function* ({ olds, news, output }) {
          // First create — let the engine create normally.
          if (output == null) return undefined;
          // Inputs not known until apply (the name of a dataset that is being
          // replaced, say) may resolve to new values or to the old ones. An
          // update that keeps nothing stable makes consumers wait for this
          // token's `id` and `token`; `reconcile` decides whether to rotate.
          if (!isResolved(news)) return { action: "update", stables: [] } as const;
          // Axiom has no update endpoint for tokens, so any actual change
          // to the inputs forces a replacement (which mints a new bearer
          // value). If nothing changed, fall through to `noop` so we don't
          // needlessly rotate the token on every deploy.
          if (deepEqual(olds, news)) return undefined;
          return { action: "replace" } as const;
        }),
        reconcile: Effect.fn(function* ({ news, olds, output }) {
          // Observe — Axiom does not echo the bearer back, so the cached
          // `output` (with the persisted Redacted token) is the current state
          // while the inputs are the ones it was minted with.
          if (output !== undefined && (olds === undefined || deepEqual(olds, news))) {
            return output;
          }

          // Ensure — mint a new token. Axiom returns the bearer exactly
          // once; capture it into Redacted state for downstream consumers.
          // Changed inputs reach here only from an update `diff` planned
          // over unresolved inputs. Regenerating mints the new token and
          // revokes the old one in one call; an old token already gone is
          // created afresh.
          const result =
            output === undefined
              ? yield* create(news)
              : yield* regenerate({
                  id: output.id,
                  existingTokenExpiresAt: DateTime.formatIso(yield* DateTime.now),
                  newToken: news,
                }).pipe(Effect.catchTag("NotFound", () => create(news)));
          if (!result.token) {
            return yield* Effect.die(new MissingApiTokenValue({ id: result.id }));
          }
          return {
            ...result,
            token: Redacted.isRedacted(result.token) ? result.token : Redacted.make(result.token),
          };
        }),
        delete: Effect.fn(function* ({ output }) {
          yield* del({ id: output.id }).pipe(Effect.catchTag("NotFound", () => Effect.void));
        }),
        read: Effect.fn(function* ({ output }) {
          if (!output?.id) return undefined;
          return yield* get({ id: output.id }).pipe(
            Effect.map((current) => ({ ...current, token: output.token })),
            Effect.catchTag("NotFound", () => Effect.undefined),
          );
        }),
      };
    }),
  );
