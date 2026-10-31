import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Portless from "@alchemy.run/portless";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import { Server, ServerProvider } from "alchemy/Website/Server";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as pathe from "pathe";
import { routeOf } from "./fixtures/Processes.ts";
import * as TestProxy from "./fixtures/TestProxy.ts";

// The adapter under the real sidecar, with the two consumers of the ingress contract: a Worker
// and a `Website.Server` site. The sidecar resolves the adapter from this project's dependencies,
// as `alchemy dev` does, and the routes it adds are its own.
const proxy = TestProxy.make();
const { test, afterAll } = Test.make({
  providers: Layer.mergeAll(ServerProvider(), Cloudflare.providers(), proxy.providers()),
  dev: true,
  stage: "dev_alice",
});
afterAll(proxy.stop);

const worker = pathe.resolve(
  import.meta.dirname,
  "../Cloudflare/Workers/fixtures/ingress/worker.ts",
);
const framework = pathe.resolve(import.meta.dirname, "fixtures/ingress-framework.ts");

test.provider(
  "a Worker's url is its Portless name, routed by the sidecar",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const deployed = yield* stack.deploy(
        Effect.gen(function* () {
          const proxy = yield* Portless.Proxy("Proxy", { domain: "ingress-consumers" });
          const builder = yield* Cloudflare.Worker("Builder", {
            main: worker,
            env: { SELF_URL: Cloudflare.Worker.URL },
            dev: { ingress: Portless.Ingress(proxy) },
          });
          return { proxy, builder };
        }),
      );
      const origin = `https://dev-alice.builder.ingress-consumers.localhost:${deployed.proxy.port}`;
      expect(deployed.builder.url).toBe(origin);

      // The name reaches the Worker over verified HTTPS with its own Host, and the Worker
      // knows itself by that name.
      const served = yield* TestProxy.serving(origin, deployed.proxy, "/hello");
      expect(served.body).toBe(
        `ingress-worker host=dev-alice.builder.ingress-consumers.localhost:${deployed.proxy.port} path=/hello`,
      );
      const self = yield* TestProxy.serving(origin, deployed.proxy, "/self-url");
      expect(JSON.parse(self.body)).toEqual({ env: origin });

      // The sidecar owns the route, not this process.
      const route = yield* routeOf(
        deployed.proxy.stateDirectory,
        "dev-alice.builder.ingress-consumers.localhost",
      );
      expect(route).toBeDefined();
      expect(route!.pid).not.toBe(process.pid);

      yield* stack.destroy();
      yield* TestProxy.answers(origin, deployed.proxy, 404);
    }),
  { tags: ["provider:cloudflare:worker", "local"], timeout: 180_000 },
);

test.provider(
  "a Website.Server's url is its Portless name, routed by the sidecar",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const deployed = yield* stack.deploy(
        Effect.gen(function* () {
          const proxy = yield* Portless.Proxy("Proxy", { domain: "ingress-consumers" });
          const site = yield* Server("Site", {
            framework,
            target: "unused",
            dev: { ingress: Portless.Ingress(proxy) },
          });
          return { proxy, site };
        }),
      );
      const origin = `https://dev-alice.site.ingress-consumers.localhost:${deployed.proxy.port}`;
      expect(deployed.site.url).toBe(origin);
      // The raw listener is still what the session dials.
      expect(deployed.site.localUrl).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);

      const served = yield* TestProxy.serving(origin, deployed.proxy, "/page");
      expect(served.body).toBe(
        `/page dev-alice.site.ingress-consumers.localhost:${deployed.proxy.port}`,
      );
      const route = yield* routeOf(
        deployed.proxy.stateDirectory,
        "dev-alice.site.ingress-consumers.localhost",
      );
      expect(route).toMatchObject({ port: Number(new URL(deployed.site.localUrl!).port) });
      expect(route!.pid).not.toBe(process.pid);

      yield* stack.destroy();
      yield* TestProxy.answers(origin, deployed.proxy, 404);
    }),
  { tags: ["local"], timeout: 180_000 },
);
