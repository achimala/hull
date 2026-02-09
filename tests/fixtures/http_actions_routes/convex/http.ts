import { httpRouter } from "convex/server";
import { correctStream } from "./ai";
import { stripeWebhook } from "./billing";

const http = httpRouter();

http.route({
  path: "/api/ai/correct-stream",
  method: "POST",
  handler: correctStream,
});

http.route({
  path: "/api/stripe/webhook",
  method: "POST",
  handler: stripeWebhook,
});

export default http;
