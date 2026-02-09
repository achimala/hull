import { httpAction } from "./_generated/server";

export const stripeWebhook = httpAction(async () => {
  return new Response("ok", { status: 200 });
});
