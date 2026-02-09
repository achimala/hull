import { httpAction } from "./_generated/server";

export const correctStream = httpAction(async () => {
  return new Response("ok", { status: 200 });
});
