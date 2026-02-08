import { mutation, query } from "./_generated/server";
import { v } from "convex/values";
import { eventPayload } from "./validators";

export const latest = query({
  args: {
    actorId: v.id("events"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const publish = mutation({
  args: {
    payload: eventPayload,
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
