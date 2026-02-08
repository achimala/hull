import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

export const byHandle = query({
  args: {
    handle: v.string(),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const disable = mutation({
  args: {
    id: v.id("accounts"),
    reason: v.optional(v.string()),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
