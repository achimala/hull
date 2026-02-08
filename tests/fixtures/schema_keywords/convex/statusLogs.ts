import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

export const get = query({
  args: {
    id: v.id("status_logs"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const put = mutation({
  args: {
    internal: v.boolean(),
    type: v.string(),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
