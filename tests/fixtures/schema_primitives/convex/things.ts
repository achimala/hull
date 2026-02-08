import { action, mutation, query } from "./_generated/server";
import { v } from "convex/values";

export const list = query({
  args: {
    cursor: v.optional(v.string()),
    includeGone: v.optional(v.boolean()),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const create = mutation({
  args: {
    item: v.object({
      title: v.string(),
      count: v.number(),
    }),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const inspect = action({
  args: {
    id: v.id("things"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
