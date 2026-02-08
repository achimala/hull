import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

export const list = query({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const create = mutation({
  args: {
    body: v.string(),
    authorId: v.id("users"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
