import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

export const byEmail = query({
  args: {
    email: v.string(),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const create = mutation({
  args: {
    email: v.string(),
    status: v.union(v.literal("active"), v.literal("disabled")),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
