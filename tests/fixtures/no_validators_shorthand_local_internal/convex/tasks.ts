import { internalMutation, mutation, query } from "./_generated/server";
import { v } from "convex/values";

const statusValidator = v.union(v.literal("todo"), v.literal("done"));

export const list = query({
  args: {
    status: statusValidator,
  },
  handler: async () => {
    return [];
  },
});

export const setStatus = mutation({
  args: {
    id: v.id("tasks"),
    status: statusValidator,
  },
  handler: async () => {
    return null;
  },
});

export const applyStatusInternal = internalMutation({
  args: {
    id: v.id("tasks"),
    status: statusValidator,
  },
  handler: async () => {
    return "ok";
  },
});
