import { query } from "./_generated/server";
import { v } from "convex/values";

export const snapshot = query({
  args: {
    id: v.id("jobs"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const stream = query({
  args: {
    id: v.id("jobs"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
