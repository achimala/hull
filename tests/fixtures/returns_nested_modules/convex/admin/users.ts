import { query } from "../_generated/server";
import { v } from "convex/values";

export const list = query({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
