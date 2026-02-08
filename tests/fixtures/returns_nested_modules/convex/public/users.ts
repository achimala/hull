import { query } from "../_generated/server";
import { v } from "convex/values";

export const list = query({
  args: {
    cursor: v.optional(v.string()),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
