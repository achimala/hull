import { query } from "../_generated/server";
import { v } from "convex/values";

export const getById = query({
  args: {
    id: v.id("users"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
