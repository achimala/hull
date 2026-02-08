import { v } from "convex/values";

export const user = v.object({
  email: v.string(),
  status: v.union(v.literal("active"), v.literal("disabled")),
});
