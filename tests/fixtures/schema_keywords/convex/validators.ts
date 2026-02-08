import { v } from "convex/values";

export const statusLog = v.object({
  internal: v.boolean(),
  type: v.string(),
  snake_case: v.string(),
  mixedCaseName: v.string(),
});
