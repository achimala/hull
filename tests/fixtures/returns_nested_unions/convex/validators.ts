import { v } from "convex/values";

export const job = v.object({
  title: v.string(),
  ownerId: v.id("jobs"),
});
