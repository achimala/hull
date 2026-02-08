import { v } from "convex/values";

export const user = v.object({
  name: v.string(),
});

export const post = v.object({
  authorId: v.id("users"),
  body: v.string(),
});
