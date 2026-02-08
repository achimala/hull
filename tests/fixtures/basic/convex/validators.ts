import { v } from "convex/values";

export const userFields = {
  name: v.string(),
  email: v.string(),
};

export const user = v.object({
  ...userFields,
  role: v.union(v.literal("admin"), v.literal("member")),
});

export const message = v.object({
  body: v.string(),
  authorId: v.id("users"),
  tags: v.array(v.string()),
});
