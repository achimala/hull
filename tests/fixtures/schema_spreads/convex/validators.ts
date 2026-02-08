import { v } from "convex/values";

export const personFields = {
  name: v.string(),
  handle: v.string(),
};

export const auditFields = {
  createdAt: v.number(),
  updatedAt: v.optional(v.number()),
};

export const accountFields = {
  ...personFields,
  ...auditFields,
  status: v.union(v.literal("active"), v.literal("disabled")),
  tags: v.array(v.string()),
};

export const account = v.object({
  ...accountFields,
});
