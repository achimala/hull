import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

const visibility = v.union(v.literal("private"), v.literal("shared"));

export default defineSchema({
  tasks: defineTable({
    name: v.string(),
    visibility,
  }),
});
