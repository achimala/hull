import { defineSchema, defineTable } from "convex/server";
import { post, user } from "./validators";

export default defineSchema({
  users: defineTable(user),
  posts: defineTable(post),
});
