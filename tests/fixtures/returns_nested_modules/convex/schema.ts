import { defineSchema, defineTable } from "convex/server";
import { user } from "./validators";

export default defineSchema({
  users: defineTable(user),
});
