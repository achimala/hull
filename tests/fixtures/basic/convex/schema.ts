import { defineSchema, defineTable } from "convex/server";
import { message, user } from "./validators";

export default defineSchema({
  users: defineTable(user),
  messages: defineTable(message),
});
