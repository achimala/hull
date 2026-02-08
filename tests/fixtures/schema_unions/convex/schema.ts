import { defineSchema, defineTable } from "convex/server";
import { eventDoc } from "./validators";

export default defineSchema({
  events: defineTable(eventDoc),
});
