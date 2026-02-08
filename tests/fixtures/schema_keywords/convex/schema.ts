import { defineSchema, defineTable } from "convex/server";
import { statusLog } from "./validators";

export default defineSchema({
  status_logs: defineTable(statusLog),
});
