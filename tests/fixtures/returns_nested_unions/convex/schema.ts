import { defineSchema, defineTable } from "convex/server";
import { job } from "./validators";

export default defineSchema({
  jobs: defineTable(job),
});
