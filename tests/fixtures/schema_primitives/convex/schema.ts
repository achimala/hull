import { defineSchema, defineTable } from "convex/server";
import { primitiveThing } from "./validators";

export default defineSchema({
  things: defineTable(primitiveThing),
});
