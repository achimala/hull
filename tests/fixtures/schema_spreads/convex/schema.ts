import { defineSchema, defineTable } from "convex/server";
import { account } from "./validators";

export default defineSchema({
  accounts: defineTable(account),
});
