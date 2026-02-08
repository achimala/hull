import { v } from "convex/values";

export const primitiveThing = v.object({
  title: v.string(),
  count: v.number(),
  visits: v.int64(),
  ratio: v.float64(),
  enabled: v.boolean(),
  anything: v.any(),
  gone: v.null(),
  role: v.literal("owner"),
  score: v.literal(7),
  tags: v.array(v.string()),
  metrics: v.record(v.string(), v.number()),
  maybeNote: v.optional(v.string()),
  maybeRef: v.optional(v.id("things")),
  status: v.union(v.literal("ok"), v.literal("bad")),
  summary: v.union(v.string(), v.null()),
});
