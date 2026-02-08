import { v } from "convex/values";

export const eventPayload = v.union(
  v.object({
    kind: v.literal("created"),
    targetId: v.id("events"),
  }),
  v.object({
    kind: v.literal("deleted"),
    targetId: v.id("events"),
    reason: v.optional(v.string()),
  })
);

export const eventDoc = v.object({
  actorId: v.id("events"),
  payload: eventPayload,
});
