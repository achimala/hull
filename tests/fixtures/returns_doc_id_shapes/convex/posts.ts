import { mutation, query } from "./_generated/server";
import { v } from "convex/values";

export const getById = query({
  args: {
    id: v.id("posts"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const listByAuthor = query({
  args: {
    authorId: v.id("users"),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});

export const create = mutation({
  args: {
    authorId: v.id("users"),
    body: v.string(),
  },
  handler: async (_ctx, _args) => {
    return null;
  },
});
