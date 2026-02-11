import { randomUUID } from "node:crypto";

export const makeId = (): string => randomUUID();
