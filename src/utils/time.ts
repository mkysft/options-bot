export const nowIso = (): string => new Date().toISOString();

export const daysBetween = (fromIso: string, toIso: string): number => {
  const from = new Date(fromIso).getTime();
  const to = new Date(toIso).getTime();
  return Math.floor((to - from) / (1000 * 60 * 60 * 24));
};

export const addDaysIso = (baseIso: string, days: number): string => {
  const dt = new Date(baseIso);
  dt.setUTCDate(dt.getUTCDate() + days);
  return dt.toISOString();
};
