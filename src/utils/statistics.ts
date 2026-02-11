export const mean = (values: number[]): number => {
  if (values.length === 0) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
};

export const stdDev = (values: number[]): number => {
  if (values.length < 2) return 0;
  const mu = mean(values);
  const variance = values.reduce((acc, value) => acc + (value - mu) ** 2, 0) / values.length;
  return Math.sqrt(variance);
};

export const zScore = (value: number, population: number[]): number => {
  if (population.length === 0) return 0;
  const sigma = stdDev(population);
  if (sigma === 0) return 0;
  return (value - mean(population)) / sigma;
};

export const clamp = (value: number, low: number, high: number): number => {
  return Math.max(low, Math.min(high, value));
};

export const sigmoid = (value: number): number => {
  return 1 / (1 + Math.exp(-value));
};
