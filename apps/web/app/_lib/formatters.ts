export function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "Unavailable";
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatRate(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "Unavailable";
  }

  return `${(value * 100).toFixed(3)}%`;
}

export function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "Unavailable";
  }

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: value % 1 === 0 ? 0 : 2,
  }).format(value);
}

export function formatLabel(value: string): string {
  return value.replaceAll("_", " ");
}
