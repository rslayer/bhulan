import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(n: number, digits = 2): string {
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function formatMinutes(min: number): string {
  if (!Number.isFinite(min) || min <= 0) return "0 min";
  if (min < 1) return `${Math.round(min * 60)} s`;
  if (min < 60) return `${min.toFixed(1)} min`;
  const totalRounded = Math.round(min);
  const hours = Math.floor(totalRounded / 60);
  const rem = totalRounded % 60;
  return `${hours} h ${rem} min`;
}
