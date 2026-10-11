function tier(pMiss: number | null): { label: string; className: string } {
  if (pMiss === null) {
    return { label: "Not yet assessed", className: "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400" };
  }
  if (pMiss >= 0.5) {
    return { label: "High risk", className: "bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-400" };
  }
  if (pMiss >= 0.2) {
    return { label: "Elevated risk", className: "bg-amber-50 text-amber-800 dark:bg-amber-950 dark:text-amber-400" };
  }
  return { label: "On track", className: "bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-400" };
}

export function RiskBadge({ pMiss }: { pMiss: number | null }) {
  const { label, className } = tier(pMiss);
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ${className}`}>
      <span className="h-1.5 w-1.5 rounded-full bg-current" />
      {label}
      {pMiss !== null && (
        <span className="tabular-nums font-semibold">{Math.round(pMiss * 100)}%</span>
      )}
    </span>
  );
}
