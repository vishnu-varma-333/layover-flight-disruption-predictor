import { Flight } from "@/lib/types";
import { formatTime } from "@/lib/format";

export function AircraftChain({ chain, highlightFlightId }: { chain: Flight[]; highlightFlightId: string }) {
  if (chain.length <= 1) return null;
  const tail = chain[0].tail_number;

  return (
    <div className="mt-4 rounded-md border border-zinc-200 p-4 dark:border-zinc-800">
      <p className="text-xs font-medium text-zinc-500">
        Aircraft {tail ?? "unknown"}&apos;s flights today
      </p>
      <ol className="mt-3 flex flex-col gap-2">
        {chain.map((flight) => {
          const isThisLeg = flight.flight_id === highlightFlightId;
          return (
            <li
              key={flight.flight_id}
              className={`flex items-center justify-between rounded px-2 py-1.5 text-sm ${
                isThisLeg
                  ? "bg-zinc-100 font-medium text-zinc-900 dark:bg-zinc-800 dark:text-zinc-100"
                  : "text-zinc-500 dark:text-zinc-400"
              }`}
            >
              <span className="font-mono">
                {flight.carrier}{flight.flight_number} {flight.origin} → {flight.dest}
              </span>
              <span className="tabular-nums">{formatTime(flight.sched_dep_utc)}</span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}
