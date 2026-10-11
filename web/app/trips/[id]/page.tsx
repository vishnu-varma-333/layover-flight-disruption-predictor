import { notFound, redirect } from "next/navigation";
import { backendFetch, BackendError } from "@/lib/backend";
import { formatMinutes, formatTime } from "@/lib/format";
import { getUserId } from "@/lib/session";
import { Flight, Trip } from "@/lib/types";
import { AircraftChain } from "@/components/AircraftChain";
import { PushSubscribeButton } from "@/components/PushSubscribeButton";
import { RiskBadge } from "@/components/RiskBadge";
import { StopTripButton } from "@/components/StopTripButton";
import { DeleteTripButton } from "@/components/DeleteTripButton";

export default async function TripDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const userId = await getUserId();
  if (!userId) redirect("/");
  const { id } = await params;

  let trip: Trip;
  try {
    trip = await backendFetch<Trip>(`/trips/${id}`);
  } catch (err) {
    if (err instanceof BackendError && err.status === 404) notFound();
    throw err;
  }
  if (trip.user_id !== userId) notFound();

  const chains = await Promise.all(
    trip.legs.map(async (leg) => {
      if (leg.min_connection_min === null) return null; // last leg: nothing to protect downstream
      try {
        const { chain } = await backendFetch<{ flight: Flight; chain: Flight[] }>(
          `/flights/${leg.flight_id}`
        );
        return chain;
      } catch {
        return null;
      }
    })
  );

  return (
    <div className="mx-auto max-w-3xl px-6 py-10">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="font-mono text-lg font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
            {trip.id}
          </h1>
          <p className="mt-1 text-sm capitalize text-zinc-500 dark:text-zinc-400">{trip.status}</p>
        </div>
        <div className="flex flex-col items-end gap-2">
          {trip.status === "active" && <StopTripButton tripId={trip.id} />}
          <DeleteTripButton tripId={trip.id} />
        </div>
      </div>

      {trip.status === "active" && (
        <div className="mt-6">
          <PushSubscribeButton tripId={trip.id} />
        </div>
      )}

      <ol className="mt-8 flex flex-col gap-6">
        {trip.legs.map((leg, i) => (
          <li key={leg.id} className="rounded-lg border border-zinc-200 p-5 dark:border-zinc-800">
            <div className="flex items-start justify-between">
              <div>
                <p className="font-mono text-sm font-medium text-zinc-900 dark:text-zinc-100">
                  {leg.flight.carrier}{leg.flight.flight_number} &middot; {leg.flight.origin} → {leg.flight.dest}
                </p>
                <p className="mt-0.5 text-xs tabular-nums text-zinc-500">
                  {formatTime(leg.flight.sched_dep_utc)} – {formatTime(leg.flight.sched_arr_utc)}
                </p>
              </div>
              {leg.min_connection_min !== null && <RiskBadge pMiss={leg.risk?.p_miss ?? null} />}
            </div>

            {leg.risk && (
              <div className="mt-4 border-t border-zinc-100 pt-4 dark:border-zinc-800">
                <p className="text-sm text-zinc-700 dark:text-zinc-300">
                  Predicted arrival delay:{" "}
                  <span className="font-medium tabular-nums">
                    {formatMinutes(leg.risk.predicted_delay)}
                  </span>
                </p>
                <ul className="mt-2 flex flex-col gap-1">
                  {leg.risk.explanation.map((line, j) => (
                    <li key={j} className="text-xs text-zinc-500 dark:text-zinc-400">
                      {line}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {leg.min_connection_min !== null && (
              <p className="mt-3 text-xs text-zinc-400">
                Minimum connection time: {leg.min_connection_min} min
              </p>
            )}

            {chains[i] && <AircraftChain chain={chains[i]!} highlightFlightId={leg.flight_id} />}
          </li>
        ))}
      </ol>
    </div>
  );
}
