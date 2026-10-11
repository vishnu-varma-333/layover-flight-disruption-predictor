import { notFound } from "next/navigation";
import { backendFetch, BackendError } from "@/lib/backend";
import { formatMinutes, formatTime } from "@/lib/format";
import { FlightDetail } from "@/lib/types";
import { AircraftChain } from "@/components/AircraftChain";

export default async function FlightPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  let detail: FlightDetail;
  try {
    detail = await backendFetch<FlightDetail>(`/flights/${id}`);
  } catch (err) {
    if (err instanceof BackendError && err.status === 404) notFound();
    throw err;
  }

  const { flight, chain, risk } = detail;

  return (
    <div className="mx-auto max-w-2xl px-6 py-10">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-400">Flight status</p>
      <h1 className="mt-1 font-mono text-xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
        {flight.carrier}{flight.flight_number} &middot; {flight.origin} → {flight.dest}
      </h1>
      <p className="mt-1 text-sm tabular-nums text-zinc-500">
        Scheduled {formatTime(flight.sched_dep_utc)} – {formatTime(flight.sched_arr_utc)}
      </p>

      <div className="mt-8 rounded-lg border border-zinc-200 p-5 dark:border-zinc-800">
        {risk ? (
          <>
            <p className="text-sm text-zinc-700 dark:text-zinc-300">
              Predicted arrival delay:{" "}
              <span className="font-medium tabular-nums">{formatMinutes(risk.predicted_delay)}</span>
            </p>
            <ul className="mt-3 flex flex-col gap-1.5">
              {risk.explanation.map((line, i) => (
                <li key={i} className="text-sm text-zinc-500 dark:text-zinc-400">
                  {line}
                </li>
              ))}
            </ul>
          </>
        ) : (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            No live risk assessment available for this flight.
          </p>
        )}
      </div>

      <AircraftChain chain={chain} highlightFlightId={flight.flight_id} />
    </div>
  );
}
