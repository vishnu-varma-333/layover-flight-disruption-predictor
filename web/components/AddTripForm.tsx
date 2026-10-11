"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { Flight } from "@/lib/types";
import { formatTime } from "@/lib/format";

interface PickedLeg {
  flight: Flight;
  minConnectionMin: string;
}

const DEMO_DATE = "2026-07-15";

export function AddTripForm() {
  const router = useRouter();
  const [carrier, setCarrier] = useState("");
  const [flightNumber, setFlightNumber] = useState("");
  const [flDate, setFlDate] = useState(DEMO_DATE);
  const [results, setResults] = useState<Flight[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [legs, setLegs] = useState<PickedLeg[]>([]);
  const [creating, setCreating] = useState(false);

  async function search(e: React.FormEvent) {
    e.preventDefault();
    setSearching(true);
    setSearchError(null);
    setResults(null);
    const qs = new URLSearchParams({ carrier, flight_number: flightNumber, fl_date: flDate });
    const res = await fetch(`/api/flights/search?${qs.toString()}`);
    if (!res.ok) {
      setSearchError("No matching flight found for that carrier, number, and date.");
      setSearching(false);
      return;
    }
    setResults(await res.json());
    setSearching(false);
  }

  function addLeg(flight: Flight) {
    if (legs.some((l) => l.flight.flight_id === flight.flight_id)) return;
    setLegs([...legs, { flight, minConnectionMin: "45" }]);
    setResults(null);
  }

  function removeLeg(flightId: string) {
    setLegs(legs.filter((l) => l.flight.flight_id !== flightId));
  }

  async function createTrip() {
    setCreating(true);
    const res = await fetch("/api/trips", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        legs: legs.map((l, i) => ({
          flight_id: l.flight.flight_id,
          min_connection_min: i < legs.length - 1 ? Number(l.minConnectionMin) : null,
        })),
      }),
    });
    const body = await res.json();
    setCreating(false);
    if (res.ok) router.push(`/trips/${body.trip_id}`);
  }

  return (
    <div className="flex flex-col gap-8">
      <form onSubmit={search} className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-zinc-500">Carrier</label>
          <input
            required
            value={carrier}
            onChange={(e) => setCarrier(e.target.value.toUpperCase())}
            placeholder="UA"
            maxLength={2}
            className="w-20 rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm uppercase text-zinc-900 outline-none focus:border-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-zinc-500">Flight number</label>
          <input
            required
            type="number"
            value={flightNumber}
            onChange={(e) => setFlightNumber(e.target.value)}
            placeholder="539"
            className="w-24 rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-900 outline-none focus:border-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-zinc-500">Date</label>
          <input
            required
            type="date"
            value={flDate}
            onChange={(e) => setFlDate(e.target.value)}
            className="rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-900 outline-none focus:border-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100"
          />
        </div>
        <button
          type="submit"
          disabled={searching}
          className="rounded-md border border-zinc-300 px-3 py-2 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-900"
        >
          {searching ? "Searching..." : "Search"}
        </button>
      </form>
      <p className="-mt-5 text-xs text-zinc-400">
        Live risk is only available for {DEMO_DATE}, the demo day this project replays.
      </p>

      {searchError && <p className="text-sm text-red-600 dark:text-red-400">{searchError}</p>}

      {results && results.length > 0 && (
        <div className="rounded-md border border-zinc-200 dark:border-zinc-800">
          <p className="border-b border-zinc-200 px-4 py-2 text-xs font-medium text-zinc-500 dark:border-zinc-800">
            {results.length > 1
              ? "This flight number covers more than one leg today — pick the right one:"
              : "Match found:"}
          </p>
          <ul className="divide-y divide-zinc-200 dark:divide-zinc-800">
            {results.map((flight) => (
              <li key={flight.flight_id} className="flex items-center justify-between px-4 py-3">
                <span className="text-sm text-zinc-700 dark:text-zinc-300">
                  <span className="font-mono">{flight.origin} → {flight.dest}</span>{" "}
                  <span className="text-zinc-400">
                    {formatTime(flight.sched_dep_utc)} – {formatTime(flight.sched_arr_utc)}
                  </span>
                </span>
                <button
                  onClick={() => addLeg(flight)}
                  className="rounded-md bg-zinc-900 px-2.5 py-1 text-xs font-medium text-white hover:bg-zinc-800 dark:bg-white dark:text-zinc-900 dark:hover:bg-zinc-200"
                >
                  Add leg
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      {legs.length > 0 && (
        <div>
          <h2 className="text-sm font-medium text-zinc-700 dark:text-zinc-300">Trip legs</h2>
          <ul className="mt-3 flex flex-col gap-2">
            {legs.map((leg, i) => (
              <li
                key={leg.flight.flight_id}
                className="flex items-center justify-between rounded-md border border-zinc-200 px-4 py-3 dark:border-zinc-800"
              >
                <span className="text-sm">
                  <span className="font-mono text-zinc-700 dark:text-zinc-300">
                    {leg.flight.carrier}{leg.flight.flight_number} {leg.flight.origin} → {leg.flight.dest}
                  </span>
                </span>
                <div className="flex items-center gap-3">
                  {i < legs.length - 1 && (
                    <label className="flex items-center gap-1.5 text-xs text-zinc-500">
                      Min. connection
                      <input
                        type="number"
                        value={leg.minConnectionMin}
                        onChange={(e) => {
                          const next = [...legs];
                          next[i] = { ...leg, minConnectionMin: e.target.value };
                          setLegs(next);
                        }}
                        className="w-14 rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs text-zinc-900 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100"
                      />
                      min
                    </label>
                  )}
                  <button
                    onClick={() => removeLeg(leg.flight.flight_id)}
                    className="text-xs text-zinc-400 hover:text-red-600 dark:hover:text-red-400"
                  >
                    Remove
                  </button>
                </div>
              </li>
            ))}
          </ul>
          <button
            onClick={createTrip}
            disabled={creating}
            className="mt-5 rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-zinc-800 disabled:opacity-50 dark:bg-white dark:text-zinc-900 dark:hover:bg-zinc-200"
          >
            {creating ? "Creating trip..." : "Start monitoring this trip"}
          </button>
        </div>
      )}
    </div>
  );
}
