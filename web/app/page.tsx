import Link from "next/link";
import { backendFetch } from "@/lib/backend";
import { formatDate } from "@/lib/format";
import { getUserId } from "@/lib/session";
import { TripSummary } from "@/lib/types";
import { SignInForm } from "@/components/SignInForm";

export default async function HomePage() {
  const userId = await getUserId();

  if (!userId) {
    return (
      <div className="mx-auto flex max-w-3xl flex-col items-center px-6 py-24 text-center">
        <h1 className="text-2xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
          Know you&apos;ll miss your connection before the airline tells you
        </h1>
        <p className="mt-3 max-w-md text-sm text-zinc-500 dark:text-zinc-400">
          Layover tracks the physical aircraft behind your flight and alerts you the moment a
          connection is genuinely at risk.
        </p>
        <div className="mt-8">
          <SignInForm />
        </div>
      </div>
    );
  }

  const trips = await backendFetch<TripSummary[]>(`/trips?user_id=${encodeURIComponent(userId)}`);

  return (
    <div className="mx-auto max-w-3xl px-6 py-10">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
          Your trips
        </h1>
        <Link
          href="/trips/new"
          className="rounded-md bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white transition hover:bg-zinc-800 dark:bg-white dark:text-zinc-900 dark:hover:bg-zinc-200"
        >
          Add a trip
        </Link>
      </div>

      {trips.length === 0 ? (
        <p className="mt-10 text-sm text-zinc-500 dark:text-zinc-400">
          No trips yet. Add one by flight number and date to start monitoring it.
        </p>
      ) : (
        <ul className="mt-6 divide-y divide-zinc-200 dark:divide-zinc-800">
          {trips.map((trip) => (
            <li key={trip.id}>
              <Link
                href={`/trips/${trip.id}`}
                className="flex items-center justify-between py-4 text-sm hover:text-zinc-600 dark:hover:text-zinc-300"
              >
                <span className="font-mono text-zinc-700 dark:text-zinc-300">{trip.id}</span>
                <span className="flex items-center gap-4 text-zinc-400">
                  <span className="capitalize">{trip.status}</span>
                  <span>{formatDate(trip.created_at)}</span>
                </span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
