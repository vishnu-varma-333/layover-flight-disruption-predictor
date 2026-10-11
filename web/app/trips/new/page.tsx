import { redirect } from "next/navigation";
import { getUserId } from "@/lib/session";
import { AddTripForm } from "@/components/AddTripForm";

export default async function NewTripPage() {
  const userId = await getUserId();
  if (!userId) redirect("/");

  return (
    <div className="mx-auto max-w-3xl px-6 py-10">
      <h1 className="text-lg font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
        Add a trip
      </h1>
      <p className="mt-1 text-sm text-zinc-500 dark:text-zinc-400">
        Search for each leg by carrier, flight number, and date, then add them in order.
      </p>
      <div className="mt-8">
        <AddTripForm />
      </div>
    </div>
  );
}
