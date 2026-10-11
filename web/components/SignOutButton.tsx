"use client";

import { useRouter } from "next/navigation";

export function SignOutButton() {
  const router = useRouter();
  return (
    <button
      onClick={async () => {
        await fetch("/api/session", { method: "DELETE" });
        router.push("/");
        router.refresh();
      }}
      className="text-zinc-500 underline-offset-2 hover:text-zinc-900 hover:underline dark:hover:text-zinc-100"
    >
      Sign out
    </button>
  );
}
