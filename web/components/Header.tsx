import Link from "next/link";
import { getUserId } from "@/lib/session";
import { SignOutButton } from "./SignOutButton";

export async function Header() {
  const userId = await getUserId();
  return (
    <header className="border-b border-zinc-200 dark:border-zinc-800">
      <div className="mx-auto flex max-w-3xl items-center justify-between px-6 py-4">
        <Link href="/" className="text-sm font-semibold tracking-tight text-zinc-900 dark:text-zinc-100">
          Layover
        </Link>
        {userId && (
          <div className="flex items-center gap-4 text-sm text-zinc-500 dark:text-zinc-400">
            <span>{userId}</span>
            <SignOutButton />
          </div>
        )}
      </div>
    </header>
  );
}
