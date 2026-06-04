import { clsx } from "clsx";
import { type ComponentProps, useState } from "react";
import { RefreshCw, Copy } from "lucide-react";

export function Button({
  variant = "primary",
  className,
  ...props
}: ComponentProps<"button"> & { variant?: "primary" | "secondary" | "ghost" }) {
  return (
    <button
      className={clsx(
        "inline-flex items-center justify-center gap-2 rounded-md px-4 py-2 text-sm font-medium transition-colors disabled:opacity-50 disabled:pointer-events-none",
        variant === "primary" && "bg-blue-600 text-white hover:bg-blue-700",
        variant === "secondary" && "border border-gray-300 bg-white text-gray-700 hover:bg-gray-50",
        variant === "ghost" && "text-gray-600 hover:bg-gray-100",
        className,
      )}
      {...props}
    />
  );
}

export function Select({ className, ...props }: ComponentProps<"select">) {
  return (
    <select
      className={clsx(
        "w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 disabled:bg-gray-100 disabled:text-gray-500",
        className,
      )}
      {...props}
    />
  );
}

export function ProgressBar({ value, label }: { value: number; label?: string }) {
  return (
    <div className="space-y-1">
      {label && <p className="text-xs text-gray-500">{label}</p>}
      <div className="h-2 w-full overflow-hidden rounded-full bg-gray-200">
        <div
          className="h-full rounded-full bg-blue-600 transition-all duration-300"
          style={{ width: `${Math.min(100, value)}%` }}
        />
      </div>
    </div>
  );
}

export function Card({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      className={clsx("rounded-lg border border-gray-200 bg-white p-6 shadow-sm", className)}
      {...props}
    />
  );
}

export function RefreshButton({ onClick }: { onClick: () => void }) {
  const [spinning, setSpinning] = useState(false);
  return (
    <button
      onClick={() => { setSpinning(true); onClick(); setTimeout(() => setSpinning(false), 500); }}
      className="rounded p-2 text-gray-500 hover:bg-gray-100"
    >
      <RefreshCw className={`h-4 w-4 transition-transform duration-500 ${spinning ? "rotate-180" : ""}`} />
    </button>
  );
}

export function CopyButton({ text, label = "Copy" }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <Button variant="ghost" onClick={() => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500); }}>
      <Copy className="h-3 w-3" /> {copied ? "Copied!" : label}
    </Button>
  );
}
