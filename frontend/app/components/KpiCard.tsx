import type { LucideIcon } from 'lucide-react';

interface KpiCardProps {
  icon: LucideIcon;
  label: string;
  value: string;
  hint?: string;
  accent?: 'blue' | 'emerald' | 'amber' | 'violet';
}

const ACCENTS: Record<NonNullable<KpiCardProps['accent']>, string> = {
  blue: 'text-blue-400 bg-blue-500/10 ring-blue-500/30',
  emerald: 'text-emerald-400 bg-emerald-500/10 ring-emerald-500/30',
  amber: 'text-amber-400 bg-amber-500/10 ring-amber-500/30',
  violet: 'text-violet-400 bg-violet-500/10 ring-violet-500/30',
};

export function KpiCard({
  icon: Icon,
  label,
  value,
  hint,
  accent = 'blue',
}: KpiCardProps) {
  return (
    <div className="group relative rounded-xl bg-zinc-900/70 ring-1 ring-zinc-800/80 backdrop-blur px-5 py-4 hover:ring-zinc-700 transition">
      <div className="flex items-start justify-between mb-3">
        <p className="text-xs uppercase tracking-wider text-zinc-500">{label}</p>
        <div
          className={`grid place-items-center w-8 h-8 rounded-lg ring-1 ${ACCENTS[accent]}`}
        >
          <Icon className="w-4 h-4" />
        </div>
      </div>
      <p className="text-2xl font-semibold text-zinc-50 tabular-nums">{value}</p>
      {hint && <p className="text-xs text-zinc-500 mt-1">{hint}</p>}
    </div>
  );
}
