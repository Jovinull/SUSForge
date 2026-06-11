import type { TopHospital } from '@/lib/queries';
import { fmtInt } from '@/lib/format';

export function TopHospitais({
  data,
  ano,
}: {
  data: TopHospital[];
  ano: number;
}) {
  return (
    <div className="rounded-xl bg-zinc-900/70 ring-1 ring-zinc-800/80 backdrop-blur p-6 h-full">
      <div className="mb-5">
        <h2 className="text-base font-semibold text-zinc-100">
          Top Hospitais por UTI
        </h2>
        <p className="text-xs text-zinc-500">Pico anual · {ano}</p>
      </div>
      <ol className="space-y-3">
        {data.map((h, i) => (
          <li
            key={h.co_cnes}
            className="flex items-center gap-3 py-2 border-b border-zinc-800/60 last:border-0"
          >
            <span className="w-6 h-6 grid place-items-center rounded-md bg-zinc-800 text-xs text-zinc-400 font-medium tabular-nums">
              {i + 1}
            </span>
            <div className="flex-1 min-w-0">
              <p className="text-sm text-zinc-100 truncate">
                {h.no_fantasia ?? 'Sem nome'}
              </p>
              <p className="text-[11px] text-zinc-500">
                {h.sg_uf ?? '—'} · CNES {h.co_cnes}
              </p>
            </div>
            <div className="text-right tabular-nums">
              <p className="text-sm font-semibold text-emerald-400">
                {fmtInt(h.uti_total_max)}
              </p>
              <p className="text-[10px] text-zinc-500">UTI</p>
            </div>
          </li>
        ))}
      </ol>
    </div>
  );
}
