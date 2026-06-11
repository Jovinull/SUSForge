import { CalendarRange, Database } from 'lucide-react';

export function Header({ ano }: { ano: number }) {
  return (
    <header className="flex items-start justify-between mb-8">
      <div>
        <p className="text-xs uppercase tracking-[0.2em] text-zinc-500 mb-1">
          Visão Executiva
        </p>
        <h1 className="text-3xl font-semibold text-zinc-50">
          Capacidade Hospitalar SUS
        </h1>
        <p className="text-sm text-zinc-400 mt-1">
          Indicadores consolidados sobre a rede pública de saúde
        </p>
      </div>
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-zinc-900 ring-1 ring-zinc-800 text-xs text-zinc-300">
          <CalendarRange className="w-3.5 h-3.5 text-blue-400" />
          <span>
            Ano de referência: <strong className="text-zinc-100">{ano}</strong>
          </span>
        </div>
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-500/10 ring-1 ring-emerald-500/30 text-xs text-emerald-300">
          <Database className="w-3.5 h-3.5" />
          <span>
            Live · <strong>gold</strong>
          </span>
        </div>
      </div>
    </header>
  );
}
