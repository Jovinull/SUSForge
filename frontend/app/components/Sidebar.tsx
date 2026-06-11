import {
  Activity,
  FileBarChart,
  Hospital,
  LayoutDashboard,
  Syringe,
} from 'lucide-react';

const NAV_ITEMS = [
  { icon: LayoutDashboard, label: 'Dashboard', active: true },
  { icon: Hospital, label: 'Hospitais & Leitos' },
  { icon: Syringe, label: 'Vacinação' },
  { icon: Activity, label: 'COVID-19' },
  { icon: FileBarChart, label: 'Relatórios' },
];

export function Sidebar() {
  return (
    <aside className="w-64 shrink-0 bg-zinc-950/80 backdrop-blur border-r border-zinc-800 px-5 py-6 flex flex-col">
      <div className="mb-10">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-emerald-400 grid place-items-center text-white font-bold text-sm">
            S
          </div>
          <div>
            <h1 className="text-base font-semibold text-zinc-100 leading-tight">
              SUSForge
            </h1>
            <p className="text-[10px] uppercase tracking-wider text-zinc-500">
              BI Enterprise
            </p>
          </div>
        </div>
      </div>

      <nav className="flex-1 space-y-1">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;
          const cls = item.active
            ? 'bg-blue-500/10 text-blue-300 ring-1 ring-blue-500/20'
            : 'text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200';
          return (
            <a
              key={item.label}
              className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition cursor-default ${cls}`}
            >
              <Icon className="w-4 h-4" />
              <span>{item.label}</span>
            </a>
          );
        })}
      </nav>

      <div className="mt-6 pt-6 border-t border-zinc-800/80">
        <div className="text-[11px] text-zinc-500 leading-relaxed">
          <p className="text-zinc-300 font-medium">Data Warehouse Medalhão</p>
          <p>Bronze · Silver · Gold</p>
          <p className="mt-2 text-zinc-600">v0.1 · 2026</p>
        </div>
      </div>
    </aside>
  );
}
