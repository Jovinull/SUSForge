import { BedDouble, Hospital, Stethoscope, Users } from 'lucide-react';
import { Header } from './components/Header';
import { KpiCard } from './components/KpiCard';
import { LeitosChart } from './components/LeitosChart';
import { Sidebar } from './components/Sidebar';
import { TopHospitais } from './components/TopHospitais';
import { fmtCompact, fmtInt } from '@/lib/format';
import {
  getKpis,
  getLeitosSusPorUf,
  getTopHospitaisPorUti,
} from '@/lib/queries';

// Server Component — busca direto na Camada Gold a cada request.
// Em produção pode-se cachear via `revalidate` ou ISR.
export const dynamic = 'force-dynamic';

export default async function DashboardPage() {
  const { kpis, ano } = await getKpis();
  const [ufRanking, topHospitais] = await Promise.all([
    getLeitosSusPorUf(ano, 10),
    getTopHospitaisPorUti(ano, 8),
  ]);

  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 px-10 py-8 overflow-x-hidden">
        <Header ano={ano} />

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <KpiCard
            icon={Hospital}
            label="Estabelecimentos ativos"
            value={fmtInt(kpis.estab_ativos)}
            hint="dim_estabelecimento · is_ativo"
            accent="blue"
          />
          <KpiCard
            icon={Stethoscope}
            label="Hospitais com atendimento"
            value={fmtInt(kpis.hospitais)}
            hint="st_atend_hospitalar = TRUE"
            accent="emerald"
          />
          <KpiCard
            icon={Users}
            label="Unidades Básicas (UBS)"
            value={fmtInt(kpis.total_ubs)}
            hint="dim_estabelecimento · is_ubs"
            accent="violet"
          />
          <KpiCard
            icon={BedDouble}
            label={`Leitos SUS · média ${ano}`}
            value={fmtCompact(kpis.leitos_sus)}
            hint="fato_leitos_anual · leitos_sus_avg"
            accent="amber"
          />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2">
            <LeitosChart data={ufRanking} ano={ano} />
          </div>
          <div className="lg:col-span-1">
            <TopHospitais data={topHospitais} ano={ano} />
          </div>
        </div>

        <footer className="mt-12 pt-6 border-t border-zinc-900 text-[11px] text-zinc-600 flex items-center justify-between">
          <span>SUSForge · Data Warehouse Medalhão · OpenDATASUS</span>
          <span>Server Component · pg Pool · live query</span>
        </footer>
      </main>
    </div>
  );
}
