'use client';

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Cell,
} from 'recharts';
import type { UfRanking } from '@/lib/queries';

const BAR_COLORS = [
  '#3b82f6', '#60a5fa', '#22d3ee', '#10b981', '#34d399',
  '#a3e635', '#fbbf24', '#f59e0b', '#fb7185', '#a78bfa',
];

export function LeitosChart({
  data,
  ano,
}: {
  data: UfRanking[];
  ano: number;
}) {
  return (
    <div className="rounded-xl bg-zinc-900/70 ring-1 ring-zinc-800/80 backdrop-blur p-6">
      <div className="mb-5 flex items-baseline justify-between">
        <div>
          <h2 className="text-base font-semibold text-zinc-100">
            Leitos SUS por UF
          </h2>
          <p className="text-xs text-zinc-500">
            Média mensal · Top {data.length} estados · {ano}
          </p>
        </div>
        <span className="text-[10px] uppercase tracking-wider text-zinc-500">
          gold.fato_leitos_anual
        </span>
      </div>

      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data} margin={{ top: 8, right: 8, left: -16, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
          <XAxis
            dataKey="sg_uf"
            stroke="#71717a"
            tick={{ fontSize: 11 }}
            tickLine={false}
            axisLine={{ stroke: '#27272a' }}
          />
          <YAxis
            stroke="#71717a"
            tick={{ fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            tickFormatter={(v: number) =>
              Intl.NumberFormat('pt-BR', {
                notation: 'compact',
                maximumFractionDigits: 1,
              }).format(v)
            }
          />
          <Tooltip
            cursor={{ fill: 'rgba(63, 63, 70, 0.25)' }}
            contentStyle={{
              background: '#0a0a0b',
              border: '1px solid #27272a',
              borderRadius: 10,
              fontSize: 12,
              padding: '8px 12px',
            }}
            labelStyle={{ color: '#fafafa', fontWeight: 600, marginBottom: 4 }}
            formatter={(v: number) =>
              Intl.NumberFormat('pt-BR').format(v) + ' leitos SUS'
            }
          />
          <Bar dataKey="leitos_sus" radius={[6, 6, 0, 0]}>
            {data.map((_, i) => (
              <Cell key={i} fill={BAR_COLORS[i % BAR_COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
