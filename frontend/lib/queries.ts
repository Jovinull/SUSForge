/**
 * Consultas SQL contra a Camada Gold.
 * Cada função retorna o shape exato que o Server Component espera.
 */
import { query } from './db';

export interface Kpis {
  estab_ativos: number;
  total_ubs: number;
  hospitais: number;
  leitos_sus: number;
}

export interface UfRanking {
  sg_uf: string;
  ds_uf: string | null;
  leitos_sus: number;
  uti_sus: number;
  hospitais: number;
}

export interface TopHospital {
  co_cnes: string;
  no_fantasia: string | null;
  sg_uf: string | null;
  uti_total_max: number;
  leitos_existentes_max: number;
}

/**
 * Ano de referência mais recente disponível no fato de leitos.
 * Usado como filtro padrão para os KPIs e rankings.
 */
async function latestYear(): Promise<number> {
  const rows = await query<{ ano: number }>(
    `SELECT max(ano_referencia)::int AS ano FROM gold.fato_leitos_anual`,
  );
  return rows[0]?.ano ?? new Date().getFullYear();
}

export async function getKpis(): Promise<{ kpis: Kpis; ano: number }> {
  const ano = await latestYear();
  const dim = await query<{
    estab_ativos: string;
    total_ubs: string;
    hospitais: string;
  }>(
    `SELECT count(*) FILTER (WHERE is_ativo)             AS estab_ativos,
            count(*) FILTER (WHERE is_ubs)               AS total_ubs,
            count(*) FILTER (WHERE st_atend_hospitalar)  AS hospitais
       FROM gold.dim_estabelecimento`,
  );
  const fato = await query<{ leitos_sus: string }>(
    `SELECT round(sum(leitos_sus_avg))::bigint AS leitos_sus
       FROM gold.fato_leitos_anual
      WHERE ano_referencia = $1`,
    [ano],
  );
  return {
    ano,
    kpis: {
      estab_ativos: Number(dim[0]?.estab_ativos ?? 0),
      total_ubs: Number(dim[0]?.total_ubs ?? 0),
      hospitais: Number(dim[0]?.hospitais ?? 0),
      leitos_sus: Number(fato[0]?.leitos_sus ?? 0),
    },
  };
}

export async function getLeitosSusPorUf(
  ano: number,
  limit = 10,
): Promise<UfRanking[]> {
  return query<UfRanking>(
    `SELECT d.sg_uf,
            d.ds_uf,
            round(sum(f.leitos_sus_avg))::int  AS leitos_sus,
            round(sum(f.uti_sus_avg))::int     AS uti_sus,
            count(DISTINCT d.co_cnes)::int     AS hospitais
       FROM gold.fato_leitos_anual f
       JOIN gold.dim_estabelecimento d ON d.co_cnes = f.co_cnes
      WHERE f.ano_referencia = $1
        AND d.sg_uf IS NOT NULL
      GROUP BY d.sg_uf, d.ds_uf
      ORDER BY leitos_sus DESC
      LIMIT $2`,
    [ano, limit],
  );
}

export async function getTopHospitaisPorUti(
  ano: number,
  limit = 8,
): Promise<TopHospital[]> {
  return query<TopHospital>(
    `SELECT f.co_cnes,
            d.no_fantasia,
            d.sg_uf,
            f.uti_total_max,
            f.leitos_existentes_max
       FROM gold.fato_leitos_anual f
       JOIN gold.dim_estabelecimento d ON d.co_cnes = f.co_cnes
      WHERE f.ano_referencia = $1
      ORDER BY f.uti_total_max DESC
      LIMIT $2`,
    [ano, limit],
  );
}
