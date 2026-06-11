/** Formatadores numéricos em pt-BR. */

const intFmt = new Intl.NumberFormat('pt-BR');
const compactFmt = new Intl.NumberFormat('pt-BR', {
  notation: 'compact',
  maximumFractionDigits: 1,
});

export function fmtInt(n: number | bigint | null | undefined): string {
  if (n === null || n === undefined) return '—';
  return intFmt.format(typeof n === 'bigint' ? Number(n) : n);
}

export function fmtCompact(n: number | bigint | null | undefined): string {
  if (n === null || n === undefined) return '—';
  return compactFmt.format(typeof n === 'bigint' ? Number(n) : n);
}
