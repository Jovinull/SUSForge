import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'SUSForge BI · Data Warehouse',
  description:
    'BI Enterprise sobre o Data Warehouse Medalhão de saúde pública (OpenDATASUS).',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="pt-BR" className="dark">
      <body className="min-h-screen bg-zinc-950 text-zinc-100 antialiased">
        <div className="relative z-10">{children}</div>
      </body>
    </html>
  );
}
