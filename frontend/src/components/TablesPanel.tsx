import type { TableInfo } from '../types'

type Props = {
    loading?: boolean
    tables: TableInfo[]
}

export function TablesPanel({ loading, tables }: Props) {
    if (loading) return <div className="muted">Cargando tablas…</div>
    if (!tables?.length) return <div className="muted">Sin tablas</div>

    return (
        <div className="panel" style={{ gap: 8 }}>
            {tables.map((t) => (
                <div
                    key={t.name}
                    style={{
                        border: '1px solid var(--border)',
                        background: 'linear-gradient(180deg, #161a27, #121521)',
                        borderRadius: 10,
                        padding: 10,
                    }}
                >
                    <div className="row" style={{ justifyContent: 'space-between' }}>
                        <strong>{t.name}</strong>
                        <span className="muted">{t.features} cols</span>
                    </div>
                    <div className="muted">{t.rows.toLocaleString()} rows</div>
                </div>
            ))}
        </div>
    )
}
