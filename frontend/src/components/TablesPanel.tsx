import type { TableInfo } from '../types'

type Props = {
    loading?: boolean
    tables: TableInfo[]

    onOpen: (tableName: string) => void

}

export function TablesPanel({ loading, tables, onOpen }: Props) {
    if (loading) return <div className="muted">Cargando tablas…</div>
    if (!tables?.length) return <div className="muted">Sin tablas</div>

    return (
        <div className="panel" style={{ gap: 8 }}>
            {tables.map((t) => {
                const featureCount =
                    typeof (t as any).features === 'number'
                        ? (t as any).features
                        : Array.isArray((t as any).features)
                            ? (t as any).features.length
                            : 0

                const rowCount =
                    typeof (t as any).rows === 'number' ? (t as any).rows : 0

                return (
                    <div
                        key={t.name}
                        style={{
                            border: '1px solid var(--border)',
                            background: 'linear-gradient(180deg, #161a27, #121521)',
                            borderRadius: 10,
                            padding: 10,
                        }}
                    >
                        <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                            <div style={{ display: 'flex', flexDirection: 'column' }}>
                                <strong style={{ fontSize: 14 }}>{t.name}</strong>
                                <span className="muted" style={{ fontSize: 12 }}>
                                    {rowCount.toLocaleString()} rows • {featureCount} cols
                                </span>
                            </div>

                            <button
                                className="button"
                                onClick={() => onOpen(t.name)}
                                title="Ver datos: SELECT * FROM <tabla>"
                                style={{ minWidth: 90 }}
                            >
                                Detalle
                            </button>
                        </div>
                    </div>
                )
            })}
        </div>
    )
}