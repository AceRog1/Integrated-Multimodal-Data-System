type Props = {
    elapsedMs: number
    tableName: string
    rowCount: number
    featureCount: number
}

export function StatsBar({ elapsedMs, tableName, rowCount, featureCount }: Props) {
    return (
        <section className="stats">
            <div className="kpi">
                <div className="kpi-label">Tiempo (ms)</div>
                <div className="kpi-value kpi-green">{elapsedMs.toFixed(2)}</div>
            </div>
            <div className="kpi">
                <div className="kpi-label">Tabla</div>
                <div className="kpi-value">{tableName || '—'}</div>
            </div>
            <div className="kpi">
                <div className="kpi-label">Registros (resultado)</div>
                <div className="kpi-value kpi-accent">{rowCount.toLocaleString()}</div>
            </div>
            <div className="kpi">
                <div className="kpi-label">Features (cols)</div>
                <div className="kpi-value">{featureCount}</div>
            </div>
        </section>
    )
}
