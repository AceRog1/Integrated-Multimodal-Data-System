import { useMemo, useState } from 'react'
import type { QueryResult } from '../types'

type Props = {
    result: QueryResult | null
}

export function ResultsPanel({ result }: Props) {
    const [pageSize, setPageSize] = useState(25)
    const [page, setPage] = useState(1)

    const total = result?.rows?.length ?? 0
    const pageCount = Math.max(1, Math.ceil(total / pageSize))

    const { slice, columns } = useMemo(() => {
        if (!result || !Array.isArray(result.rows)) {
            return { slice: [] as any[][], columns: [] as string[] }
        }
        const safeCols = Array.isArray(result.columns) ? result.columns : []
        const start = (page - 1) * pageSize
        const end = start + pageSize
        return { slice: result.rows.slice(start, end), columns: safeCols }
    }, [result, page, pageSize])

    if (!result || !Array.isArray(result.rows)) {
        return <div className="muted">Ejecuta una consulta para ver resultados</div>
    }
    if (result.rows.length === 0) {
        return <div className="muted">Sin filas</div>
    }

    return (
        <>
            <div className="table-wrap" style={{ maxHeight: '42vh' }}>
                <table className="table">
                    <thead>
                        <tr>
                            {columns.map((c) => (
                                <th key={c}>{c}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {slice.map((row, i) => (
                            <tr key={i}>
                                {row.map((cell: any, j: number) => (
                                    <td key={j}>{formatCell(cell)}</td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            <div className="table-footer">
                <div>
                    Página {page} de {pageCount} — {total} filas totales
                </div>
                <div className="row">
                    <label className="muted">Rows/page</label>
                    <input
                        type="number"
                        min={5}
                        max={200}
                        step={5}
                        value={pageSize}
                        onChange={(e) => {
                            const v = Number(e.target.value || 25)
                            const clamped = Math.min(200, Math.max(5, v))
                            setPageSize(clamped)
                            setPage(1)
                        }}
                    />
                    <button
                        className="button secondary"
                        onClick={() => setPage((p) => Math.max(1, p - 1))}
                    >
                        Prev
                    </button>
                    <button
                        className="button"
                        onClick={() => setPage((p) => Math.min(pageCount, p + 1))}
                    >
                        Next
                    </button>
                </div>
            </div>
        </>
    )
}

function formatCell(v: unknown) {
    if (v === null || v === undefined) return '—'
    if (typeof v === 'object') return JSON.stringify(v)
    return String(v)
}
