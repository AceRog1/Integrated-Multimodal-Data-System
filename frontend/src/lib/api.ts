import type { QueryResult, TableInfo } from '../types'

const BASE_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'
const USE_MOCK = (import.meta.env.VITE_USE_MOCK ?? 'true') === 'true'

export async function fetchTables(): Promise<TableInfo[]> {
    if (USE_MOCK) {
        return [
            { name: 'Restaurantes', rows: 12500, features: 6 },
            { name: 'Usuarios', rows: 3421, features: 9 },
            { name: 'Pedidos', rows: 78012, features: 12 },
        ]
    }
    const res = await fetch(`${BASE_URL}/api/tables`)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
}

export async function runQuery(sql: string): Promise<QueryResult> {
    if (USE_MOCK) {
        const t0 = performance.now()
        const simulated = await new Promise<QueryResult>((resolve) =>
            setTimeout(() => {
                resolve({
                    columns: ['id', 'nombre', 'fechaRegistro'],
                    rows: [
                        [1, 'La Panka', '2024-03-10'],
                        [2, 'Pardos', '2023-12-05'],
                        [3, 'Tanta', '2025-01-21'],
                    ],
                    meta: { table: 'Restaurantes', elapsedMs: 12.7 },
                })
            }, 120)
        )
        const elapsed = performance.now() - t0
        return { ...simulated, meta: { ...simulated.meta, elapsedMs: elapsed } }
    }

    const t0 = performance.now()
    const res = await fetch(`${BASE_URL}/api/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const data = (await res.json()) as QueryResult
    const elapsed = performance.now() - t0
    return { ...data, meta: { ...data.meta, elapsedMs: elapsed } }
}
