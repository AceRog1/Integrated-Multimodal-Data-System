export type TableInfo = { name: string; rows: number; features: number }

export type QueryResult = {
    columns: string[]
    rows: any[][]
    meta: { table?: string; elapsedMs: number }
}

export type ExplainResult = {
    plan: Array<{ step: number; op: string; detail: string }>
    cost?: { pages?: number; ms?: number }
}

export type SystemStats = {
    total_tables: number
    total_records: number
    estimated_size_mb?: number
    uptime?: string
}
