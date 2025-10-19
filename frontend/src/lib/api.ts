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

const BASE = 'http://localhost:8000'  // Cambia la URL base según tu configuración
const PREFIX = '/api/v1'
const API = `${BASE}${PREFIX}`

async function http<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${API}${path}`, {
        headers: { 'Content-Type': 'application/json', ...(init?.headers || {}) },
        ...init,
    })

    const raw = await res.text()
    let data: any = null
    try {
        data = raw ? JSON.parse(raw) : null
    } catch {
        data = raw
    }

    if (!res.ok) {
        const detail =
            typeof data === 'string' ? data : data ? JSON.stringify(data) : ''
        throw new Error(`HTTP ${res.status} ${res.statusText}${detail ? ` — ${detail}` : ''}`)
    }

    return data as T
}

export const pingRoot = () => http<string | Record<string, unknown>>('/')
export const health = () => http<{ status: string; time?: string }>('/health')

export const stats = async (): Promise<SystemStats> => {
    const r = await http<any>('/stats')
    const s = r?.stats ?? r ?? {}

    return {
        total_tables:
            typeof s.total_tables === 'number' ? s.total_tables :
                typeof s.count === 'number' ? s.count :
                    0,
        total_records:
            typeof s.total_records === 'number' ? s.total_records :
                typeof s.rows === 'number' ? s.rows :
                    0,
        estimated_size_mb:
            typeof s.estimated_size_mb === 'number' ? s.estimated_size_mb :
                typeof s.estimated_size === 'number' ? s.estimated_size :
                    undefined,
        uptime: typeof s.uptime === 'string' ? s.uptime : undefined,
    }
}

export const listTables = async (): Promise<TableInfo[]> => {
    const r = await http<any>('/tables')
    const arr = Array.isArray(r) ? r : r?.tables

    return (arr ?? []).map((t: any) => ({
        name: t?.name ?? t?.table ?? '',
        rows:
            typeof t?.total_records === 'number'
                ? t.total_records
                : typeof t?.rows === 'number'
                    ? t.rows
                    : typeof t?.count === 'number'
                        ? t.count
                        : 0,
        features: Array.isArray(t?.columns)
            ? t.columns.length
            : typeof t?.features === 'number'
                ? t.features
                : typeof t?.columns === 'number'
                    ? t.columns
                    : 0,
    }))
}


export const getTableInfo = async (name: string) => {
    const t = await http<any>(`/tables/${encodeURIComponent(name)}`)
    return {
        name: t?.name ?? name,
        rows:
            typeof t?.total_records === 'number'
                ? t.total_records
                : typeof t?.rows === 'number'
                    ? t.rows
                    : 0,
        features: Array.isArray(t?.columns) ? t.columns.length : 0,
        columns: Array.isArray(t?.columns) ? t.columns : [],
        primary_key: t?.primary_key ?? null,
        primary_index_type: t?.primary_index_type ?? null,
        indexed_columns: Array.isArray(t?.indexed_columns) ? t.indexed_columns : [],
        record_size: t?.record_size ?? null,
        raw: t,
    }
}

export const runQuery = async (sql: string): Promise<QueryResult> => {
    const r = await http<any>('/query', {
        method: 'POST',
        body: JSON.stringify({ query: sql }),
    })

    const payload = (r && (r.result ?? r)) || {}

    if (Array.isArray(payload.columns) && Array.isArray(payload.rows)) {
        const baseMeta = payload.meta ?? { elapsedMs: 0, table: payload.table }
        return {
            columns: payload.columns,
            rows: payload.rows,
            meta: {
                ...baseMeta,
                error: payload.error ?? null,
                explain: typeof payload.explain === 'string' ? payload.explain : undefined,
            } as any,
        }
    }

    if (Array.isArray(payload.data)) {
        const colSet = new Set<string>()
        for (const row of payload.data) Object.keys(row ?? {}).forEach(k => colSet.add(k))
        const columns = Array.from(colSet)
        const rows = payload.data.map((obj: any) => columns.map(c => obj?.[c] ?? null))

        const elapsedMs =
            typeof payload.time === 'number' ? Math.round(payload.time * 1000) :
                payload.meta?.elapsedMs ?? 0

        let table: string | undefined = payload.meta?.table
        if (!table && typeof payload.explain === 'string') {
            const m = payload.explain.match(/'([^']+)'/)
            if (m) table = m[1]
        }

        return {
            columns,
            rows,
            meta: {
                elapsedMs,
                table,
                error: payload.error ?? null,
                explain: typeof payload.explain === 'string' ? payload.explain : undefined,
            } as any,
        }
    }

    const baseMeta = payload.meta ?? { elapsedMs: 0, table: payload.table }
    return {
        columns: [],
        rows: [],
        meta: {
            ...baseMeta,
            error: payload.error ?? null,
            explain: typeof payload.explain === 'string' ? payload.explain : undefined,
        } as any,
    }
}


export const explainQuery = async (sql: string): Promise<ExplainResult> => {
    const r = await http<any>('/explain', {
        method: 'POST',
        body: JSON.stringify({ query: sql }),
    })

    const payload = (r && (r.result ?? r)) || {}

    let planRows: Array<{ step: number; op: string; detail: string }> = []

    if (Array.isArray(payload.plan)) {
        planRows = payload.plan.map((p: any, i: number) => ({
            step: Number(p?.step ?? i + 1),
            op: String(p?.op ?? p?.operation ?? 'Step'),
            detail:
                typeof p?.detail === 'string'
                    ? p.detail
                    : p?.description
                        ? String(p.description)
                        : JSON.stringify(p),
        }))
    } else if (payload.plan && typeof payload.plan === 'object') {
        const entries = Object.entries(payload.plan)
        planRows = entries.map(([k, v], i) => ({
            step: i + 1,
            op: String(k),
            detail: typeof v === 'string' ? v : JSON.stringify(v),
        }))
    } else if (typeof payload.explain === 'string') {
        planRows = [{ step: 1, op: 'Explain', detail: payload.explain }]
    } else {
        planRows = []
    }

    const cost =
        payload.cost ??
        (typeof payload.time === 'number' ? { ms: Math.round(payload.time * 1000) } : undefined)

    return { plan: planRows, cost }
}

