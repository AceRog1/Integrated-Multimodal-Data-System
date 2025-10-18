import type { QueryResult, TableInfo } from '../types'

const TABLES: TableInfo[] = [
    { name: 'Customer', rows: 910, features: 8 },
    { name: 'Order', rows: 10000, features: 12 },
]

const SAMPLE_RESULT: QueryResult = {
    columns: ['Order ID', 'Customer ID', 'Quantity', 'Ship City', 'Ship Country', 'Is Closed', 'OrderDate'],
    rows: [
        [10001, 'FRANS', 44, 'Graz', 'Austria', true, '2011-06-21 12:00:00'],
        [10002, 'FRANS', 52, 'Resende', 'Brazil', true, '2011-03-14 12:00:00'],
        [10003, 'FRANS', 47, 'Montreal', 'Canada', true, '2011-01-15 12:00:00'],
        [10004, 'FRANS', 28, 'Graz', 'Austria', false, '2011-06-21 12:00:00'],
    ],
    meta: { table: 'Order', elapsedMs: 1500 },
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

export async function mockFetchTables(): Promise<TableInfo[]> {
    await sleep(300)
    return TABLES
}

export async function mockRunQuery(_sql: string): Promise<QueryResult> {
    const jitter = 200 + Math.random() * 300
    await sleep(jitter)
    return { ...SAMPLE_RESULT, meta: { ...SAMPLE_RESULT.meta, elapsedMs: 1500 + Math.round(jitter) } }
}
