export type Column = string
export type QueryResult = {
    columns: Column[];
    rows: any[][];
    meta: {
        table?: string;
        elapsedMs: number
    }
}
export type TableInfo = {
    name: string;
    rows: number;
    features: number
}
