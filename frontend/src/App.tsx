import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { QueryPanel } from './components/QueryPanel'
import { TablesPanel } from './components/TablesPanel'
import { ResultsPanel } from './components/ResultsPanel'
import { StatsBar } from './components/StatsBar'
import type { QueryResult, TableInfo } from './types'

// --- MOCKS SIN BACKEND ---
const MOCK_TABLES: TableInfo[] = [
  { name: 'Customer', rows: 910, features: 8 },
  { name: 'Order', rows: 10000, features: 12 },
]
const MOCK_RESULT: QueryResult = {
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

type Tab = 'result' | 'explain' | 'transx'

export default function App() {
  const [tables, setTables] = useState<TableInfo[]>([])
  const [loadingTables, setLoadingTables] = useState(false)
  const [queryLoading, setQueryLoading] = useState(false)
  const [lastResult, setLastResult] = useState<QueryResult | null>(null)
  const [activeTab, setActiveTab] = useState<Tab>('result')
  const [error, setError] = useState<string | null>(null)

  // Cargar tablas mock
  useEffect(() => {
    (async () => {
      setLoadingTables(true)
      await sleep(250)
      setTables(MOCK_TABLES)
      setLoadingTables(false)
    })()
  }, [])

  const stats = useMemo(() => {
    if (!lastResult) return null
    const { meta } = lastResult
    return {
      elapsedMs: meta.elapsedMs,
      tableName: meta.table ?? '—',
      rowCount: lastResult.rows.length,
      featureCount: lastResult.columns.length,
    }
  }, [lastResult])

  const onRunQuery = async (_sql: string) => {
    setQueryLoading(true)
    setError(null)
    try {
      await sleep(350 + Math.random() * 300)
      setLastResult({ ...MOCK_RESULT, meta: { ...MOCK_RESULT.meta, elapsedMs: 1200 + Math.round(Math.random() * 400) } })
      setActiveTab('result')
    } catch (e: any) {
      setError(e?.message ?? 'Query failed')
    } finally {
      setQueryLoading(false)
    }
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <h2 className="sidebar-title">Tables</h2>
        <TablesPanel loading={loadingTables} tables={tables} />
      </aside>

      <main className="main">
        <section className="panel card">
          <h1 className="title">Query Console</h1>
          <QueryPanel loading={queryLoading} onRun={onRunQuery} />
        </section>

        {stats && (
          <StatsBar
            elapsedMs={stats.elapsedMs}
            tableName={stats.tableName}
            rowCount={stats.rowCount}
            featureCount={stats.featureCount}
          />
        )}


        <section className="panel card">
          <div className="tabs">
            <button
              className={`tab ${activeTab === 'result' ? 'tab--active' : ''}`}
              onClick={() => setActiveTab('result')}
            >
              Result
            </button>
            <button
              className={`tab ${activeTab === 'explain' ? 'tab--active' : ''}`}
              onClick={() => setActiveTab('explain')}
            >
              Explain
            </button>
            <button
              className={`tab ${activeTab === 'transx' ? 'tab--active' : ''}`}
              onClick={() => setActiveTab('transx')}
            >
              Transx
            </button>
          </div>

          {activeTab === 'result' && (
            <>
              <div className="panel-header">
                <h2 className="subtitle">Resultados</h2>
                {lastResult && (
                  <span className="muted">
                    {lastResult.rows.length} rows • {lastResult.columns.length} columns
                  </span>
                )}
              </div>
              {error && <div className="error">{error}</div>}
              <ResultsPanel result={lastResult} />
            </>
          )}

          {activeTab === 'explain' && (
            <ExplainPanel result={lastResult} />
          )}

          {activeTab === 'transx' && <TransxPanel />}
        </section>

        <footer className="status-bar card">
          <div className="status-item">
            Table <strong>{stats?.tableName ?? '—'}</strong>
          </div>
          <div className="status-sep" />
          <div className="status-item">
            {(stats?.rowCount ?? 0).toLocaleString()} records
          </div>
          <div className="status-sep" />
          <div className="status-item">
            {(stats ? stats.elapsedMs / 1000 : 0).toFixed(1)} sec
          </div>
        </footer>
      </main>
    </div>
  )
}

function ExplainPanel({ result }: { result: QueryResult | null }) {
  const plan = [
    { step: 1, op: 'Parse', detail: 'SQL → Plan interno' },
    { step: 2, op: 'Optimizer', detail: 'Índice elegido: B+Tree (clustered)' },
    { step: 3, op: 'Index Scan', detail: 'Range scan: key [A..M]' },
    { step: 4, op: 'Filter', detail: "nombre BETWEEN 'A' AND 'M'" },
    { step: 5, op: 'Project', detail: 'Cols: id, nombre, fecha' },
  ]
  return (
    <div className="table-wrap" style={{ maxHeight: '42vh' }}>
      <table className="table">
        <thead>
          <tr>
            <th>Step</th>
            <th>Operator</th>
            <th>Detail</th>
          </tr>
        </thead>
        <tbody>
          {plan.map((p) => (
            <tr key={p.step}>
              <td>{p.step}</td>
              <td>{p.op}</td>
              <td>{p.detail}</td>
            </tr>
          ))}
          {result && (
            <tr>
              <td>—</td>
              <td>Cost</td>
              <td>
                ~{Math.max(1, Math.floor(result.rows.length / 1000))} page reads ·{' '}
                {result.meta.elapsedMs.toFixed(1)} ms (sim)
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

function TransxPanel() {
  const rows = [
    { id: 'TX-10231', op: 'INSERT', table: 'Order', status: 'COMMIT', ts: '2025-09-30 17:11:03' },
    { id: 'TX-10230', op: 'SELECT', table: 'Customer', status: 'OK', ts: '2025-09-30 16:58:41' },
    { id: 'TX-10229', op: 'DELETE', table: 'Order', status: 'ROLLBACK', ts: '2025-09-30 16:45:02' },
  ]
  return (
    <div className="table-wrap" style={{ maxHeight: '42vh' }}>
      <table className="table">
        <thead>
          <tr>
            <th>Txn ID</th>
            <th>Op</th>
            <th>Table</th>
            <th>Status</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id}>
              <td>{r.id}</td>
              <td>{r.op}</td>
              <td>{r.table}</td>
              <td style={{ color: r.status === 'COMMIT' ? 'var(--accent-2)' : r.status === 'ROLLBACK' ? 'var(--danger)' : 'var(--muted)' }}>
                {r.status}
              </td>
              <td>{r.ts}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
