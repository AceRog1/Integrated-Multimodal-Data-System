import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { QueryPanel } from './components/QueryPanel'
import { TablesPanel } from './components/TablesPanel'
import { ResultsPanel } from './components/ResultsPanel'
import { StatsBar } from './components/StatsBar'
import type { QueryResult, TableInfo, ExplainResult, SystemStats } from './types'
import { listTables, runQuery, explainQuery, stats as fetchStats, health as fetchHealth } from './lib/api'

type Tab = 'result' | 'explain'

export default function App() {
  const [tables, setTables] = useState<TableInfo[]>([])
  const [loadingTables, setLoadingTables] = useState(false)

  const [queryLoading, setQueryLoading] = useState(false)
  const [lastResult, setLastResult] = useState<QueryResult | null>(null)
  const [lastSql, setLastSql] = useState<string>('')
  const [activeTab, setActiveTab] = useState<Tab>('result')
  const [error, setError] = useState<string | null>(null)

  const [sysStats, setSysStats] = useState<SystemStats | null>(null)
  const [healthy, setHealthy] = useState<boolean | null>(null)

  async function refreshTables() {
    try {
      setLoadingTables(true)
      const t = await listTables()
      setTables(t ?? [])
    } catch {
    } finally {
      setLoadingTables(false)
    }
  }

  useEffect(() => {
    (async () => {
      try {
        setLoadingTables(true)
        const [hOk, s, t] = await Promise.all([
          fetchHealth().then(() => true).catch(() => false),
          fetchStats().catch(() => null),
          listTables().catch(() => [] as TableInfo[]),
        ])
        setHealthy(hOk)
        if (s) setSysStats(s)
        setTables(t)
      } finally {
        setLoadingTables(false)
      }
    })()
  }, [])

  const stats = useMemo(() => {
    if (!lastResult) return null
    const { meta } = lastResult
    return {
      elapsedMs: meta?.elapsedMs ?? 0,
      tableName: (meta as any)?.table ?? '—',
      rowCount: Array.isArray(lastResult.rows) ? lastResult.rows.length : 0,
      featureCount: Array.isArray(lastResult.columns) ? lastResult.columns.length : 0,
    }
  }, [lastResult])

  const onRunQuery = async (sql: string) => {
    setQueryLoading(true)
    setError(null)
    try {
      setLastSql(sql)
      const res = await runQuery(sql)

      const normalized: QueryResult = {
        columns: Array.isArray(res.columns) ? res.columns : [],
        rows: Array.isArray(res.rows) ? res.rows : [],
        meta: res.meta ?? { elapsedMs: 0 },
      }

      setLastResult(normalized)
      setActiveTab('result')

      const op = sql.trim().split(/\s+/)[0]?.toUpperCase() || ''
      if (/^(CREATE|DROP|ALTER|TRUNCATE|INSERT|UPDATE|DELETE|LOAD)$/.test(op)) {
        await refreshTables()
      }
    } catch (e: any) {
      setError(e?.message ?? 'Query failed')
      setLastResult(null)
    } finally {
      setQueryLoading(false)
    }
  }

  const openTable = async (tableName: string) => {
    const sql = `SELECT * FROM ${tableName} LIMIT 100;`
    await onRunQuery(sql)
    setActiveTab('result')
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <h2 className="sidebar-title">Tables</h2>
        <TablesPanel loading={loadingTables} tables={tables} onOpen={openTable} />
        <div className="muted" style={{ marginTop: 8 }}>
          Health: {healthy === null ? '…' : healthy ? 'OK' : 'DOWN'}
        </div>
        {
          (() => {
            const totalTables =
              (sysStats && sysStats.total_tables) || tables.length
            const totalRows =
              (sysStats && sysStats.total_records) ||
              tables.reduce((acc, t) => acc + (typeof (t as any).rows === 'number' ? (t as any).rows : 0), 0)

            return (
              <div className="muted" style={{ marginTop: 4 }}>
                {totalTables} tables • {totalRows.toLocaleString()} rows
              </div>
            )
          })()
        }

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
          </div>

          {activeTab === 'result' && (
            <>
              <div className="panel-header">
                <h2 className="subtitle">Resultados</h2>
                {lastResult && (
                  <span className="muted">
                    {(lastResult.rows?.length ?? 0)} rows • {(lastResult.columns?.length ?? 0)} columns
                  </span>
                )}
              </div>

              {lastResult?.meta && (
                (lastResult.meta as any).error
                  ? <div className="error">{String((lastResult.meta as any).error)}</div>
                  : (lastResult.meta as any).explain
                    ? <div className="muted">{String((lastResult.meta as any).explain)}</div>
                    : null
              )}

              {error && <div className="error">{error}</div>}
              <ResultsPanel result={lastResult} />
            </>
          )}

          {activeTab === 'explain' && <ExplainPanel sql={lastSql} />}

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

function ExplainPanel({ sql }: { sql: string }) {
  const [planRes, setPlanRes] = useState<ExplainResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      if (!sql?.trim()) {
        setPlanRes(null)
        return
      }
      setLoading(true)
      setErr(null)
      try {
        const res = await explainQuery(sql)
        setPlanRes(res)
      } catch (e: any) {
        setErr(e?.message ?? 'Explain failed')
        setPlanRes(null)
      } finally {
        setLoading(false)
      }
    })()
  }, [sql])

  if (!sql) return <div className="muted">Ejecuta una consulta para ver el plan</div>
  if (loading) return <div className="muted">Generando plan…</div>
  if (err) return <div className="error">{err}</div>
  if (!planRes) return <div className="muted">Sin plan</div>

  const rows = Array.isArray(planRes.plan) ? planRes.plan : []
  if (!rows.length) return <div className="muted">El backend no devolvió pasos del plan.</div>

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
          {rows.map((p, i) => (
            <tr key={i}>
              <td>{p.step ?? i + 1}</td>
              <td>{p.op ?? 'Step'}</td>
              <td>{p.detail ?? '—'}</td>
            </tr>
          ))}
          {planRes.cost && (planRes.cost.pages != null || planRes.cost.ms != null) && (
            <tr>
              <td>—</td>
              <td>Cost</td>
              <td>
                {planRes.cost.pages != null ? `${planRes.cost.pages} page reads` : '—'} ·{' '}
                {planRes.cost.ms != null ? `${planRes.cost.ms} ms` : '—'}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

