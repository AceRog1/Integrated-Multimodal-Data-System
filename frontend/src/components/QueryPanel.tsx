import { useEffect, useRef, useState } from 'react'

type Props = {
    loading?: boolean
    onRun: (sql: string) => Promise<void> | void
}

const PLACEHOLDER = `-- Ingresar una consulta SQL aquí`

export function QueryPanel({ loading, onRun }: Props) {
    const [sql, setSql] = useState('')
    const taRef = useRef<HTMLTextAreaElement | null>(null)

    useEffect(() => {
        taRef.current?.focus()
    }, [])

    return (
        <div className="panel">
            <textarea
                ref={taRef}
                className="query-box"
                placeholder={PLACEHOLDER}
                value={sql}
                onChange={(e) => setSql(e.target.value)}
            />
            <div className="row">
                <button className="button" disabled={loading} onClick={() => onRun(sql)}>
                    {loading ? 'Ejecutando…' : 'Ejecutar'}
                </button>
                <button
                    className="button secondary"
                    disabled={loading}
                    onClick={() => setSql('')}
                    title="Limpiar"
                    style={{ marginLeft: 8 }}
                >
                    Limpiar
                </button>
            </div>
        </div>
    )
}
