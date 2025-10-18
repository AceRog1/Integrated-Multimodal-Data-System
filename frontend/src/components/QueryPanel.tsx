import { useEffect, useRef, useState } from 'react'

type Props = {
    loading?: boolean
    onRun: (sql: string) => Promise<void> | void
}

const EXAMPLE = `-- Ejemplos:
-- select * from Restaurantes where id = 7
-- select * from Restaurantes where nombre between 'A' and 'M'
-- select * from Restaurantes where ubicacion in (point[ -12.07, -77.04 ], 5.0)
select * from Restaurantes limit 100;`

export function QueryPanel({ loading, onRun }: Props) {
    const [sql, setSql] = useState(EXAMPLE)
    const taRef = useRef<HTMLTextAreaElement | null>(null)

    useEffect(() => {
        taRef.current?.focus()
    }, [])

    return (
        <div className="panel">
            <textarea
                ref={taRef}
                className="query-box"
                placeholder="Escribe tu consulta SQL-like aquí…"
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
                    onClick={() => setSql(EXAMPLE)}
                    title="Cargar ejemplo"
                >
                    Ejemplo
                </button>
            </div>
        </div>
    )
}
