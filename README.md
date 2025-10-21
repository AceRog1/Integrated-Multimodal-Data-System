# Informe del Proyecto

---

## 1. Introducción

### 1.1. Objetivo del proyecto
El objetivo de este proyecto es crear y comparar varias formas de hacer índices en una base de datos para que funcione más rápido cuando buscamos, insertamos o borramos datos. Queremos responder a una idea simple: ¿cómo logramos que la base de datos responda rápido y bien cuando hay muchos datos?  
Para eso armamos un entorno de prueba donde comparamos un recorrido normal de toda la tabla (escaneo secuencial) con estructuras de índice más avanzadas como B+ Tree, AVL File, Extendible Hashing, ISAM, RTree y Sequential File.

### 1.2. Aplicación de interés
Pensemos en casos que usamos todos los días: una tienda online, un portal de noticias o un registro de actividades del trabajo/cole. En estos escenarios casi siempre hacemos tres tipos de búsquedas:

- **Igualdad:** por ejemplo, “tráeme el producto con id 123”.
- **Rango:** por ejemplo, “noticias del 01/06 al 10/06” o “productos entre 50 y 80 soles”.
- **Texto:** buscar palabras o frases dentro de descripciones, como “celular con buena batería” o “accidente en la Av. X”.

Lo que queremos comprobar es qué tanto ayudan los índices frente a revisar todo “uno por uno”. La meta es que, aunque la tabla tenga muchísimos datos, el sistema responda **rápido** y con **resultados útiles**, sin que el usuario tenga que esperar.

### 1.3. Resultados esperados
Esperamos que el sistema **tarde menos** y **lea menos del disco** que con el recorrido completo. A medida que los datos crecen (de miles a millones), revisar todo se vuelve muy lento; con índices bien elegidos, los tiempos deberían bajar bastante. También queremos graficar cómo cambia el rendimiento al aumentar el tamaño de la tabla y **cuándo conviene cada técnica**: algunas brillan en rangos, otras en igualdades exactas, y otras son mejores para lecturas estables con pocas escrituras.

---

## 2. Técnicas de Indexación Utilizadas
En esta sección contamos **qué hace cada estructura**, **cómo inserta / busca / elimina**, cuánto **cuesta en E/S de disco**, qué **optimizaciones** aplicamos y **cómo el parser tipo “SQL”** decide a qué índice mandar cada consulta.

---

### 2.1. Árbol AVL (búsqueda, rango, inserción y eliminación)

**Búsqueda específica — `search(key)`.**  
Un AVL busca comparando la clave con la del nodo actual. Si la clave es menor, baja a la izquierda; si es mayor, a la derecha. Si coincide, devuelve el registro. Si hay claves repetidas, se recogen todas las coincidencias cercanas. Como el AVL está balanceado, la altura es pequeña y el número de “saltos” para llegar al dato también, lo que mantiene pocos accesos a disco incluso con muchos registros.

**Búsqueda por rango — `rangeSearch(begin, end)`.**  
Se recorre el árbol en orden, **podando** ramas que seguro no aportan resultados: si el nodo es menor que `begin`, se ignora su izquierda; si es mayor que `end`, se ignora su derecha. Cuando la clave cae dentro del intervalo, se agrega y se exploran ambos lados por si hay más. Así se reduce trabajo y se enfocan las lecturas justo donde hay posibilidades reales.

**Inserción — `add(registro)`.**  
Primero se ubica la hoja donde va la nueva clave y se inserta. Luego, al “subir”, se actualizan alturas y se revisa si algún subárbol quedó desbalanceado. Si pasa, se usan **rotaciones** (simples o dobles) para recuperar el equilibrio. En la práctica, afecta pocas páginas: la ruta recorrida y, cuando toca, los nodos involucrados en la rotación.

**Eliminación — `remove(key)`.**  
Hay tres casos: (1) si es hoja, se borra; (2) si tiene un solo hijo, se “engancha” el hijo con el padre; (3) si tiene dos hijos, se reemplaza por su sucesor (o predecesor) y luego se borra esa copia. Después se actualizan alturas y se rotan los nodos que lo necesiten. Con esto, el árbol no se “inclina” y los tiempos se mantienen estables.

---

### 2.2. B+ Tree (no agrupado y agrupado: búsqueda, rango, inserción y eliminación)

**Búsqueda específica — `search(key)`.**  
Los nodos internos del B+ guían el recorrido; las **claves viven en las hojas**. Se baja nivel por nivel hasta la hoja candidata. Si hay duplicados, se recogen en esa hoja (y, si hace falta, en la hoja siguiente gracias a los punteros entre hojas). En modo **no agrupado**, la hoja apunta a un **heap** externo (una lectura extra por coincidencia); en modo **agrupado**, las hojas apuntan a posiciones en un archivo ordenado, mejorando la **localidad** y reduciendo saltos.

**Búsqueda por rango — `rangeSearch(begin, end)`.**  
Se desciende hasta la primera hoja que puede contener `begin` y, desde ahí, se recorre **hoja a hoja** con los punteros de enlace hasta pasar `end`. Como las hojas están ordenadas y casi contiguas en disco, esta lectura resulta muy rápida para intervalos amplios.

**Inserción — `add(registro)`.**  
Se inserta la clave en la hoja correspondiente. Si la hoja se llena, se **divide** en dos y la clave mediana se **promociona** al padre. Si el padre también se llena, el proceso se repite hacia arriba; si la raíz se divide, nace una nueva raíz. Estas divisiones no pasan todo el tiempo, así que la mayoría de inserciones tocan pocas páginas y el árbol se mantiene eficiente.

**Eliminación — `remove(key)`.**  
Se elimina la entrada en su hoja. Si la hoja queda con muy poco contenido, se intenta **redistribuir** con el hermano; si no se puede, se **fusionan** y se ajustan las claves del padre. En casos raros, la altura baja si la raíz se queda con un solo hijo. Esto cuida el espacio y mantiene tiempos de lectura parecidos.

---

### 2.3. Hash extensible (directorio + buckets: búsqueda, rango, inserción y eliminación)

**Búsqueda específica — `search(key)`.**  
Se calcula un hash y se usa un **directorio** para ubicar la **cubeta** (bucket). Normalmente se toca **una sola página**; si hubo colisión y hay desbordamiento (overflow), se siguen pocos enlaces. En promedio, el tiempo es muy cercano a **constante**, ideal para igualdades.

**Búsqueda por rango — `rangeSearch(begin, end)`.**  
El hash no guarda orden, así que para rangos habría que revisar muchas cubetas. En la práctica, se parece a un escaneo y no es lo recomendable para este tipo de consulta.

**Inserción — `add(registro)`.**  
Si hay espacio en la cubeta, se agrega y listo. Si se llenó, hay tres salidas: **dividir localmente** (aumenta la profundidad local y reparte claves), **overflow** (encadenar una cubeta extra) o **expandir el directorio** (duplicarlo y reubicar lo necesario). Esto mantiene colisiones controladas sin rehacer toda la estructura.

**Eliminación — `remove(key)`.**  
Se marca la entrada y se **compacta** la cadena de overflow para que la cubeta base recupere espacio. Si dos cubetas hermanas quedan con poca carga, se **fusionan**; y si el directorio tiene mitades iguales, se **encoge**. Así se cuida el espacio y la estabilidad de los accesos.

---

### 2.4. ISAM de dos niveles (índice estático con overflow: búsqueda, rango, inserción y eliminación)

**Búsqueda específica — `search(key)`.**  
ISAM guarda páginas de datos **ordenadas** y un índice en uno o dos niveles con claves separadoras. Se lee la **raíz** para saber a qué zona ir, luego una **página intermedia** (si existe) y finalmente la **página de datos**. Si la clave entró por overflow, se siguen sus punteros. Es muy predecible cuando la carga es mayormente de lectura.

**Búsqueda por rango — `rangeSearch(begin, end)`.**  
Se llega a la primera página que toca `begin` y se avanza por páginas **contiguas** mientras el rango siga. Solo se agregan saltos extra cuando hay overflow asociado.

**Inserción — `add(registro)`.**  
Primero se intenta en la **página base**; si no hay espacio, se crea o usa **overflow** al final del archivo. Como el índice principal no se reestructura al vuelo, muchas inserciones pueden alargar cadenas de overflow con el tiempo. Por eso ISAM rinde mejor en escenarios con pocas escrituras.

**Eliminación — `remove(key)`.**  
Normalmente es **lógica**: se marca el registro como eliminado (base u overflow). La reorganización física se deja para tareas de mantenimiento, evitando escrituras costosas en el día a día.

---

### 2.5. R-Tree (datos espaciales): búsqueda por rango — `rangeSearch(point, radio)`
La R-Tree agrupa objetos en **rectángulos mínimos** (MBR). Para consultar un punto y un radio, se arma un **rectángulo de búsqueda** que cubre ese círculo y se bajan solo las ramas cuyos MBR **se superponen** con ese rectángulo. En las hojas se verifica de forma precisa si los objetos están dentro del radio. Esta **poda espacial** evita recorrer áreas irrelevantes y reduce mucho las lecturas.

---

### 2.6. R-Tree (datos espaciales): *k* vecinos más cercanos — `knnSearch(point, k)`
Se usa una estrategia “**best-first**” guiada por distancia: primero se exploran los MBR más cercanos al punto de consulta. A medida que aparecen candidatos, se mantiene un **umbral** con la peor distancia dentro de los *k* mejores. Todo nodo cuyo MBR quede más lejos que ese umbral se descarta sin leerlo. Así se centra el trabajo donde hay más chances de encontrar los verdaderos vecinos.

---

## 2.7. Análisis comparativo

### Búsqueda por igualdad (`search(key)`)
- **AVL.**  
  **E/S:** ~ `h` páginas (una por nivel).  
  **Tiempo:** `O(log n)`.

- **B+ Tree (agrupado / no agrupado).**  
  **E/S:** ~ `h` hasta la hoja; si es **no agrupado**, sumar lecturas a *heap* (≈ una por coincidencia).  
  **Tiempo:** `O(log_b n)`.

- **Hash extensible.**  
  **E/S:** 1 página en **promedio**; +`α` si hay overflow.  
  **Tiempo:** `O(1)` (promedio); `O(n)` (peor caso por colisiones extremas).

- **ISAM (2 niveles + overflow).**  
  **E/S:** raíz (1) + intermedio (1) + datos (1) ≈ **3**; +overflow si aplica.  
  **Tiempo:** `O(log_b n)` (altura pequeña).

---

### Búsqueda por rango (`rangeSearch(begin, end)`)
- **AVL.**  
  **E/S:** ~ `h` para “entrar” + páginas con los `k` resultados (no siempre contiguas).  
  **Tiempo:** `O(log n + k)`.

- **B+ Tree.**  
  **E/S:** ~ `h` + **barrido de hojas enlazadas** (casi contiguas) para `k`.  
  **Tiempo:** `O(log_b n + k)`.

- **Hash extensible.**  
  **E/S:** alta (no hay orden).  
  **Tiempo:** `O(n)`.

- **ISAM.**  
  **E/S:** primera página y luego **contiguas**; +overflow si corresponde.  
  **Tiempo:** `O(log_b n + k)`.

---

### Inserción (`add(registro)`)
- **AVL.**  
  **E/S:** ~ `h` en la ruta + 0–2 rotaciones.  
  **Tiempo:** `O(log n)`.

- **B+ Tree.**  
  **E/S:** ~ `h`; **split** ocasional (y a veces sube un nivel).  
  **Tiempo:** `O(log_b n)` (amortizado).

- **Hash extensible.**  
  **E/S:** 1 cubeta; si se llena, **split local**, **overflow** o **expandir directorio**.  
  **Tiempo:** `O(1)` (promedio); `O(n)` (peor caso patológico).

- **ISAM.**  
  **E/S:** base; si no hay espacio, **overflow** al final.  
  **Tiempo:** `O(1)`.

---

### Eliminación (`remove(key)`)
- **AVL.**  
  **E/S:** ~ `h` para ubicar + reequilibrio (0–2 rotaciones).  
  **Tiempo:** `O(log n)`.

- **B+ Tree.**  
  **E/S:** ~ `h` + posible **redistribución** o **fusión**.  
  **Tiempo:** `O(log_b n)` (amortizado).

- **Hash extensible.**  
  **E/S:** marcar + **compactar** overflow; posible **fusión** y **shrink**.  
  **Tiempo:** `O(1)` (promedio); `O(n)` (peor).

- **ISAM.**  
  **E/S:** **marcado lógico**.  
  **Tiempo:** `O(1)`.

> **Conclusión práctica:**  
> - Igualdad → **Hash extensible** (muy rápido en promedio).  
> - Rangos → **B+ Tree** (mejor aún si es **agrupado**).  
> - Lectura estable con pocas escrituras → **ISAM**.  
> - Buen equilibrio general → **AVL** (siempre logarítmico).

---

## 2.8. Optimizaciones de E/S aplicadas en el código

**Lecturas/escrituras compactas.**  
Usamos encabezados pequeños (longitudes, contadores) para **saltar** bytes y no leer de más. En estructuras en disco (AVL/ISAM/Hash) definimos formatos binarios fijos, que se parsean rápido y permiten acceso directo.

**Localidad y secuencialidad.**  
En **B+ Tree**, las hojas enlazadas permiten **barridos** muy eficientes para rangos. En **ISAM**, las páginas de datos quedan **ordenadas** y contiguas, por lo que avanzar en el rango requiere pocos saltos.

**Actualizaciones amortizadas.**  
El **Hash extensible** intenta **split local** antes de expandir el directorio; acepta **overflow** controlado si conviene. **ISAM** y **Hash** usan **borrado lógico** y dejan la compactación para mantenimiento, evitando reescrituras grandes en tiempo crítico.

**Ocupación saludable.**  
**B+ Tree** redistribuye/fusiona solo cuando hace falta para mantener **páginas bien cargadas**. **Hash** fusiona cubetas hermanas y reduce el directorio cuando sobra espacio.

> Resultado: menos páginas tocadas, menos *seeks* y rendimiento más estable a medida que crecen los datos.

---

## 2.9. Parser SQL

**¿Para qué sirve?**  
Para que el usuario escriba consultas en un **SQL sencillo** y el sistema las traduzca a operaciones sobre nuestras estructuras (crear tabla/índices, insertar, borrar, consultar) sin enredarse.

**Sentencias soportadas y qué extrae:**
- **CREATE TABLE … (columnas …) [FROM FILE "ruta"] [USING INDEX tipo]**  
  Lee el **nombre de la tabla**, las **columnas** (tipo y tamaño), la **PRIMARY KEY** y el **tipo de índice** principal deseado. Valida que haya **PRIMARY KEY** y registra preferencias de índice (por ejemplo, `btree` o `hash`).
- **INSERT INTO tabla [(col1, col2, …)] VALUES (…)**  
  Toma la lista de columnas (si viene) y los **valores** en orden. Acepta números, cadenas con comillas y **ARRAY[…]** para coordenadas (útil en espacial).
- **DELETE FROM tabla WHERE …**  
  Extrae la **condición** de borrado (igualdad o rango simple).
- **SELECT columnas FROM tabla [WHERE …]**  
  Lee las columnas (`*` o lista), la tabla y la **condición** (igualdad, `BETWEEN` o espacial simplificada).

**Cómo decide a qué índice mandar la consulta (enrutamiento):**
1. Detecta la **intención**: igualdad, rango o espacial.  
2. Revisa qué **índice** hay disponible para esa columna (ideal: **Hash** para igualdad, **B+** para rango, **R-Tree** para espacial).  
3. Usa el índice elegido; si no hay, cae a otro razonable (por ejemplo, **AVL**) y, en última opción, **escaneo secuencial**.

**Reglas de tipos:**  
Soportamos **INT, FLOAT, VARCHAR[tamaño], DATE, ARRAY** (para pares de `float` en espacial). `VARCHAR` debe indicar tamaño. La **PRIMARY KEY** guía qué columna indexa la estructura principal (por ejemplo, B+ agrupado o hash si buscamos igualdades puras).

**Límites (claros y honestos):**  
El `WHERE` acepta **un solo predicado simple** (igualdad, `BETWEEN` o espacial). No hay `JOIN` ni combinaciones complejas (`AND/OR`). La carga desde archivo en `CREATE TABLE … FROM FILE` se asume como parte de un ETL del proyecto.

**¿Por qué mejora la E/S?**  
Porque el parser **elige la estructura adecuada** según la consulta, en lugar de forzar siempre el mismo camino. Además, el diseño con expresiones regulares es **simple y directo**, con rutas claras para cada sentencia, lo que evita parseos pesados y tiempos sorpresivos.

---

# 3. Evaluación de Técnicas de Indexación con Dataset F1 Drivers

---

## 3.1. Metodología
- **Dataset:** `data/drivers.csv` (N = 862)
- **Esquema:**  
  `(driverId INT, driverRef VARCHAR, number INT, code VARCHAR, forename VARCHAR, surname VARCHAR, dob DATE, nationality VARCHAR, url VARCHAR)`
- **Tablas creadas:** `Drivers_HASH`, `Drivers_BTREE`, `Drivers_AVL`, `Drivers_ISAM`, `Drivers_SEQ`

## 3.2. Creación de tablas (SQL)
- **HASH (PK por `driverId` + hash en PK):**  
  `CREATE TABLE Drivers_HASH (driverId INT PRIMARY KEY INDEX HASH, driverRef VARCHAR[50], number INT, code VARCHAR[10], forename VARCHAR[60], surname VARCHAR[60], dob DATE, nationality VARCHAR[40], url VARCHAR[200]) FROM FILE "data/drivers.csv";`

- **B+TREE (PK por `driverId` + btree en PK):**  
  `CREATE TABLE Drivers_BTREE (driverId INT PRIMARY KEY INDEX BTREE, driverRef VARCHAR[50], number INT, code VARCHAR[10], forename VARCHAR[60], surname VARCHAR[60], dob DATE, nationality VARCHAR[40], url VARCHAR[200]) FROM FILE "data/drivers.csv";`

- **AVL (PK por `driverId` + avl en PK):**  
  `CREATE TABLE Drivers_AVL (driverId INT PRIMARY KEY INDEX AVL, driverRef VARCHAR[50], number INT, code VARCHAR[10], forename VARCHAR[60], surname VARCHAR[60], dob DATE, nationality VARCHAR[40], url VARCHAR[200]) FROM FILE "data/drivers.csv";`

- **ISAM (PK por `driverId` + isam en PK):**  
  `CREATE TABLE Drivers_ISAM (driverId INT PRIMARY KEY INDEX ISAM, driverRef VARCHAR[50], number INT, code VARCHAR[10], forename VARCHAR[60], surname VARCHAR[60], dob DATE, nationality VARCHAR[40], url VARCHAR[200]) FROM FILE "data/drivers.csv";`

- **SECUENCIAL (sin índice):**  
  `CREATE TABLE Drivers_SEQ (driverId INT, driverRef VARCHAR[50], number INT, code VARCHAR[10], forename VARCHAR[60], surname VARCHAR[60], dob DATE, nationality VARCHAR[40], url VARCHAR[200]) FROM FILE "data/drivers.csv";`

## 3.3. Consultas evaluadas
- **Igualdad por PK:** `SELECT * FROM Drivers_<IDX> WHERE driverId = 20;`
- **Texto exacto:** `SELECT * FROM Drivers_<IDX> WHERE surname = 'Hamilton';`
- **Rango numérico:** `SELECT * FROM Drivers_<IDX> WHERE number BETWEEN 1 AND 50;`
- **Rango de fecha:** `SELECT * FROM Drivers_<IDX> WHERE dob BETWEEN '1979-01-01' AND '1985-12-31';`
- **Texto corto (código):** `SELECT * FROM Drivers_<IDX> WHERE code = 'SEN';`
- **Mantenimiento:**  
  - `INSERT INTO Drivers_<IDX> VALUES (999, 'new_ref', 77, 'NEW', 'New', 'Driver', '1990-01-01', 'Nowherian', 'http://example.com/new');`  
  - `DELETE FROM Drivers_<IDX> WHERE driverId = 999;`  
  _(Repetir para HASH/BTREE/AVL/ISAM/SEQ cambiando `<IDX>`)._

## 3.4. Resultados (tiempo en ms)

### 3.4.1. Carga inicial (CREATE TABLE)
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |       21199 |
| B+TREE          |       16638 |
| AVL             |      905295 |
| ISAM            |       28299 |
| SEQUENTIAL      |       16246 |

### 3.4.2. Igualdad por PK (`driverId = 20`)
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |        4091 |
| B+TREE          |        3985 |
| AVL             |        4025 |
| ISAM            |        4108 |
| SEQUENTIAL      |        4000 |

### 3.4.3. Igualdad texto (`surname = 'Hamilton'`)
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |        4037 |
| B+TREE          |        4013 |
| AVL             |        3760 |
| ISAM            |        3940 |
| SEQUENTIAL      |        4052 |

### 3.4.4. Rango numérico (`number BETWEEN 1 AND 50`)
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |        4083 |
| B+TREE          |        2759 |
| AVL             |        2811 |
| ISAM            |        2813 |
| SEQUENTIAL      |        2925 |

### 3.4.5. Rango de fecha (`dob BETWEEN '1979-01-01' AND '1985-12-31'`)
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |        2999 |
| B+TREE          |        2956 |
| AVL             |        2949 |
| ISAM            |        3075 |
| SEQUENTIAL      |        2961 |

### 3.4.6. Igualdad código (`code = 'SEN'`)
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |        3153 |
| B+TREE          |        2995 |
| AVL             |        2969 |
| ISAM            |        2974 |
| SEQUENTIAL      |        2856 |

### 3.4.7. Operaciones de mantenimiento
**INSERT (1 fila)**  
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |          69 |
| B+TREE          |          80 |
| AVL             |         924 |
| ISAM            |          83 |
| SEQUENTIAL      |          39 |

**DELETE (1 fila)**  
| Índice / Método | Tiempo (ms) |
| --------------- | ----------: |
| HASH            |        3107 |
| B+TREE          |        3568 |
| AVL             |         910 |
| ISAM            |        2910 |
| SEQUENTIAL      |        2762 |

## 3.5. Discusión
- **Carga inicial:** B+Tree y Secuencial cargan más rápido que Hash e ISAM; AVL es significativamente más costoso en construcción (rotaciones y reequilibrado intensivo).
- **Igualdad por PK:** Las diferencias entre HASH, B+Tree y AVL son pequeñas en este dataset; todos resuelven en ~4s. El hashing **no dominó** por un gran margen, lo que sugiere que el tamaño del dataset y/o efectos de caché redujeron la brecha teórica.
- **Igualdad de texto:** AVL y B+Tree obtienen los mejores tiempos (≈3.0–3.9s), consistentes con búsquedas ordenadas. SEQ queda cerca, probablemente por tamaño moderado del dataset.
- **Rangos (número y fecha):** B+Tree/AVL/ISAM superan a HASH (que no es óptimo para rangos). Las tres estructuras ordenadas están muy parecidas (≈2.75–3.1s), con B+Tree apenas mejor en numérico.
- **Mantenimiento:** **INSERT** es más barato en SEQ (append) y HASH/ISAM; AVL es el más costoso (≈924 ms). **DELETE** favorece SEQ e ISAM; B+Tree es más caro por reestructuración.
- **Conclusión operativa:**  
  - Si predominan **consultas por igualdad** (especialmente PK) y pocas inserciones: **HASH** / **B+Tree**.  
  - Si predominan **rangos** y lecturas: **B+Tree** o **AVL**; **ISAM** también es buena opción cuando las inserciones son raras (lecturas rápidas, penando inserciones).  
  - **Secuencial** es aceptable en datasets pequeños o como baseline.

## 3.6. Conclusiones
- **Recomendación por tipo de consulta:**
  - Igualdad de clave: **HASH** o **B+Tree**.
  - Rangos/ordenación: **B+Tree / AVL** (o **ISAM** en escenarios read-heavy y data relativamente estática).
  - Base sin índice (SEQ): útil como control y/o para cargas rápidas con pocas consultas complejas.
  
## 3.7 Anexo:

- En la carpeta imagenes podrá econtrar todas las capturas de los test que se realizaron para cada uno de los índices.

# 4. Pruebas de uso y presentación

En la demostración utilizamos la GUI (frontend en `http://localhost:5173`) para ejecutar consultas sobre `Drivers_HASH`, `Drivers_BTREE`, `Drivers_AVL`, `Drivers_ISAM` y `Drivers_SEQ`, registrando **tiempo (ms)**, **filas** y **EXPLAIN** cuando aplica. Las capturas de pantalla (inicio, listar tablas, igualdad por PK y por texto, rangos numérico/fecha, igualdad por `code`, `INSERT`, `DELETE`) se incluyen en la carpeta **`/IMAGENES`**.

## Video

https://drive.google.com/drive/folders/1zZy-wkZe6r-rLZsQtibujLG5DOPg8Kvk?usp=sharing

