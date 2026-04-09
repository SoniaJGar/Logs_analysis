# 📊 Proyecto: Análisis SQL de Logs de Servidor con DuckDB

## 🧠 Descripción
Este proyecto realiza un análisis exploratorio de logs de servidor utilizando SQL sobre datos en formato JSON, usando DuckDB como motor analítico en memoria.

El objetivo es obtener insights clave sobre:
- Uso de endpoints
- Errores del sistema
- Rendimiento (latencias)
- Tendencias de tráfico

## 🔍 Análisis realizados (detalle técnico)

### 1. Exploración inicial
Consulta:
- Total de registros
- Periodo temporal (`MIN` / `MAX`)
- Usuarios únicos (`COUNT DISTINCT`)
- Endpoints distintos  

📁 Output: `Exploracion_inicial.csv`

---

### 2. Endpoints más usados
Agrupación por endpoint:
- Número de peticiones
- Porcentaje sobre el total  

Ordenado por volumen descendente.

📁 Output: `endpoints_mas_usados.csv`

---

### 3. Análisis de errores (HTTP 500)
Filtrado por `status_code = 500`:
- Conteo de errores por endpoint
- Número de usuarios afectados  

👉 Permite priorizar endpoints críticos.

📁 Output: `endpoint_con_errores.csv`

---

### 4. Análisis de rendimiento (latencias)
Métricas por endpoint (excluyendo errores):
- `AVG(response_time_ms)`
- Percentil 50 (`PERCENTILE_CONT(0.5)`)
- Percentil 95 (`PERCENTILE_CONT(0.95)`)
- Máximo tiempo de respuesta  

Filtrado: endpoints con más de 100 peticiones.

👉 El **P95** identifica degradaciones en el peor 5% de casos.

📁 Output: `percentiles.csv`

---

### 5. Tendencia horaria
Agrupación por hora:
- Volumen de tráfico
- Tiempo medio de respuesta
- Número de errores  

👉 Permite detectar picos de carga.

📁 Output: `trafico_endpoints.csv`

---

### 6. Análisis con Window Functions
Uso de `ROW_NUMBER()`:
- Ranking de las requests más lentas por endpoint
- Selección del top 3 por endpoint  

👉 Identificación de outliers de rendimiento.

---

### 7. Comparación temporal
Agrupación diaria:
- Conteo de requests
- Tiempo medio  

Uso de `LAG()`:
- Comparación con el día anterior
- Diferencia de tráfico (`dif_conteo`)  

👉 Permite analizar tendencias y cambios.

📁 Output: `cambio_trafico.csv`