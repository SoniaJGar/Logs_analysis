## Proyecto Analisis SQL de logs de Servidor
import duckdb

# Conectar a DuckDB (en memoria - los datos se pierden al cerrar)
# Si querés persistir: con = duckdb.connect('mis_logs.db')
con = duckdb.connect()

# Leemos archivos json descargados
con.execute("""
    CREATE TABLE logs AS 
    SELECT * FROM
    read_json_auto('./data/logs_access_logs.json')
            """)

#Comprobamos si cargo bien, contamos filas
print('El numero de filas es:', con.execute('SELECT COUNT(*) FROM logs').fetchone()[0])

#Observamos estructura de la tabla 
# Ver estructura de la tabla (igual que en PostgreSQL)
print("\nColumnas:")
for col in con.execute("DESCRIBE logs").fetchall():
    print(f"  {col[0]}: {col[1]}")

#Observamos las 3 primeras filas
print('Las 3 primeras filas son:')
print(con.execute('SELECT * FROM logs LIMIT 3').fetchdf())
# ========================================
#1. Exploracion inicial
# ========================================
#CUantos datos hay que, periodo cubren, usuarios unicos
in_exp = con.execute('SELECT COUNT(*) as conteo,' \
'                       MIN(timestamp) AS PERIODO_MIN,' \
'                       MAX(timestamp) AS PERIODO_MAX,' \
'                       COUNT(DISTINCT log_id) AS USUARIOS_UNICOS,' \
'                       COUNT(DISTINCT endpoint) as endpoints_unicos FROM logs').fetchdf()
in_exp.to_csv('./output/Exploracion_inicial.csv')
# ========================================
#2. Endpoints mas usados
# ========================================
endp_usados = con.execute('SELECT endpoint, COUNT(*) AS conteo,' \
'                          COUNT(*)/(SELECT COUNT(*) FROM logs)*100 as porcentage' \
'                            FROM logs GROUP BY endpoint order by conteo desc' \
'                            LIMIT 10').fetchdf()
endp_usados.to_csv('./output/endpoints_mas_usados.csv')
print('Endpoints mas usados:')
print(endp_usados)
# ========================================
#3. Analisis de errores
# ========================================
# Si un endpoint tiene muchos errores y muchos usuarios afectados es prioridad alta
# Veamos que endpoint tiene mas errores 500
endp_err500 = con.execute('SELECT endpoint, COUNT(*) AS conteo,' \
'COUNT(DISTINCT user_id) as usuarios_afectados FROM logs' \
' WHERE status_code = 500 GROUP BY endpoint ORDER BY conteo ' \
'LIMIT 10').fetchdf()
endp_err500.to_csv('./output/endpoint_con_errores.csv')
# ========================================
#4. Que performance son mas lentos
# ========================================
#Con el percentil 95 vemos que el 5% de los datos estan por encima de ese valor
endp_time = con.execute('SELECT ' \
'    endpoint,' \
'    COUNT(*) as conteo,' \
'   ROUND(AVG(response_time_ms), 2) as avg_time,' \
 '   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_ms), 2) as p50,' \
'    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms), 2) as p95,' \
'    MAX(response_time_ms) as max_time ' \
'FROM logs ' \
'WHERE status_code < 500 ' \
'GROUP BY endpoint ' \
'HAVING conteo > 100 '  \
'ORDER BY p95 desc ' \
'LIMIT 10').fetchdf()
print('Exloramos el percentil 50 y 95:')
print(endp_time)
endp_time.to_csv('./output/percentiles.csv')
# ========================================
# 5. TENDENCIA HORARIA
# ========================================
# ¿A qué hora hay más tráfico?
endp_trf = con.execute(
    'SELECT EXTRACT(HOUR FROM timestamp) as hora,' \
    'COUNT(*) AS conteo,' \
    'ROUND(AVG(response_time_ms),2) AS TIEMPO_MEDIO,' \
    'SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) as errors' \
    ' FROM logs' \
    ' GROUP BY EXTRACT(HOUR FROM timestamp) ORDER BY hora'
).fetchdf()
print('¿A qué hora hay más tráfico?:')
print(endp_trf)
endp_trf.to_csv('./output/trafico_endpoints.csv')
# ========================================
# 5. WINDOW FUNCTION
# ========================================
# Enumeramos filas

index = con.execute('WITH A AS (SELECT ' \
'endpoint, ' \
'timestamp,' \
'response_time_ms,' \
'user_id,' \
'ROW_NUMBER() OVER(PARTITION BY endpoint ORDER BY response_time_ms DESC) AS orden' \
' FROM logs WHERE status_code < 500)' \
'SELECT * FROM A WHERE orden <=3 ORDER BY endpoint, orden').fetchdf()
print('Enumeramos filas por tiempo de respuesta:')
print(index)

# ========================================
# 7. COMPARACIÓN CON PERÍODO ANTERIOR
# ========================================
# ¿Cómo cambia el tráfico día a día?

cambio = con.execute('WITH A AS(SELECT DATE(timestamp) as fecha, ' \
'COUNT(*) AS conteo,' \
'ROUND(AVG(response_time_ms),2) AS media FROM logs ' \
'GROUP BY DATE(timestamp))' \
'SELECT fecha, conteo, media, LAG(conteo) OVER' \
'(ORDER BY fecha) as conteo_anterior,' \
'conteo - LAG(conteo) OVER (ORDER BY fecha) AS dif_conteo' \
' FROM A order by fecha').fetchdf()
print('¿Cómo cambia el tráfico día a día?:')
print(cambio)
cambio.to_csv('./output/cambio_trafico.csv')