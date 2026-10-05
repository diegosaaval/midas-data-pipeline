# Decisiones de diseño y trade-offs

Cada decisión incluye la alternativa descartada y por qué. Es la base para defender el proyecto en una entrevista.

## 1. Idempotencia con *dynamic partition overwrite* (no Delta/Iceberg… todavía)

**Decisión.** Silver se escribe en Parquet con `partitionOverwriteMode=dynamic`: al re-procesar un día solo se reemplazan las particiones presentes en el DataFrame.

**Por qué.** Es nativo de Spark, de Glue y de Athena, sin dependencias extra, y basta para un batch diario con un solo escritor.

**Trade-off.** No hay transacciones ACID ni `MERGE` fila a fila: para un *upsert* se reescribe la partición completa (union + dedup). Con particiones de un día eso es barato. Si hubiera varios escritores concurrentes o actualizaciones muy dispersas, el siguiente paso es **Apache Iceberg** (soportado por Glue y Athena), que trae `MERGE INTO`, *time travel* y compactación. Está en el roadmap (fase 4).

## 2. Datos tardíos: se integran a su partición original

**Decisión.** Silver particiona por **fecha del evento** (`transaction_date`), no por fecha de llegada. Un pago que llega 3 días tarde se integra a `dt` de hace 3 días: se leen solo las particiones afectadas (*partition pruning*), se unen con el lote, se deduplica y se reescriben solo esas.

**Por qué.** Los reportes financieros se consultan por fecha de transacción. Si se particionara por fecha de llegada, cada consulta de "ventas del 10 de septiembre" tendría que leer todo.

**Límite.** Se aceptan hasta `FINFLOW_LATE_DAYS` (7) días de atraso. Lo más viejo va a cuarentena con el motivo `late:out_of_window` y se recupera con un backfill consciente. El modelo incremental de dbt reprocesa la misma ventana de 7 días.

## 3. Bronze guarda todo como texto

**Decisión.** Bronze lee el JSON con `primitivesAsString=true`.

**Por qué.** Bronze debe ser una copia fiel. Si se infiere el tipo, un `"amount": "mil pesos"` se convertiría en NULL y se perdería el valor original. Los tipos se aplican en silver, donde un valor inválido se detecta y va a cuarentena con su valor crudo.

## 4. Cuarentena en vez de descartar o fallar

**Decisión.** Cada registro que incumple el contrato va a `quarantine/` con una lista de motivos (`amount:below_min`, `merchant_id:unknown`, `record:malformed_json`…).

**Por qué.** Descartar en silencio esconde problemas de la fuente; fallar todo el job por un registro detiene el negocio. En Spark 4 el modo ANSI viene activo: un `cast` normal haría fallar el job completo. Por eso se usa `try_cast` / `try_to_timestamp` (un test lo cubre).

## 5. Padres tardíos: se conservan marcados

Una devolución cuyo pago aún no llega no se rechaza: se guarda con `payment_found = false`, y el test de relación de dbt lo reporta como **advertencia**, no como error. En sistemas distribuidos el orden de llegada no está garantizado. Sí se rechaza una devolución **mayor** al pago original, porque eso es un error de la fuente.

## 6. Evolución de esquema por versiones de contrato

Las columnas nuevas **compatibles** (opcionales) se declaran en el contrato con `since=2` y un valor por defecto: v1 y v2 conviven en silver (`currency` = COP para registros v1). Las columnas **no acordadas** (por ejemplo `promo_code`) se registran como evento en bronze y no pasan a silver hasta que alguien las agregue al contrato. Así un cambio en la fuente nunca rompe el pipeline ni contamina las capas de consumo sin una decisión explícita.

## 7. Airflow orquesta, no transforma

El DAG solo llama al CLI (`finflow task silver.payments --date {{ ds }}`). La lógica vive en Python con tests, y el mismo código puede correr en Airflow, en un Glue Job o en local. `catchup=True` + `max_active_runs=1` garantiza backfills en orden cronológico; los reintentos con backoff exponencial cubren fallas transitorias. Si la fuente no ha entregado el archivo, el runner no reintenta (no ayuda): en Airflow lo cubre un `FileSensor` en modo `reschedule`.

## 8. dbt sobre DuckDB en local, Athena en la nube

Los modelos usan SQL portable y macros multi-motor (`dbt.dateadd`, `dbt.datediff`). Localmente dbt lee el Parquet de silver con DuckDB: no hace falta ningún servidor, corre en CI y en segundos. En AWS se usa el perfil `aws` (dbt-athena) sobre las tablas del Glue Catalog. **Trade-off:** se pierden algunas funciones específicas de cada motor a cambio de no mantener dos proyectos.

## 9. Manejo de skew: salting + AQE

Un marketplace concentra ~25% de los pagos. En la agregación por comercio esa llave caerá en una sola tarea. Se agrega una sal aleatoria (8 sub-llaves), se agrega parcialmente y luego se combina. Para joins, AQE (`skewJoin.enabled`) divide particiones sesgadas automáticamente. **Ojo:** las métricas no aditivas (por ejemplo, `count distinct`) no se pueden combinar entre sub-llaves; por eso `merchant_daily` solo usa sumas, conteos y máximos.

## 10. Tamaño de archivos

`repartition("dt")` antes de escribir deja un archivo por partición. Sin eso, con 8 particiones de shuffle se escribirían hasta 8 archivos pequeños por día, que a la larga encarecen Athena (más archivos = más solicitudes a S3) y hacen más lentas las lecturas.

## 11. Metadatos operativos en SQLite

Corridas, tareas (duración, filas leídas/escritas/en cuarentena, particiones, intentos, errores), eventos de esquema y watermark se guardan en SQLite: cero infraestructura en local. En AWS lo natural es una tabla en Postgres/RDS o DynamoDB, o eventos en CloudWatch. La interfaz (`Metadata`) aísla ese cambio.

## Lo que NO se hizo a propósito

- **Kafka/streaming:** el negocio consume indicadores diarios; un batch diario idempotente es más simple, barato y fácil de operar.
- **Kubernetes / EMR:** para este volumen, Spark local o Glue es suficiente.
- **Databricks:** no aporta nada que S3 + Glue + Athena no cubran aquí; agregarlo solo por el nombre sería sobreingeniería.
