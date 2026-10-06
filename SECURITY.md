# Seguridad

MIDAS es un proyecto de portafolio con datos 100% sintéticos: el generador inventa clientes, comercios y pagos. No almacena información real de personas ni credenciales.

## Cómo reportar un problema
Si encuentras una vulnerabilidad, por favor **no abras un issue público**. Usa **[Report a vulnerability](https://github.com/diegosaaval/midas-data-pipeline/security/advisories/new)** (pestaña *Security* del repositorio). Respondo en un máximo de 7 días.

## Medidas implementadas
| Riesgo | Medida |
|---|---|
| Pantalla de etapas | Solo lectura: abre la base de metadatos en modo `ro`, no tiene acciones que cambien el pipeline y escucha solo en `127.0.0.1`. Los planes de ejecución se piden por nombre validado con una expresión regular (sin rutas: nada de `../`). |
| XSS / clickjacking | Todo el contenido dinámico se escapa en la interfaz; encabezados `Content-Security-Policy` (solo recursos propios, sin scripts en línea), `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy` y `Permissions-Policy`. |
| Datos inválidos de las fuentes | Nada se ejecuta a partir de los datos: bronze guarda todo como texto y silver tipa con `try_cast`, así un valor malicioso o corrupto va a cuarentena con su motivo. |
| Secretos | Ninguno en el repositorio. En AWS, el acceso a S3, Glue y Athena es por roles IAM de mínimo privilegio, no por llaves. |
| Contenedor | Imagen oficial de Airflow con usuario sin privilegios (`airflow`). En `docker-compose`, Airflow escucha solo en `127.0.0.1` y Postgres no expone puertos; su clave se cambia con `POSTGRES_PASSWORD`. |
| Dependencias | Alertas de seguridad de GitHub, `pip-audit` y CodeQL (Python y JavaScript) en cada cambio. Las actualizaciones se aplican y se prueban con el CI antes de integrarlas; pandas y pyspark mayores, con pruebas aparte. |

## Fuera de alcance
El Airflow de `docker-compose` es para desarrollo local: todos sus usuarios son administradores y no tiene inicio de sesión. En un uso real iría en un servicio administrado (MWAA) o detrás del inicio de sesión corporativo (SSO), y la pantalla de etapas detrás de un proxy autenticado.
