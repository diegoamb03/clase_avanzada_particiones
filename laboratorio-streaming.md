# Laboratorio: Structured Streaming con PySpark

De un puerto TCP a Apache Kafka, en tres prácticas que suben de complejidad.

**Duración estimada:** 75 a 90 minutos
**Nivel:** Python básico, SQL avanzado, conocimientos previos de DataFrames en PySpark

## Objetivos

Al terminar el laboratorio podrás:

1. Leer un stream con `spark.readStream` desde un socket y desde Kafka.
2. Convertir eventos JSON en columnas tipadas con `from_json`.
3. Elegir el modo de salida correcto (`append`, `update`, `complete`) según la consulta.
4. Agregar por ventanas de tiempo del evento usando watermark.
5. Escribir resultados de un stream en un topic de Kafka con checkpoint.

## Estructura de la carpeta

Crea una carpeta de trabajo y deja ahí todos los archivos del laboratorio:

```
streaming-lab/
├── lab1_socket.py
├── servidor_eventos.py
├── lab2a_parse.py
├── lab2b_ventanas.py
├── productor.py
├── lab3_kafka.py
└── reto_final.py
```

```bash
mkdir -p ~/streaming-lab && cd ~/streaming-lab
```

---

## Parte 0. Preparar la máquina virtual

Ejecuta cada comando y confirma que responde sin error antes de seguir.

```bash
java -version            # Spark 3.5 necesita Java 8, 11 o 17
python3 --version
python3 -c "import pyspark; print(pyspark.__version__)"
nc -h                    # netcat; si no existe: sudo apt install netcat-openbsd
docker --version
```

Si PySpark no está instalado:

```bash
pip install pyspark==3.5.1
```

> **Anota tu versión de Spark.** El conector de Kafka de la Parte 3 debe coincidir con ella. Con Spark 3.5.x se usa `spark-sql-kafka-0-10_2.12:<tu versión>`; con Spark 4.0.x, `spark-sql-kafka-0-10_2.13:<tu versión>`.

**Ajuste para las VMs del curso.** En todos los scripts usamos `spark.sql.shuffle.partitions = 2`. Por defecto Spark usa 200 particiones en cada agregación, y en una VM pequeña eso hace que cada micro-lote tarde varios segundos aunque solo lleguen tres eventos.

---

## Parte 1. Escuchar un puerto con netcat

**Idea:** netcat abre un servidor TCP en el puerto 9999. Spark se conecta como cliente y trata cada línea que escribas como un evento nuevo.

### Paso 1. Crear el script

`lab1_socket.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = (SparkSession.builder
         .appName("lab1_socket")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Fuente: un socket TCP
lineas = (spark.readStream
          .format("socket")
          .option("host", "localhost")
          .option("port", 9999)
          .load())

# 2. Consulta: separar palabras y contarlas
palabras = lineas.select(explode(split(col("value"), " ")).alias("palabra"))
conteo = palabras.groupBy("palabra").count()

# 3. Sink: la consola
consulta = (conteo.writeStream
            .outputMode("complete")
            .format("console")
            .start())

consulta.awaitTermination()
```

### Paso 2. Ejecutar en dos terminales

**Terminal A**, primero siempre:

```bash
nc -lk 9999
```

**Terminal B:**

```bash
cd ~/streaming-lab
python3 lab1_socket.py
```

Escribe en la Terminal A, por ejemplo `hola spark hola streaming`, y presiona Enter.

### Resultado esperado

```
-------------------------------------------
Batch: 1
-------------------------------------------
+---------+-----+
|  palabra|count|
+---------+-----+
|     hola|    2|
|    spark|    1|
|streaming|    1|
+---------+-----+
```

### Paso 3. Experimentos

Detén el script con `Ctrl+C`, haz un cambio a la vez y vuelve a ejecutarlo.

| # | Cambio | Qué observar |
|---|---|---|
| 1.1 | Cambia `complete` por `update` | ¿Qué palabras aparecen en cada lote? |
| 1.2 | Cambia a `append` | ¿Qué error lanza Spark y por qué? |
| 1.3 | Agrega `.trigger(processingTime="10 seconds")` antes de `.start()` y escribe varias líneas rápido | ¿Cuántos lotes se generan? |
| 1.4 | Con el script corriendo, cierra netcat con `Ctrl+C` | ¿Qué le pasa al stream? ¿Se puede recuperar lo que escribiste? |

### Preguntas

1. ¿Cuál es la diferencia entre la salida de `complete` y la de `update`?
2. ¿Por qué Spark no permite `append` con un `groupBy` sin watermark?
3. ¿Qué limitación del socket descubriste en el experimento 1.4?

---

## Parte 2. Un servidor que emite eventos

**Idea:** reemplazamos al humano que escribe en netcat por un programa que emite un evento JSON por segundo, como lo haría una aplicación real. Spark lo convierte en columnas, filtra y agrega por ventanas de tiempo.

### Paso 1. El servidor de eventos

`servidor_eventos.py`

Esta versión acepta reconexiones: cuando detienes Spark, el servidor espera a que vuelvas a conectarte, así no hay que reiniciarlo en cada prueba. Uno de cada diez eventos llega con 90 segundos de retraso para poder probar el watermark.

```python
import socket, json, time, random
from datetime import datetime, timedelta

USUARIOS = ["ana", "luis", "sofia", "juan"]
ACCIONES = ["login", "compra", "logout"]

def crear_evento():
    ts = datetime.now()
    if random.random() < 0.1:            # evento tardío
        ts -= timedelta(seconds=90)
    return {
        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "usuario": random.choice(USUARIOS),
        "accion": random.choice(ACCIONES),
        "monto": round(random.uniform(10, 500), 2),
    }

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("localhost", 9999))
srv.listen(1)
print("Esperando a Spark en el puerto 9999...")

while True:
    conn, addr = srv.accept()
    print("Spark conectado desde", addr)
    try:
        while True:
            evento = crear_evento()
            conn.sendall((json.dumps(evento) + "\n").encode())
            print("enviado:", evento)
            time.sleep(1)
    except (BrokenPipeError, ConnectionResetError):
        print("Spark se desconectó. Esperando una nueva conexión...")
        conn.close()
```

**Terminal A:**

```bash
python3 servidor_eventos.py
```

> El socket de Spark admite una conexión por consulta y este servidor atiende una a la vez. Por eso la Parte 2 tiene dos scripts que se ejecutan por separado, nunca al mismo tiempo.

### Paso 2. De texto JSON a columnas

`lab2a_parse.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp

spark = (SparkSession.builder
         .appName("lab2a_parse")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# El esquema se escribe como en un CREATE TABLE
esquema = "ts STRING, usuario STRING, accion STRING, monto DOUBLE"

crudo = (spark.readStream.format("socket")
         .option("host", "localhost").option("port", 9999)
         .load())

eventos = (crudo
           .select(from_json(col("value"), esquema).alias("e"))
           .select("e.*")
           .withColumn("ts", to_timestamp("ts")))

compras = eventos.filter(col("accion") == "compra")

(compras.writeStream
 .outputMode("append")
 .format("console")
 .option("truncate", False)
 .start()
 .awaitTermination())
```

**Terminal B:**

```bash
python3 lab2a_parse.py
```

**Resultado esperado:** una tabla por lote con las columnas `ts`, `usuario`, `accion`, `monto`, solo con compras.

**Ejercicios 2A**

1. Agrega una columna `monto_iva` igual a `monto * 1.19`.
2. Muestra solo compras mayores a 300.
3. Cambia el nombre de un campo en el esquema (por ejemplo `usuario` por `user`). ¿Qué pasa con esa columna? ¿Spark lanza error?

### Paso 3. Ventanas de tiempo y watermark

Detén `lab2a_parse.py` con `Ctrl+C`. El servidor sigue corriendo y acepta la nueva conexión.

`lab2b_ventanas.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, to_timestamp,
                                   window, count, sum as _sum)

spark = (SparkSession.builder
         .appName("lab2b_ventanas")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

esquema = "ts STRING, usuario STRING, accion STRING, monto DOUBLE"

crudo = (spark.readStream.format("socket")
         .option("host", "localhost").option("port", 9999)
         .load())

eventos = (crudo
           .select(from_json(col("value"), esquema).alias("e"))
           .select("e.*")
           .withColumn("ts", to_timestamp("ts")))

ventas = (eventos
          .withWatermark("ts", "1 minute")
          .groupBy(window("ts", "30 seconds"), "accion")
          .agg(count("*").alias("eventos"),
               _sum("monto").alias("total"))
          .select(col("window.start").alias("inicio"),
                  col("window.end").alias("fin"),
                  "accion", "eventos", "total"))

(ventas.writeStream
 .outputMode("update")
 .format("console")
 .option("truncate", False)
 .trigger(processingTime="10 seconds")
 .start()
 .awaitTermination())
```

```bash
python3 lab2b_ventanas.py
```

**Resultado esperado:** cada 10 segundos, las filas de las ventanas de 30 segundos que cambiaron, por acción.

**Ejercicios 2B**

1. Observa la Terminal A. Cuando el servidor envía un evento con 90 segundos de retraso, ¿aparece en alguna ventana? ¿Por qué?
2. Cambia el watermark a `"2 minutes"`. ¿Qué pasa ahora con los eventos tardíos?
3. Cambia a ventanas deslizantes: `window("ts", "1 minute", "30 seconds")`. ¿En cuántas ventanas cae cada evento?
4. Cambia `update` por `append`. Ahora sí se permite: ¿por qué? ¿Cuándo aparece cada ventana en la consola?

---

## Parte 3. Integración con Apache Kafka

**Idea:** el mismo evento, ahora viajando por Kafka. A diferencia del socket, Kafka guarda los eventos en disco: si Spark se cae, no se pierde nada.

```
productor.py ──► topic "eventos" ──► Spark ──► consola (ventanas)
                                          └──► topic "alertas"
```

### Paso 1. Levantar Kafka en Docker

```bash
# Un solo nodo en modo KRaft (sin ZooKeeper)
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0

# Esperar unos segundos y crear los topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic eventos --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic alertas --bootstrap-server localhost:9092

# Verificar
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

Si `docker` pide `sudo`, antepónlo a cada comando o agrega tu usuario al grupo `docker`.

Instala el cliente de Kafka para Python:

```bash
pip install kafka-python
```

### Paso 2. El productor

`productor.py`

```python
import json, time, random
from datetime import datetime
from kafka import KafkaProducer

USUARIOS = ["ana", "luis", "sofia", "juan"]
ACCIONES = ["login", "compra", "logout"]

productor = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Enviando eventos al topic 'eventos'. Ctrl+C para detener.")
while True:
    evento = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "usuario": random.choice(USUARIOS),
        "accion": random.choice(ACCIONES),
        "monto": round(random.uniform(10, 500), 2),
    }
    productor.send("eventos", evento)
    print("enviado:", evento)
    time.sleep(0.5)
```

**Terminal A:**

```bash
python3 productor.py
```

Comprueba que los eventos llegan al topic, en otra terminal:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic eventos --bootstrap-server localhost:9092
```

### Paso 3. Spark lee de Kafka y escribe alertas

`lab3_kafka.py`

Ajusta `PAQUETE_KAFKA` a tu versión de Spark (ver Parte 0). La primera ejecución descarga el conector desde Maven, así que la VM necesita internet.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, to_timestamp, window,
                                   count, sum as _sum)

PAQUETE_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SERVIDOR = "localhost:9092"

spark = (SparkSession.builder
         .appName("lab3_kafka")
         .config("spark.jars.packages", PAQUETE_KAFKA)
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

esquema = "ts STRING, usuario STRING, accion STRING, monto DOUBLE"

# 1. Fuente: el topic "eventos"
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", SERVIDOR)
         .option("subscribe", "eventos")
         .option("startingOffsets", "earliest")
         .load())

# key y value llegan como bytes: hay que castearlos a texto
eventos = (crudo
           .select(from_json(col("value").cast("string"), esquema).alias("e"),
                   col("partition"), col("offset"))
           .select("e.*", "partition", "offset")
           .withColumn("ts", to_timestamp("ts")))

# 2. Consulta A: ventanas por acción, a la consola
ventas = (eventos
          .withWatermark("ts", "1 minute")
          .groupBy(window("ts", "30 seconds"), "accion")
          .agg(count("*").alias("eventos"), _sum("monto").alias("total")))

q_ventanas = (ventas.writeStream
              .outputMode("update")
              .format("console")
              .option("truncate", False)
              .option("checkpointLocation", "/tmp/chk/lab3_ventanas")
              .start())

# 3. Consulta B: compras mayores a 400, al topic "alertas"
alertas = (eventos
           .filter((col("accion") == "compra") & (col("monto") > 400))
           .selectExpr("usuario AS key", "to_json(struct(*)) AS value"))

q_alertas = (alertas.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", SERVIDOR)
             .option("topic", "alertas")
             .option("checkpointLocation", "/tmp/chk/lab3_alertas")
             .outputMode("append")
             .start())

# Mantener vivas las dos consultas
spark.streams.awaitAnyTermination()
```

**Terminal B:**

```bash
python3 lab3_kafka.py
```

**Terminal C**, para ver las alertas:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic alertas --bootstrap-server localhost:9092
```

### Resultado esperado

En la Terminal B, cada lote muestra ventanas por acción. En la Terminal C aparecen JSON como este:

```json
{"ts":"2026-10-05T10:12:31.000-05:00","usuario":"sofia","accion":"compra","monto":456.12,"partition":0,"offset":87}
```

### Paso 4. Probar la tolerancia a fallos

1. Con todo corriendo, detén `lab3_kafka.py` con `Ctrl+C`. Deja el productor encendido.
2. Espera 30 segundos.
3. Vuelve a ejecutar `python3 lab3_kafka.py`.
4. Observa el primer lote: procesa de una vez todo lo que llegó mientras Spark estaba detenido.
5. Revisa la Terminal C: no deben aparecer alertas repetidas.

Ahora repite la prueba borrando los checkpoints antes de reiniciar:

```bash
rm -rf /tmp/chk/lab3_ventanas /tmp/chk/lab3_alertas
```

### Preguntas

1. En la prueba con checkpoint, ¿desde qué offset retomó Spark? ¿Usó `startingOffsets`?
2. Sin checkpoint y con `earliest`, ¿qué pasó en el topic `alertas`?
3. ¿Qué cambiarías para que, sin checkpoint, Spark solo lea eventos nuevos? ¿Qué se perdería?
4. ¿Por qué cada consulta necesita su propia carpeta de checkpoint?

---

## Reto final

Usando Kafka como fuente, calcula el **total de compras por usuario en ventanas de 1 minuto** y publica en el topic `alertas` solo los usuarios cuyo total en la ventana supere un umbral que tú definas.

Requisitos:

- Watermark de 1 minuto.
- El `key` del mensaje es el usuario.
- El `value` es un JSON con el inicio de la ventana, el usuario y el total.
- Checkpoint en `/tmp/chk/reto`.

<details>
<summary>Solución de referencia</summary>

`reto_final.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, to_timestamp, window,
                                   sum as _sum, to_json, struct)

PAQUETE_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SERVIDOR = "localhost:9092"
UMBRAL = 800

spark = (SparkSession.builder
         .appName("reto_final")
         .config("spark.jars.packages", PAQUETE_KAFKA)
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

esquema = "ts STRING, usuario STRING, accion STRING, monto DOUBLE"

eventos = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", SERVIDOR)
           .option("subscribe", "eventos")
           .option("startingOffsets", "latest")
           .load()
           .select(from_json(col("value").cast("string"), esquema).alias("e"))
           .select("e.*")
           .withColumn("ts", to_timestamp("ts")))

grandes = (eventos
           .filter(col("accion") == "compra")
           .withWatermark("ts", "1 minute")
           .groupBy(window("ts", "1 minute"), "usuario")
           .agg(_sum("monto").alias("total"))
           .filter(col("total") > UMBRAL)
           .select(col("usuario").alias("key"),
                   to_json(struct(col("window.start").alias("inicio"),
                                  "usuario", "total")).alias("value")))

(grandes.writeStream
 .format("kafka")
 .option("kafka.bootstrap.servers", SERVIDOR)
 .option("topic", "alertas")
 .option("checkpointLocation", "/tmp/chk/reto")
 .outputMode("update")
 .start()
 .awaitTermination())
```

Con `update`, un usuario puede aparecer varias veces en la misma ventana a medida que su total crece. Con `append` aparecería una sola vez, cuando el watermark cierre la ventana, a cambio de esperar más.

</details>

---

## Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `Connection refused` al iniciar Spark | Spark arrancó antes que netcat o el servidor | Inicia primero el servidor en la Terminal A |
| `Failed to find data source: kafka` | Falta el conector de Kafka | Revisa `spark.jars.packages` y que coincida con tu versión de Spark |
| La descarga del paquete falla | La VM no tiene salida a Maven Central | Revisa la red o pide el jar al instructor |
| Cada lote tarda mucho | 200 particiones de shuffle por defecto | `spark.sql.shuffle.partitions = 2` |
| Columnas en `null` después de `from_json` | El esquema no coincide con el JSON | Revisa nombres y tipos de cada campo |
| El stream falla al reiniciar con otro código | El checkpoint es de una consulta distinta | Usa una carpeta nueva o borra la anterior |
| `NoBrokersAvailable` en el productor | Kafka aún no termina de arrancar | Espera unos segundos; revisa `docker logs kafka` |
| Error con `six.moves` al importar `kafka` | Versión antigua de kafka-python en Python 3.12 | `pip install -U kafka-python` |

## Limpieza

```bash
docker stop kafka && docker rm kafka
rm -rf /tmp/chk
```
