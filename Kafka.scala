// Databricks notebook source
// MAGIC %md
// MAGIC # Topic

// COMMAND ----------

// MAGIC %md
// MAGIC ## Opciones
// MAGIC kafka-topics.sh

// COMMAND ----------

// MAGIC %md
// MAGIC ## Errores al crear
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --create
// MAGIC 
// MAGIC <pre>WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
// MAGIC Missing required argument "[partitions]"</pre>
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3
// MAGIC 
// MAGIC <pre>WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
// MAGIC Missing required argument "[replication-factor]"</pre>
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --
// MAGIC replication-factor 2
// MAGIC 
// MAGIC <pre>Error while executing topic command : Replication factor: 2 larger than available brokers: 1. 
// MAGIC ERROR org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 2 larger than available brokers: 1.</pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Crear
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --
// MAGIC replication-factor 1
// MAGIC 
// MAGIC <pre>Created topic &quot;first_topic&quot;.</pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Listar
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
// MAGIC 
// MAGIC <pre>first_topic</pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Descripcion
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --describe
// MAGIC 
// MAGIC <pre>Topic:first_topic	PartitionCount:3	ReplicationFactor:1	Configs:
// MAGIC 	Topic: first_topic	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
// MAGIC 	Topic: first_topic	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
// MAGIC 	Topic: first_topic	Partition: 2	Leader: 0	Replicas: 0	Isr: 0
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Crear un segundo topic
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic second_topic --create --partitions 6 --replication-factor 1
// MAGIC 
// MAGIC ## Listar los topics
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
// MAGIC 
// MAGIC <pre>first_topic
// MAGIC second_topic
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Eliminar 
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic second_topic --delete
// MAGIC 
// MAGIC <pre>Topic second_topic is marked for deletion.
// MAGIC Note: This will have no impact if delete.topic.enable is not set to true.
// MAGIC </pre>
// MAGIC 
// MAGIC ## Mostrar
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
// MAGIC 
// MAGIC <pre>first_topic</pre>

// COMMAND ----------

// MAGIC %md
// MAGIC # Producer

// COMMAND ----------

// MAGIC %md
// MAGIC ## Opciones 
// MAGIC 
// MAGIC kafka-console-producer.sh

// COMMAND ----------

// MAGIC %md
// MAGIC ## Broker 
// MAGIC 
// MAGIC Escribir en un topic
// MAGIC 
// MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic first_topic
// MAGIC 
// MAGIC <pre>&gt;Hola mundo
// MAGIC &gt;aprendiendo kafka
// MAGIC &gt;mas texto para seguir aprendiendo
// MAGIC &gt;otro mensaje
// MAGIC </pre>
// MAGIC 
// MAGIC Ctrl+C para salir

// COMMAND ----------

// MAGIC %md
// MAGIC ## Broker confirmacion
// MAGIC 
// MAGIC espera la sincronización de las replicas para 
// MAGIC realizar la confirmación, lo cual garantiza que el registro no se pierda mientras al menos
// MAGIC una replica se mantenga levantado y en ejecución, esta (acks=all) es la garantía mas fuerte de disponibilidad. 
// MAGIC 
// MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic firsopic --producer-property acks=all
// MAGIC 
// MAGIC <pre>&gt;Un mensaje que es confirmado
// MAGIC &gt;otro mensaje mas
// MAGIC &gt;seguimos aprendiendo
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Broker a un topic inexistente
// MAGIC 
// MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic new_topic
// MAGIC 
// MAGIC <pre>&gt;Mensaje en topic nuevo inexistente
// MAGIC WARN [Producer clientId=console-producer] Error while fetching metadata with correlation id 1 : {firnew_topic=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
// MAGIC &gt;otro mensaje
// MAGIC &gt;un ultimo mensaje</pre>
// MAGIC 
// MAGIC Al no existir ese topic, se crea uno nuevo automaticamente
// MAGIC 
// MAGIC ## Listar
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
// MAGIC 
// MAGIC <pre>firnew_topic
// MAGIC first_topic
// MAGIC </pre>
// MAGIC 
// MAGIC ## Descripcion
// MAGIC 
// MAGIC kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic firnew_topic --describe
// MAGIC 
// MAGIC <pre>Topic:firnew_topic	PartitionCount:1	ReplicationFactor:1	Configs:
// MAGIC 	Topic: firnew_topic	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md
// MAGIC # Consumer

// COMMAND ----------

// MAGIC %md
// MAGIC ## Opciones 
// MAGIC 
// MAGIC kafka-console-consumer.sh

// COMMAND ----------

// MAGIC %md
// MAGIC ## leer
// MAGIC  
// MAGIC Iniciamos el consumer para que este a la escucha de ese topic (no aparece ningun registro)
// MAGIC 
// MAGIC kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic
// MAGIC 
// MAGIC Despues se inicia el broker para introducir los datos
// MAGIC 
// MAGIC kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic first_topic
// MAGIC 
// MAGIC - Mensaje de broker:
// MAGIC <pre>&gt;hola nuevo mensaje para el consumer
// MAGIC &gt;mas mensajes de prueba
// MAGIC &gt;
// MAGIC </pre>
// MAGIC - Mensaje desde consumer:
// MAGIC <pre>hola nuevo mensaje para el consumer
// MAGIC mas mensajes de prueba
// MAGIC </pre>
// MAGIC  
// MAGIC Cancelamos el consumer (utilizando Ctrl+C) y lo lanzamos de nuevo pero indicando que
// MAGIC lea los mensajes desde el principio del topic y así podremos ver todos los mensajes 
// MAGIC además de los que vayan llegando
// MAGIC 
// MAGIC kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning
// MAGIC 
// MAGIC <pre>aprendiendo kafka
// MAGIC otro mensaje mas
// MAGIC hola nuevo mensaje para el consumer
// MAGIC Hola mundo
// MAGIC otro mensaje
// MAGIC Un mensaje que es confirmado
// MAGIC mas texto para seguir aprendiendo
// MAGIC seguimos aprendiendo
// MAGIC mas mensajes de prueba
// MAGIC 
// MAGIC </pre>
