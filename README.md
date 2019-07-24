# hdfs-spark-kafka
ensure good connectivity between Apche HDFS and Apache Spark and Apache Kafka "Read csv file from HDFS / Process it with spark / Produce it in Kafka / Consume product data / Validate test

#	Application Interaction Spark/HDFS/Kafka

Cette application a pour but tester les interactions entre Apache Spark et Apache HDFS et Apache Cassandra, elle permet :

-	La lecture d’un fichier csv depuis HDFS. 
-	Faire des traitements Spark sur le fichier csv créer.
-	La production des diffusions des données traiter. 
-	La consommation de ces diffusions de données. 
-	Générer un fichier résultat en format JSON, qui contient les informations du test et son résultat. 

	Composants concernés


Composant	   	Version
- Spark		2.3.2
- HDFS		2.6.0
- Kafka		1.1.1
- DC-OS		1.12
- Scala		2.11.8
- Assembly		0.14.9


# Prérequis 
-	 Avant de lancer l’application vous devez la configurer, cela se fait au niveau du fichier de configuration de l’application, qui est dans le chemin (/src/main/resources/Kafka.conf).


# Traitements 
-	Lancer l’application sur le dcos bootstrap avec la commande 
dcos spark --name="spark-2-3" run --submit-args="--conf spark.mesos.containerizer=docker --conf spark.driver.memory=4G --conf spark.cores.max=3 --conf spark.executor.cores=1 --conf spark.executor.memory=4G --conf spark.mesos.executor.docker.forcePullImage=false --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs:///spark_history  --class hdfsToKafka hdfs:///jars/HDFS-Spark-Kafka-assembly-0.1.jar"
 
# Validation du test 
Vérifier le statut du test dans le fichier résultat. 
