import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fonction pour créer un keyspace Cassandra
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace 'spark_streams' created successfully!")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")

# Fonction pour créer une table dans Cassandra
def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT);
        """)
        logging.info("Table 'created_users' created successfully in keyspace 'spark_streams'!")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

# Fonction pour créer une connexion Spark
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        # Configuration pour ignorer les logs non nécessaires
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


# Fonction pour connecter Kafka à Spark
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created: {e}")
        return None

# Fonction pour créer une connexion Cassandra
def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['cassandra'], auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("Connected to Cassandra successfully!")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None

# Fonction pour définir la structure des données et extraire les informations de Kafka
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    return sel

# Point d'entrée principal
if __name__ == "__main__":
    
    # Création de la connexion Spark
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        
        # Connexion à Kafka via Spark
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            # Création de la connexion Cassandra
            session = create_cassandra_connection()

            if session is not None:
                # Création du keyspace et de la table dans Cassandra
                create_keyspace(session)
                session.set_keyspace('spark_streams')  # Connexion au keyspace
                create_table(session)

                logging.info("Streaming is being started...")

                # Définition du flux de données en temps réel vers Cassandra
                selection_df = create_selection_df_from_kafka(spark_df)
                streaming_query = (selection_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')  # emplacement pour le checkpoint
                                   .option('keyspace', 'spark_streams')  # keyspace dans Cassandra
                                   .option('table', 'created_users')  # table dans le keyspace
                                   .start())

                streaming_query.awaitTermination()
