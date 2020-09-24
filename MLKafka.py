import json
import sys

from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType
from pyspark.streaming import StreamingContext

# getting the kafka brokers from CL args
kafkaBrokers = sys.argv[1]

# initializing Spark and streaming context
sc = SparkContext(appName="PythonSparkStreamingKafka")
ssc = StreamingContext(sc, 2)

# load the model saved by MLModel.py
model = PipelineModel.load("hdfs:///user/spark/model")

# Spark Structured streaming, retrieving the data from Kafka
df = ssc \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaBrokers) \
    .option("subscribe", "carstream") \
    .load()

# getting the JSONs and making them as a string dataframe
rows = df.selectExpr("CAST(value AS STRING)")

# schema from CarData.java
schema = StructType([StructField("Id", FloatType(), False),
                     StructField("coolantTemp", FloatType(), False),
                     StructField("intakeAirTemp", FloatType(), False),
                     StructField("intakeAirFlowSpeed", FloatType(), False),
                     StructField("batteryPercentage", FloatType(), False),
                     StructField("batteryVoltage", FloatType(), False),
                     StructField("speed", FloatType(), False),
                     StructField("engineVibrationAmplitude", FloatType(), False),
                     StructField("throttlePos", FloatType(), False),
                     StructField("tirePressure11", FloatType(), False),
                     StructField("tirePressure12", FloatType(), False),
                     StructField("tirePressure21", FloatType(), False),
                     StructField("tirePressure22", FloatType(), False),
                     StructField("accelerometer11Value", FloatType(), False),
                     StructField("accelerometer12Value", FloatType(), False),
                     StructField("accelerometer21Value", FloatType(), False),
                     StructField("accelerometer22Value", FloatType(), False),
                     StructField("controlUnitFirmware", IntegerType(), False),
                     StructField("failureOccurred", BooleanType(), False)])

# parsing the JSONs based on the schema
unlabeled = rows.map(lambda x: json.loads(x[1], schema)).drop("failureOccurred")

# make the predictions
predictionsDF = model.transform(unlabeled)

# start the streaming
ssc.start()
ssc.awaitTermination()
