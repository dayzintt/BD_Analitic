import os
import findspark
from pyspark.sql import SparkSession

# Defina o HADOOP_HOME diretamente no script
os.environ["HADOOP_HOME"] = 'C:/hadoop'

# Defina o JAVA_HOME diretamente no script
os.environ["JAVA_HOME"] = 'C:/Program Files/Java/jdk-22'

# Inicialize o findspark
findspark.init()

# Inicialize a SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()

# Caminho do arquivo CSV original
file_loc = 'F:/coisas de bigdata/tb_bb_2023-12.csv'

# Leia o arquivo CSV
dataset = spark.read.csv(file_loc, inferSchema=True, header=True)

# Filtrar as linhas onde SG_UF é RJ
filtered_dataset = dataset.filter(dataset.SG_UF == 'RJ')

# Caminho para salvar o arquivo CSV filtrado
output_loc = 'F:/coisas de bigdata/tb_bb_2023-12_filtered'

# Salvar o DataFrame filtrado como um novo arquivo CSV com overwrite mode
filtered_dataset.write.mode("overwrite").csv(output_loc, header=True)

# Pare a SparkSession
spark.stop()
