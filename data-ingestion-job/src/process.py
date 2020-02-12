# -*- coding: utf-8 -*-
import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def main():
    """
    Cette methode est le point d'entrée de mon job.
    Elle va essentiellement faire 3 choses:
        - recuperer les arguments passés via la ligne de commande
        - creer une session spark
        - lancer le traitement
    Le traitement ne doit pas se faire dans la main pour des soucis de testabilité.
    """
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-e', "--ecoles_data", help='Ecoles dataset', required=True)
    parser.add_argument('-l', "--logements_data", help='Logements dataset', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.ecoles_data,args.logements_data,args.output)

def valueToCategory(value):
    premier_deg = [101, 102, 103, 111, 151, 152, 153, 160, 161, 162, 169, 170]
    deuxieme_deg = [300, 332, 350, 340, 306, 154, 310, 301, 312, 320, 390, 352, 335, 334, 370, 344, 349, 342, 302, 315]
    if value in premier_deg: return 1
    elif value in deuxieme_deg: return 2
    else: return -1


def process(spark, ecoles_data, logements_data, output):
    """
    Contient les traitements qui permettent de lire,transformer et sauvegarder mon resultat.
    :param spark: la session spark
    :param videos_file:  le chemin du dataset des videos
    :param categories_file: le chemin du dataset des categories
    :param output: l'emplacement souhaité du resultat
    """
    ecoles = spark.read.option('header', 'true').option('inferSchema','true').option('delimiter',';').csv(ecoles_data)
    
    logements = spark.read.option('header', 'true').option('inferSchema','true').option('delimiter',',').csv(logements_data)

    local_type = ['Appartement','Dépendance','Maison']
    logements = logements.filter(logements['type_local'].isin(local_type))
    logements = logements.filter(logements['valeur_fonciere'].isNotNull())

    premier_deg = [101,102,103,111,151,152,153,160,161,162,169,170]
    ecoles = ecoles.withColumnRenamed('Code Nature','code_nature')
    tmp = ecoles.select("code_nature").distinct().collect()
    tmp = [x.code_nature for x in tmp]
    deuxieme_deg = [x for x in tmp if x not in premier_deg]
    
    udfValueToCategory = udf(valueToCategory, IntegerType())
    ecoles = ecoles.withColumn('degré', udfValueToCategory('code_nature'))
    
    ecoles = ecoles.filter(ecoles['Etat établissement'].isin("OUVERT","A OUVRIR"))
    for i in ecoles.columns:
         ecoles = ecoles.withColumnRenamed(i, str("ecole_"+ i.replace("é","e")))
    for i in logements.columns:
         logements = logements.withColumnRenamed(i, i.replace("é","e"))

    ecoles = ecoles.withColumnRenamed("ecole_Code commune", "ecole_Code_commune")
    ecoles = ecoles.withColumn("ecole_Code_commune", ecoles.ecole_Code_commune.cast('integer'))
    logements = logements.withColumn("code_commune", logements.code_commune.cast('integer'))

    df_join = logements.join(ecoles, logements.code_commune == ecoles.ecole_Code_commune,'inner')

    
    df_join.write.csv(output)
   #df_join.write.parquet(output)


if __name__ == '__main__':
    main()
