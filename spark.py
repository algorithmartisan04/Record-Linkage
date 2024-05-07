# pylint: disable=E0401

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types

import splink.spark.comparison_library as cl
from splink.spark.jar_location import similarity_jar_location
from splink.spark.linker import SparkLinker

import logging
import time
import datetime
import sys

#Makes a spark Conf object
conf = SparkConf()
conf.setAppName("Splink Spark")
# Adds the necessary jars for splink comparison to spark
path = similarity_jar_location()
conf.set("spark.jars", path)


conf.set("spark.default.parallelism", "10") # HACK: Change from 5 to 1
# conf.set("spark.memory.fraction", "0.6") # Changing from 0.6 to 0.8 causes error
# conf.set("spark.executor.memory", "30g")
# conf.set("spark.driver.memory", "10g")


# Set the current spark conf and add it to the cluster alongside cluster set conf

# Also set the temp dir
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
# spark.sparkContext.setCheckpointDir("/mmfs1/home/seunguk/spark/temp")
# HACK: Testing new temp folder
spark.sparkContext.setCheckpointDir("/gscratch/stf/sid04/spark_temp")

# Supress log level
# logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
# for log in logs:
#     logging.getLogger(log).setLevel(logging.ERROR)

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "id",
    "comparisons": [
        cl.levenshtein_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="res_street_address", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="birth_year", distance_threshold_or_thresholds=1, include_exact_match_level=False)
    ],
    # Blocking used here
    "blocking_rules_to_generate_predictions": [
        "substr(l.first_name,1,1) = substr(r.first_name,1,1) and substr(l.last_name,1,2) = substr(r.last_name,1,2) and l.zip_code = r.zip_code"
        ]
}

path = "/gscratch/stf/hackathon/recordlinkage/small"
path_big = "/gscratch/stf/sid04/big_df/"
# TODO: Change here to fit desired levels
lst = [2000]
for x in lst:
    dfA = spark.read.csv(path + str(x) + "_dfA.csv", header = True)
    dfB = spark.read.csv(path + str(x) + "_dfB.csv", header = True)

    time_start = time.time()

    # Creates the linker object used to compare dfA and dfB and
    linker = SparkLinker([dfA, dfB], settings)
    # linker.max_pairs(target_rows=1e6)
    linker.estimate_u_using_random_sampling(max_pairs=1e7)
    training = ["l.first_name = r.first_name",
                "l.middle_name = r.middle_name",
                "l.last_name = r.last_name",
                "l.res_street_address = r.res_street_address",
                "l.birth_year = r.birth_year"
                ]

    # Trainig the linker on predetermined training matches from above
    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i)
    predict = linker.predict(0.5)

    time_end = time.time()

    # Transform into dataframe to calculate the accuracy
    df_predict = predict.as_pandas_dataframe()

    # Total number of comparisons from the given blocking rule
    # pairs = linker.count_num_comparisons_from_blocking_rule("l.zip_code = r.zip_code")
    # TODO
    pairs = linker.count_num_comparisons_from_blocking_rule("substr(l.first_name,1,1) = substr(r.first_name,1,1) and substr(l.last_name,1,2) = substr(r.last_name,1,2) and l.zip_code = r.zip_code")

    false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
    true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
    false_negative = round(x / 2) - true_positive

    precision = true_positive / (true_positive + false_positive)
    recall = true_positive / (true_positive + false_negative)

    now = datetime.datetime.now()

    # Print out the output
    with open("/mmfs1/home/sid04/spark.txt", "a") as f:
        f.writelines(
            "Sample Size: " + str(f"{x:_}") +
            "|Links Predicted: " + str(f"{len(df_predict):_}") +
            "|Time Taken: " + str(f"{round((time_end - time_start),2):_}") +
            "|Precision: " + str(round(precision, 4)) +
            "|Recall: " + str(round(recall,4)) +
            "|Linkage Pairs: " + str(f"{pairs:_}") +
            "|Finish Time: " + str(now) +
            "|Blocking rules: "+ str(settings["blocking_rules_to_generate_predictions"]) +
            "\n"
        )

sc.stop()
spark.stop()