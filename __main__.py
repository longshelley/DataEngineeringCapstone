import logging
from pyspark.sql import SparkSession

import selections
from cc_handler import CCHandler as CC

if __name__ == "__main__":
    try:
        # Set the log level to ERROR to suppress the log message
        logging.getLogger("py4j").setLevel(logging.ERROR)

        spark = SparkSession.builder.appName('capstonedemo').getOrCreate()
        
        branchdf = spark.read.json("..\\DataEngineeringCapstone\\data\\cdw_sapp_branch.json")
        creditdf = spark.read.json("..\\DataEngineeringCapstone\\data\\cdw_sapp_credit.json")
        customerdf = spark.read.json("..\\DataEngineeringCapstone\\data\\cdw_sapp_customer.json")

        CC = CC(branchdf, creditdf, customerdf)
        print(CC.branchdf)

        selections.main_menu(CC, spark)

    except FileNotFoundError:
        print("Missing data files")
