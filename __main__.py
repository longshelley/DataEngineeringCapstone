from pyspark.sql import SparkSession

import selections


def clear():
    # check and make call for specific operating system
    _ = call('clear' if os.name == 'posix' else 'cls')

if __name__ == "__main__":
    try:
        spark = SparkSession.builder.appName('capstonedemo').getOrCreate()
        branchdf = spark.read.json("..\\data\\cdw_sapp_branch.json")
        creditdf = spark.read.json("..\\data\\cdw_sapp_credit.json")
        customerdf = spark.read.json("..\\data\\cdw_sapp_customer.json")

        CCHandler(branchdf, creditdf, customerdf)

        selections.main_menu(spark)

    except FileNotFoundError:
        print("Missing data files")
