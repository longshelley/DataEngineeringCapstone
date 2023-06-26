import calendar
import requests
import pyspark, json
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, concat_ws, lit, substring, initcap, lower, regexp_replace, to_timestamp
from pyspark.sql.types import StringType

from cc_handler import CCHandler

class DataVisualization:
    def transaction_type_count():
        # Group by transaction type and count the number of transactions
        transaction_count = creditdf.groupBy("TRANSACTION_TYPE").count().alias("transaction_count")

        # Sort the transactions by count in descending order
        sorted_transactions = transaction_count.orderBy("count", ascending=False).collect()

        # Get transaction types and counts
        transaction_types = [row["TRANSACTION_TYPE"] for row in sorted_transactions]
        transaction_count = [row["count"] for row in sorted_transactions]

        # Plot transaction types and counts
        bars = plt.bar(transaction_types, transaction_count)
        plt.xlabel("Transaction Type")
        plt.ylabel("Transaction Count")
        plt.title("Transaction Type vs Transaction Count")
        plt.xticks(rotation=70)

        # Labels and colors for readability
        bars[0].set_color("#dd6e42")
        for bar in bars[1:]:
            bar.set_color("#4f6d7a")

        for i in range(len(transaction_types)):
            plt.text(i, transaction_count[i], str(transaction_count[i]), ha="center", va="bottom").set_color("#4f6d7a")

        plt.show()