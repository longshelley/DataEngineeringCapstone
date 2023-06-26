import calendar
import requests
import pyspark, json
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, concat_ws, lit, substring, initcap, lower, regexp_replace, to_timestamp
from pyspark.sql.types import StringType

class CCHandler:
    def __init__(self, branchdf, creditdf, customerdf):
        self.branchdf = branchdf
        self.creditdf = creditdf
        self.customerdf = customerdf

        # Apply mapping logic to CDW_SAPP_CUSTOMER.JSON

        self.customerdf = self.customerdf.withColumn("SSN", col("SSN").cast("int"))

        self.customerdf = self.customerdf.withColumn("FIRST_NAME", initcap(col("FIRST_NAME")))

        self.customerdf = self.customerdf.withColumn("MIDDLE_NAME", lower(col("MIDDLE_NAME")))

        self.customerdf = self.customerdf.withColumn("LAST_NAME", initcap(col("LAST_NAME")))

        self.customerdf = self.customerdf.withColumnRenamed("CREDIT_CARD_NO", "Credit_card_no")

        # Concatenate STREET_NAME and APT_NO with comma separator
        self.customerdf = self.customerdf.withColumn("FULL_STREET_ADDRESS", concat_ws(", ", col("STREET_NAME"), col("APT_NO")))

        # Format the phone number
        self.customerdf = self.customerdf.withColumn("CUST_PHONE", regexp_replace(col("CUST_PHONE"), 
                                                                        r"(\d{3})(\d{4})", 
                                                                        concat(lit("(555)"), lit(" "), substring(col("CUST_PHONE"), 0, 3), lit("-"), substring(
                                                                            col("CUST_PHONE"), 4, 7))))

        # Convert LAST_UPDATED to TIMESTAMP
        self.customerdf = self.customerdf.withColumn("LAST_UPDATED", to_timestamp(col("LAST_UPDATED"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

        # Reorder columns to match json file
        self.customerdf = self.customerdf.select(self.customerdf.FIRST_NAME, self.customerdf.MIDDLE_NAME, self.customerdf.LAST_NAME, self.customerdf.SSN, self.customerdf.Credit_card_no, 
                                        self.customerdf.FULL_STREET_ADDRESS, self.customerdf.CUST_CITY, self.customerdf.CUST_STATE, self.customerdf.CUST_COUNTRY,
                                        self.customerdf.CUST_ZIP, self.customerdf.CUST_PHONE, self.customerdf.CUST_EMAIL, self.customerdf.LAST_UPDATED)

        # Save the transformed customer data to the target table
        self.customerdf.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "credit_card_system") \
            .mode("overwrite") \
            .option("user", "root") \
            .option("password", "password") \
            .save()
        
        self.creditdf.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
            .mode("overwrite") \
            .option("user", "root") \
            .option("password", "password") \
            .save()
        self.branchdf.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
            .option("dbtable", "CDW_SAPP_BRANCH") \
            .mode("overwrite") \
            .option("user", "root") \
            .option("password", "password") \
            .save()
        
    def get_transactions(self, spark):
        self.creditdf.createOrReplaceTempView("transactions")
        self.customerdf.createOrReplaceTempView("customers")

        input_zip_code = input("Enter zip code: ")  #23223
        input_month = int(input("Enter month: "))   #2
        input_year = int(input("Enter year: "))     #2018

        # Query to filter transactions by zip code, month, and year, ordered by day in descending order
        query = """
        SELECT t.* 
        FROM transactions t
        JOIN customers c ON t.CREDIT_CARD_NO = c.CREDIT_CARD_NO
        WHERE c.CUST_ZIP = '{}' AND t.MONTH = {} AND t.YEAR = {}
        ORDER BY t.DAY DESC
        """.format(input_zip_code, input_month, input_year)

        result = spark.sql(query).select(self.creditdf.TRANSACTION_ID, self.creditdf.DAY, self.creditdf.MONTH, self.creditdf.YEAR, self.creditdf.CREDIT_CARD_NO, 
                                        self.creditdf.CUST_SSN, self.creditdf.BRANCH_CODE, self.creditdf.TRANSACTION_TYPE, self.creditdf.TRANSACTION_VALUE)
        result.show()
        
    def get_count_value(self, transaction_type, spark):
        # Query to count how many transaction and calculate total value of transactions by type
        query = """
        SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT, SUM(TRANSACTION_VALUE) AS TOTAL_VALUE
        FROM transactions
        WHERE TRANSACTION_TYPE = '{}'
        GROUP BY TRANSACTION_TYPE
        """.format(transaction_type)

        result = spark.sql(query)
        result.show()

    def get_value_total(self, state, spark):
        self.branchdf.createOrReplaceTempView("branches")

        # Query to count the number and calculate the total value of transactions by branch and state
        query = """
        SELECT b.BRANCH_CODE, b.BRANCH_NAME, SUM(t.TRANSACTION_VALUE) AS total_value, COUNT(*) AS total_transactions
        FROM branches b
        JOIN transactions t ON b.BRANCH_CODE = t.BRANCH_CODE
        WHERE b.BRANCH_STATE = '{}'
        GROUP BY b.BRANCH_CODE, b.BRANCH_NAME
        """.format(state)

        result = spark.sql(query)
        result.show()

    def get_customer(self, ssn, spark):
        # Query for customer
        query = """
        SELECT *
        FROM customers
        WHERE SSN = {}
        """.format(ssn)

        result = spark.sql(query)
        result.show()

    def set_customer(self, ssn, updated_first_name, updated_middle_name, 
                     updated_last_name, updated_apt_no, 
                     updated_street_name, updated_cust_city,
                     updated_cust_state, updated_cust_zip, 
                     updated_cust_phone, updated_cust_email):
        # Read the JSON file
        with open("cdw_sapp_customer.json", "r") as file:
            customers_json = [json.loads(line) for line in file]

        # Find the customer with the specified SSN and update their details
        for customer in customers_json:
            if customer["SSN"] == ssn:
                # Update the fields that have changed
                if updated_first_name != "":
                    customer["FIRST_NAME"] = updated_first_name
                if updated_middle_name != "":
                    customer["MIDDLE_NAME"] = updated_middle_name
                if updated_last_name != "":
                    customer["LAST_NAME"] = updated_last_name
                if updated_apt_no != "":
                    customer["APT_NO"] = updated_apt_no
                if updated_street_name != "":
                    customer["STREET_NAME"] = updated_street_name
                if updated_cust_city != "":
                    customer["CUST_CITY"] = updated_cust_city
                if updated_cust_state != "":
                    customer["CUST_STATE"] = updated_cust_state
                if updated_cust_zip != "":
                    customer["CUST_ZIP"] = updated_cust_zip
                if updated_cust_phone != "":
                    customer["CUST_PHONE"] = updated_cust_phone
                if updated_cust_email != "":
                    customer["CUST_EMAIL"] = updated_cust_email
                break

        # Write the updated customer data back to the JSON file
        with open("cdw_sapp_customer.json", "w") as file:
            for customer in customers_json:
                json.dump(customer, file)
                file.write("\n")

    def get_monthly_bill(self, credit_card_number, input_month, input_year):
        # Filter credit based on credit card number, month, and year
        filtered_df = self.creditdf.filter((self.creditdf["CREDIT_CARD_NO"] == credit_card_number) &
                                (self.creditdf["MONTH"] == input_month) &
                                (self.creditdf["YEAR"] == input_year))\
                                    .select(self.creditdf.YEAR, self.creditdf.MONTH, self.creditdf.DAY, 
                                            self.creditdf.TRANSACTION_TYPE, self.creditdf.TRANSACTION_VALUE)

        filtered_df.show()

        # Calculate the total amount spent
        total_transaction_value = filtered_df.agg({"TRANSACTION_VALUE": "sum"}).collect()[0][0]

        print("Payment Due: {}".format(total_transaction_value))

    def get_transactions_between(self, start_date, end_date, ssn):
        # Filter credit based on SSN and date range
        filtered_df = self.creditdf.filter((self.creditdf["CUST_SSN"] == ssn) &
                                    (self.creditdf["YEAR"].between(start_date[:4], end_date[:4])) &
                                    (self.creditdf["MONTH"].between(start_date[5:7], end_date[5:7])) &
                                    (self.creditdf["DAY"].between(start_date[8:], end_date[8:])))

        # Sort by year, month, and day in descending order
        sorted_df = filtered_df.orderBy(col("YEAR").desc(), col("MONTH").desc(), col("DAY").desc())

        sorted_df = sorted_df.select(self.creditdf.TRANSACTION_ID, self.creditdf.YEAR, self.creditdf.MONTH, self.creditdf.DAY,
                                    self.creditdf.CREDIT_CARD_NO, self.creditdf.CUST_SSN, self.creditdf.BRANCH_CODE,
                                    self.creditdf.TRANSACTION_TYPE, self.creditdf.TRANSACTION_VALUE)
        sorted_df.show()

    

    def get_loan_data(self, spark):
        url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
        response = requests.get(url)
        status_code = response.status_code

        # Check if status code is 200 (OK)
        if status_code == 200:
            # Retrieve the JSON data from the response
            data = response.json()
            
            # Convert JSON data to a DataFrame
            df = spark.read.json(spark.sparkContext.parallelize([data]))
            
            # Write the DataFrame to RDBMS table
            df.write.format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("dbtable", "CDW_SAPP_loan_application") \
                .option("user", "root") \
                .option("password", "password") \
                .mode("overwrite") \
                .save()

            return df
        else:
            print("Failed to retrieve data from the API.")