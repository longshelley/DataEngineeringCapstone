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
        
        url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
        response = requests.get(url)
        status_code = response.status_code

        # Check if status code is 200 (OK)
        if status_code == 200:
            # Retrieve the JSON data from the response
            data = response.json()
            
            # Create SparkSession
            spark = SparkSession.builder.appName("Loan Application").getOrCreate()
            
            # Convert JSON data to a DataFrame
            self.loandf = spark.read.json(spark.sparkContext.parallelize([data]))
            
            # Write the DataFrame to RDBMS table
            self.loandf.write.format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("dbtable", "CDW_SAPP_loan_application") \
                .option("user", "root") \
                .option("password", "password") \
                .mode("overwrite") \
                .save()
        else:
            print("Failed to retrieve loan data from the API.")
        
    def get_transactions(self, spark):
        self.creditdf.createOrReplaceTempView("transactions")
        self.customerdf.createOrReplaceTempView("customers")

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
        self.creditdf.createOrReplaceTempView("transactions")

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
        self.creditdf.createOrReplaceTempView("transactions")

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
        self.customerdf.createOrReplaceTempView("customers")
        
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

    def transaction_type_count(self):
        # Group by transaction type and count the number of transactions
        transaction_count = self.creditdf.groupBy("TRANSACTION_TYPE").count().alias("transaction_count")

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

    def state_customer_count(self):
        # Group by state and count the number of customers
        customer_count = self.customerdf.groupBy("CUST_STATE").agg({"SSN": "count"}).alias("customer_count")

        # Sort the states by count in descending order
        sorted_states = customer_count.orderBy("count(SSN)", ascending=False).collect()

        # Get states and customer counts
        states = [row["CUST_STATE"] for row in sorted_states]
        customer_count = [row["count(SSN)"] for row in sorted_states]

        # Plot states and customer counts
        bars = plt.bar(states, customer_count)
        plt.xlabel("State")
        plt.ylabel("Customer Count")
        plt.title("State vs Customer Count")
        plt.xticks(rotation=70)

        # Labels and colors for readability
        bars[0].set_color("#dd6e42")
        for bar in bars[1:]:
            bar.set_color("#4f6d7a")

        plt.text(0, customer_count[0], str(customer_count[0]), ha="center", va="bottom")

        plt.show()

    def top_10_customer(self):
        # Group by customer SSN and sum the transaction amounts
        customer_transactions = self.creditdf.groupBy("CUST_SSN").agg({"TRANSACTION_VALUE": "sum"}).alias("transaction_amount")

        # Sort the transactions by sum in descending order
        top_10_customers = customer_transactions.orderBy("sum(TRANSACTION_VALUE)", ascending=False).limit(10).collect()

        # Get top 10 customers and transaction sums
        customer_ssn = [str(row["CUST_SSN"]) for row in top_10_customers]
        transaction_amount = [row["sum(TRANSACTION_VALUE)"] for row in top_10_customers]

        # Plot customer SSNs and transaction sums
        bars = plt.bar(customer_ssn, transaction_amount)
        plt.xlabel("Customer")
        plt.ylabel("Total Transaction Amount")
        plt.title("Top 10 Customers by Total Transaction Amount")
        plt.xticks(rotation=70)

        # Labels and colors for readability
        bars[0].set_color("#dd6e42")
        for bar in bars[1:]:
            bar.set_color("#4f6d7a")

        plt.show()

    def approved_selfemploy(self):
        # Filter and count self-employed applicants
        self_employed_approved = self.loandf.filter(col("Self_Employed") == "Yes").filter(col("Application_Status") == "Y").count()
        self_employed_total = self.loandf.filter(col("Self_Employed") == "Yes").count()

        # Calculate percentage of approved applications for self-employed applicants
        percentage_approved = (self_employed_approved / self_employed_total) * 100

        # Plot percentage
        labels = ['Approved', 'Rejected']
        sizes = [percentage_approved, 100 - percentage_approved]
        colors = ['#dd6e42', '#4f6d7a']
        explode = (0.01, 0)

        plt.pie(sizes, labels=labels, colors=colors, explode=explode, autopct='%1.1f%%', startangle=90)
        plt.axis('equal') # plot circle graph
        plt.title('Percentage of Approved Applications for Self-Employed Applicants')
        plt.show()

    def rejected_married_male(self):
        # Filter and count rejected married male
        rejected_married_male = self.loandf.filter(col("Gender") == "Male").filter(col("Married") == "Yes").filter(
            col("Application_Status") == "N").count()
        total_married_male = self.loandf.filter(col("Gender") == "Male").filter(col("Married") == "Yes").count()

        # Calculate percentage of rejection for married male
        percentage_rejected = (rejected_married_male / total_married_male) * 100

        # Plotting percentage
        labels = ['Rejected', 'Approved']
        sizes = [percentage_rejected, 100 - percentage_rejected]
        colors = ['#dd6e42', '#4f6d7a']
        explode = (0.01, 0)

        fig, ax = plt.subplots()
        plt.pie(sizes, labels=labels, colors=colors, explode=explode, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title('Percentage of Rejected Applications for Married Male Applicants')

        plt.show()

    def top_3_months(self):
        # Group by month and calculate total amount of transaction value
        monthly_total = self.creditdf.groupBy("MONTH").sum("TRANSACTION_VALUE")

        # Sort result in desc order and select the top 3 months
        top_3_months = monthly_total.orderBy(col("sum(TRANSACTION_VALUE)").desc()).limit(3)

        # Get month names and corresponding total amount of transaction value
        months = [calendar.month_name[row["MONTH"]] for row in top_3_months.collect()]
        values = [round(row["sum(TRANSACTION_VALUE)"], 2) for row in top_3_months.collect()]

        # Plotting graph
        bars = plt.bar(months, values, width=0.5)
        plt.xlabel("Month")
        plt.ylabel("Total Amount of Transaction Value")
        plt.title("Top Three Months with the Largest Transaction Data")

        for bar in bars:
            bar.set_color("#dd6e42")

        for i in range(len(months)):
            plt.text(i, values[i], str(values[i]), ha="center", va="bottom")

        plt.show()

    def branch_highest_value(self):
        # Filter for only healthcare transactions
        healthcare_trans = self.creditdf.filter(col("TRANSACTION_TYPE") == "Healthcare")

        # Group by branch code and calculate the total value of healthcare transactions (showing top 10 in comparison)
        branch_total = healthcare_trans.groupBy("BRANCH_CODE").sum("TRANSACTION_VALUE").orderBy(
            col("sum(TRANSACTION_VALUE)").desc()).limit(10).collect()

        # Extract the branch code and total dollar value
        branch_code = [str(row["BRANCH_CODE"]) for row in branch_total]
        total_value = [round(row["sum(TRANSACTION_VALUE)"], 2) for row in branch_total]

        # Plotting graph
        bars = plt.bar(branch_code, total_value)
        plt.xlabel("Branch Code")
        plt.ylabel("Total Transaction Dollar Value")
        plt.title("Branch with the Highest Total Dollar Value of Healthcare Transactions\n(in Comparison to Top 10 HealthCare Transactions)")

        # Labels and colors for readability
        bars[0].set_color("#dd6e42")
        for bar in bars[1:]:
            bar.set_color("#4f6d7a")

        plt.text(0, total_value[0], str(total_value[0]), ha="center", va="bottom")

        plt.show()
    