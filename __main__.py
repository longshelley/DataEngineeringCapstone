from pyspark.sql import SparkSession

import input_checker
from cc_handler import CCHandler
from data_vis import DataVisualization


if __name__ == "__main__":

    spark = SparkSession.builder.appName('capstonedemo').getOrCreate()
    branchdf = spark.read.json("cdw_sapp_branch.json")
    creditdf = spark.read.json("cdw_sapp_credit.json")
    customerdf = spark.read.json("cdw_sapp_customer.json")

    CCHandler(branchdf, creditdf, customerdf)

    
    transaction_type = input("Enter transaction type: ").title()    #grocery
    CCHandler.get_count_value(transaction_type, spark)

    state = input("Enter state abbreviation: ").upper() #ny
    CCHandler.get_value_total( state, spark)

    ssn = int(input("Enter SSN: ")) #123459988
    CCHandler.get_customer(ssn, spark)

    print("1) Modify the customer details" +
          "2) Show transactions made between two dates")
    
    choice = input("Modify the customer details (Y/N): ")
    # input code to give option to modify customer details or not
    # if yes
    if choice == "1":
        # will ask for inputs for new details but currently testing with hardcode
        updated_first_name = input("First name: ")
        updated_middle_name = input("Middle name: ")
        updated_last_name = input("Last name: ")
        updated_apt_no = input("Apt no: ")
        updated_street_name = input("Street name: ")
        updated_cust_city = input("City name: ")
        updated_cust_state = input("State: ")
        updated_cust_zip = int(input("Zip code: "))
        updated_cust_phone = input("Phone number: ")
        updated_cust_email = input("Email address: ")
        CCHandler.set_customer(ssn, updated_first_name, updated_middle_name, 
                     updated_last_name, updated_apt_no, 
                     updated_street_name, updated_cust_city,
                     updated_cust_state, updated_cust_zip, 
                     updated_cust_phone, updated_cust_email)

        customerdf = spark.read.json("cdw_sapp_customer.json")
        CCHandler(branchdf, creditdf, customerdf)

        print("Customer details updated and saved.")
    elif choice == "2":
        start_date = (input("Enter start date (YYYY-MM-DD): ")) #2018-04-01
        end_date = (input("Enter end date (YYYY-MM-DD): "))     #2018-08-04
        # Check to make sure end_date comes after start_date and follow format
        CCHandler.get_transactions_between(start_date, end_date, ssn)

    credit_card_number = int(input("Enter credit card number: ")) #4210653310061055
    input_month = int(input("Enter month: "))   #6
    input_year = int(input("Enter year: "))     #2018
    CCHandler.get_monthly_bill(credit_card_number, input_month, input_year)

    start_date = (input("Enter start date (YYYY-MM-DD): ")) #2018-04-01
    end_date = (input("Enter end date (YYYY-MM-DD): "))     #2018-08-04
    # Check to make sure end_date comes after start_date and follow format
    CCHandler
