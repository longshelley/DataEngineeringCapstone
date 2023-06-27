import os
from time import sleep
from subprocess import call

import input_checker
from cc_handler import CCHandler

def clear():
    # check and make call for specific operating system
    _ = call('clear' if os.name == 'posix' else 'cls')

def main_menu(spark):
    
    while True:
        clear()
        print("Select an option to continue:")
        print("1. Querying for Data")
        print("2. Visualizations")
        print("3. Exit")
        choice = input("--> ")
        if choice == "1":
            option_one(spark)
        elif choice == "2":
            option_two(spark)
        elif choice == "3":
            print("Exiting...")
            sleep(3)
            exit()
        else:
            print("Invalid choice. Please try again.")

def option_one(spark):
    while True:
        clear()
        print("Querying for Data:")
        print("1. Transactions made by customers in given zip code for given month and year")
        print("2. The total number and values of transactions for given type")
        print("3. The total number and values of transactions for branches in a given state")
        print("4. Check existing account details of a customer")
        print("5. Monthly bill for a credit card for given month and year")
        print("6. Go Back")
        choice = input("--> ")
        if choice == "1":
            choice_1_1(spark)
        elif choice == "2":
            choice_1_2(spark)
        elif choice == "3":
            choice_1_3(spark)
        elif choice == "4":
            choice_1_4(spark)
        elif choice == "5":
            choice_1_5(spark)
        elif choice == "6":
            main_menu()
        else:
            print("Invalid choice. Please try again.")

def option_two():
    while True:
        clear()
        print("Visualizations:")
        print("1. Which transaction type has a high rate of transactions")
        print("2. Which state has a high number of customers")
        print("3. The sum of all transactions for the top 10 customers")
        print("4. The percentage of applications approved for self-employed applicants")
        print("5. The percentage of rejection for married male applicants")
        print("6. The top three months with the largest transaction data")
        print("7. Which branch processed the highest total dollar value of healthcare transactions")
        print("8. Go Back")
        choice = input("--> ")
        if choice == "1":
            choice_2_1()
        elif choice == "2":
            choice_2_2()
        elif choice == "3":
            choice_2_3()
        elif choice == "4":
            choice_2_4()
        elif choice == "5":
            choice_2_5()
        elif choice == "6":
            choice_2_6()
        elif choice == "7":
            choice_2_7()
        elif choice == "8":
            main_menu()
        else:
            print("Invalid choice. Please try again.")




def choice_1_1(spark):
    clear()
    print("Transactions made by customers in given zip code for given month and year")
    input_zip_code = input("Enter zip code: ")  #23223
    input_month = int(input("Enter month: "))   #2
    input_year = int(input("Enter year: "))     #2018
    CCHandler.get_transactions(input_zip_code, input_month, input_year, spark)
    
def choice_1_2(spark):
    clear()
    print("The total number and values of transactions for given type")
    transaction_type = input("Enter transaction type: ").title()    #grocery
    CCHandler.get_count_value(transaction_type, spark)

def choice_1_3(spark):
    clear()
    print("The total number and values of transactions for branches in a given state")
    state = input("Enter state abbreviation: ").upper() #ny
    CCHandler.get_value_total( state, spark)

def choice_1_4(spark):
    clear()
    print("Check existing account details of a customer")
    ssn = int(input("Enter SSN: ")) #123459988
    CCHandler.get_customer(ssn, spark)

    print("1. Modify the customer details")
    print("2. Show transactions made between two dates")
    choice = input("--> ")
    if choice == "1":
        select_1_4_1(ssn, spark)
    elif choice == "2":
        select_1_4_2(ssn, spark)

def choice_1_5():
    clear()
    print("Monthly bill for a credit card for given month and year")
    credit_card_number = int(input("Enter credit card number: ")) #4210653310061055
    input_month = int(input("Enter month: "))   #6
    input_year = int(input("Enter year: "))     #2018
    CCHandler.get_monthly_bill(credit_card_number, input_month, input_year)

def select_1_4_1(ssn):
    clear()
    print("Modify the customer details")
    print("Press ENTER to skip")
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
    print("Customer details updated and saved.")

def select_1_4_2(ssn):
    clear()
    print("Show transactions made between two dates")
    start_date = (input("Enter start date (YYYY-MM-DD): ")) #2018-04-01
    end_date = (input("Enter end date (YYYY-MM-DD): "))     #2018-08-04
    # Check to make sure end_date comes after start_date and follow format
    CCHandler.get_transactions_between(start_date, end_date, ssn)

def choice_2_1():
    clear()
    print("Which transaction type has a high rate of transactions")

def choice_2_2():
    clear()
    print("Which state has a high number of customers")

def choice_2_3():
    clear()
    print("The sum of all transactions for the top 10 customers")

def choice_2_4():
    clear()
    print("The percentage of applications approved for self-employed applicants")
    
def choice_2_5():
    clear()
    print("The percentage of rejection for married male applicants")

def choice_2_6():
    clear()
    print("The top three months with the largest transaction data")
    
def choice_2_7():
    clear()
    print("Which branch processed the highest total dollar value of healthcare transactions")