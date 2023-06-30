import re
import sys
from time import sleep
from dateutil.parser import parse

def zip_code():
    while True:
        pattern = r"^\d{5}$"
        input_zip_code = input("Zip Code: ")
        if input_zip_code == "":
            return input_zip_code
        if re.match(pattern, input_zip_code):
            return input_zip_code
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Must be 5 intergers.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def months():
    while True:
        months = {
            "january": 1, "jan": 1,
            "february": 2, "feb": 2,
            "march": 3, "mar": 3,
            "april": 4, "apr": 4,
            "may": 5,
            "june": 6, "jun": 6,
            "july": 7, "jul": 7,
            "august": 8, "aug": 8,
            "september": 9, "sep": 9,
            "october": 10, "oct": 10,
            "november": 11, "nov": 11,
            "december": 12, "dec": 12
        }
        input_month = input("Month: ").lower()
        if input_month in months:
            return months.get(input_month)
        try:
            if 1 <= int(input_month) <= 12:
                return int(input_month)
            else:
                sleep(1)
                sys.stdout.write("\033[F\033[K")
                print("Invalid input. Must be a month either in word or numeral form.")
                sleep(2)
                sys.stdout.write("\033[F\033[K")
        except ValueError:
            sleep(1)
            sys.stdout.write("\033[F\033[K")
            print("Invalid input. Must be a month either in word or numeral form.")
            sleep(2)
            sys.stdout.write("\033[F\033[K")

def years():
    while True:
        pattern = r"^(?:19|20)\d{2}$"
        input_year = input("Year: ")
        if re.match(pattern, input_year):
            return int(input_year)
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Must be a year.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def types():
    while True:
        trans_types = ["Grocery", "Education", "Entertainment", "Bills", "Gas", "Test", "Healthcare"]
        transaction_type = input("Transaction Type: ").title()
        if transaction_type in trans_types:
            return transaction_type
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Possible transactions:")
        sleep(2)
        sys.stdout.write("\033[F\033[K")
        print("Grocery, Education, Entertainment, Bills, Gas, Test, Healthcare")
        sleep(3)
        sys.stdout.write("\033[F\033[K")

def states():
    while True:
        us_states = {
            "ALABAMA": "AL",
            "ALASKA": "AK",
            "ARIZONA": "AZ",
            "ARKANSAS": "AR",
            "CALIFORNIA": "CA",
            "COLORADO": "CO",
            "CONNECTICUT": "CT",
            "DELAWARE": "DE",
            "FLORIDA": "FL",
            "GEORGIA": "GA",
            "HAWAII": "HI",
            "IDAHO": "ID",
            "ILLINOIS": "IL",
            "INDIANA": "IN",
            "IOWA": "IA",
            "KANSAS": "KS",
            "KENTUCKY": "KY",
            "LOUISIANA": "LA",
            "MAINE": "ME",
            "MARYLAND": "MD",
            "MASSACHUSETTS": "MA",
            "MICHIGAN": "MI",
            "MINNESOTA": "MN",
            "MISSISSIPPI": "MS",
            "MISSOURI": "MO",
            "MONTANA": "MT",
            "NEBRASKA": "NE",
            "NEVADA": "NV",
            "NEW HAMPSHIRE": "NH",
            "NEW JERSEY": "NJ",
            "NEW MEXICO": "NM",
            "NEW YORK": "NY",
            "NORTH CAROLINA": "NC",
            "NORTH DAKOTA": "ND",
            "OHIO": "OH",
            "OKLAHOMA": "OK",
            "OREGON": "OR",
            "PENNSYLVANIA": "PA",
            "RHODE ISLAND": "RI",
            "SOUTH CAROLINA": "SC",
            "SOUTH DAKOTA": "SD",
            "TENNESSEE": "TN",
            "TEXAS": "TX",
            "UTAH": "UT",
            "VERMONT": "VT",
            "VIRGINIA": "VA",
            "WASHINGTON": "WA",
            "WEST VIRGINIA": "WV",
            "WISCONSIN": "WI",
            "WYOMING": "WY"
        }
        state = input("State: ").upper()
        if state == "":
            return state
        if state in us_states:
            return us_states.get(state)
        elif state in us_states.values():
            return state
        else:
            sleep(1)
            sys.stdout.write("\033[F\033[K")
            print("Invalid input. Must be one of the 50 states in the US.")
            sleep(2)
            sys.stdout.write("\033[F\033[K")

def ss_number():
    while True:
        pattern = r"^(?!000|666)\d{9}$"
        ssn = input("SSN: ")
        if re.match(pattern, ssn):
            return int(ssn)
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Must be 9 intergers. Note: SSN does not start with 000 or 666.")
        sleep(3)
        sys.stdout.write("\033[F\033[K")

def cc_number():
    while True:
        pattern = r"\d{16}$"
        cc_num = input("Credit Card Number: ")
        if re.match(pattern, cc_num):
            return cc_num
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Must be 16 intergers.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def any_name(str):
    while True:
        pattern = r"^[A-Za-z]+$"
        name = input("{} Name: ".format(str))
        if name == "":
            return name
        if re.match(pattern, name):
            return name.title()
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Must be letters only.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def apt_no():
    while True:
        pattern = r"^\d{1,4}$"
        updated_apt_no = input("Apt no: ")
        if updated_apt_no == "":
            return updated_apt_no
        if re.match(pattern, updated_apt_no):
            return updated_apt_no
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Must be 1 to 4 digits only.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def st_name():
    while True:
        pattern = r"^[A-Za-z0-9\s]+$"
        updated_street_name = input("Street name: ")
        if updated_street_name == "":
            return updated_street_name
        elif re.match(pattern, updated_street_name):
            return updated_street_name
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. No symbols.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def city_name():
    while True:
        pattern = r"^[A-Za-z\s]+$"
        updated_city_name = input("City name: ")
        if updated_city_name == "":
            return updated_city_name
        if re.match(pattern, updated_city_name):
            return updated_city_name
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. No symbols or numbers.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def phone_no():
    while True:
        pattern = r"^\d{7}$"    # if adjusted, will have to adjust handler
        updated_cust_phone = input("Phone Number: ")
        if updated_cust_phone == "":
            return updated_cust_phone
        if re.match(pattern, updated_cust_phone):
            return int(updated_cust_phone)
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. 7 digits only.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def email():
    while True:
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        updated_cust_email = input("Email address: ")
        if updated_cust_email == "":
            return updated_cust_email
        if re.match(pattern, updated_cust_email):
            return updated_cust_email
        sleep(1)
        sys.stdout.write("\033[F\033[K")
        print("Invalid input. Email format only.")
        sleep(2)
        sys.stdout.write("\033[F\033[K")

def any_date(str):
    while True:
        date_input = input("Enter {} date: ".format(str))

        try:
            date = parse(date_input)
        except ValueError:
            sleep(1)
            sys.stdout.write("\033[F\033[K")
            print("Invalid date format. Please enter valid dates.")
            sleep(2)
            sys.stdout.write("\033[F\033[K")
            continue

        # Valid date input
        return date.strftime("%Y-%m-%d")

        