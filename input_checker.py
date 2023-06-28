import re

def zip_code():
    while True:
        pattern = r'^\d{5}$'
        input_zip_code = input("Enter zip code: ")
        if re.match(pattern, input_zip_code):
            return input_zip_code
        else:
            print("Invalid input. Must be 5 intergers.")

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
        input_month = input("Enter month: ").lower()
        if input_month in months:
            return months.get(input_month)
        elif 1 <= int(input_month) <= 12:
            return int(input_month)
        else:
            print("Invalid input. Must be a month either in word or numeral form.")

def years():
    while True:
        pattern = r'^\d{4}$'
        input_year = input("Enter year: ")
        if re.match(pattern, input_year):
            return input_year
        else:
            print("Invalid input. Must be 4 intergers.")

