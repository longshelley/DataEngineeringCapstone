import sys
from time import sleep


print("test this line - must stay\n")
while True:
    transaction_type = input("Enter transaction type: ").title()


    if transaction_type in ["Grocery", "Education", "Entertainment", "Bills", "Gas", "Test", "Healthcare"]:
        break

    # Clear previous line for invalid input message
    sleep(1)
    sys.stdout.write("\033[F\033[K")

    print("Invalid input. Possible transactions:")

    # Clear previous line for list of possible transactions
    sleep(1)
    sys.stdout.write("\033[F\033[K")

    print("Grocery, Education, Entertainment, Bills, Gas, Test, Healthcare")

    # Clear previous line for list of possible transactions
    sleep(1)
    sys.stdout.write("\033[F\033[K")