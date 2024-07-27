import json
import time
import random
from datetime import datetime, timedelta


# Function to generate random transaction data
def generate_random_transaction(transaction_id):
    customer_id = f"C{random.randint(1, 10):03}"
    items = [
        {
            "item_id": f"I{random.randint(1, 20):03}",
            "product_name": random.choice(["Laptop", "Mouse", "Keyboard", "Monitor", "Printer"]),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(20.00, 1500.00), 2)
        }
        for _ in range(random.randint(1, 5))
    ]
    transaction_time = (datetime.utcnow() - timedelta(minutes=random.randint(1, 120))).strftime("%Y-%m-%dT%H:%M:%SZ")
    delivery_time = (datetime.utcnow() + timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%dT%H:%M:%SZ")
    status = random.choice(["Delivered", "Shipped", "Pending"])
    address = {
        "street": f"{random.randint(100, 999)} {random.choice(['Main St', 'Elm St', 'Maple St', 'Pine St'])}",
        "city": random.choice(["Metropolis", "Gotham", "Star City", "Central City"]),
        "state": random.choice(["NY", "NJ", "CA", "TX"]),
        "zip": f"{random.randint(10000, 99999)}"
    }

    transaction = {
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "items": items,
        "transaction_time": transaction_time,
        "delivery_info": {
            "status": status,
            "delivery_time": delivery_time,
            "address": address
        }
    }
    return transaction


def main():
    transaction_id_counter = 1
    file_counter = 1
    while True:
        transactions = [generate_random_transaction(f"T{transaction_id_counter + i:03}") for i in range(3)]
        transaction_id_counter += 3

        with open(f"C:\\Users\\paras\\OneDrive\\Desktop\\Startup\\Repositories\\scalaRepos\\complete_big_data_project\\datamigration_project\\src\\main\\scala\\com\\its\\youtube\\chapter1\\datasets\\transactions_{file_counter}.json", "w") as file:
            json.dump(transactions, file, indent=2)

        print(f"Generated transactions_{file_counter}.json")
        file_counter += 1
        time.sleep(2)


if __name__ == "__main__":
    main()
