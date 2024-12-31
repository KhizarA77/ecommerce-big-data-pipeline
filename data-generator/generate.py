import argparse
import random
import string
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# ----------------------------
# Single-chunk generation
# ----------------------------
def generate_chunk_with_trends(chunk_id, chunk_size, seed, start_date, end_date, category_list, categories,
                               payment_methods, user_devices, user_cities, user_genders, base_fraud_probability):
    """
    Generates a chunk (subset) of the synthetic e-commerce data with fraud trends.
    Returns a Pandas DataFrame.
    """
    random.seed(seed + chunk_id)
    np.random.seed(seed + chunk_id)

    transaction_ids = []
    user_ids = []
    product_ids = []
    category_col = []
    subcategory_col = []
    prices = []
    quantities = []
    total_amounts = []
    payment_col = []
    timestamps = []
    devices = []
    cities = []
    ages = []
    genders = []
    incomes = []
    fraud_labels = []

    delta = end_date - start_date
    for i in range(chunk_size):
        global_index = chunk_id * chunk_size + i + 1

        transaction_ids.append(global_index)
        user_ids.append(random.randint(1, chunk_size // 10))

        # Category and subcategory
        cat = random.choice(category_list)
        subcat = random.choice(categories[cat])
        category_col.append(cat)
        subcategory_col.append(subcat)

        # Generate product_id
        product_ids.append("PID_" + "".join(random.choices(string.digits, k=8)))

        # Price and quantity
        price = round(random.uniform(1.0, 999.99), 2)
        prices.append(price)
        qty = random.randint(1, 5)
        quantities.append(qty)
        total_amounts.append(round(price * qty, 2))

        # Payment method
        payment_method = random.choice(payment_methods)
        payment_col.append(payment_method)

        # Timestamp
        random_second = random.randint(0, int(delta.total_seconds()))
        tx_time = start_date + timedelta(seconds=random_second)
        timestamps.append(tx_time.strftime("%Y-%m-%d %H:%M:%S"))

        # User details
        devices.append(random.choice(user_devices))
        cities.append(random.choice(user_cities))
        age = random.randint(18, 70)
        ages.append(age)
        gender = random.choice(user_genders)
        genders.append(gender)
        income = random.randint(20000, 200000)
        incomes.append(income)

        # Fraud trends
        fraud_probability = base_fraud_probability
        # Increase fraud risk for high prices
        if price > 500:
            fraud_probability += 0.1
        # Increase fraud risk for certain payment methods
        if payment_method in ["CreditCard", "PayPal"]:
            fraud_probability += 0.05
        # Increase fraud risk for younger users
        if age < 25:
            fraud_probability += 0.05
        # Decrease fraud risk for high-income users
        if income > 150000:
            fraud_probability -= 0.05
        # Increase fraud risk for specific categories
        if cat in ["Electronics", "Automotive"]:
            fraud_probability += 0.05

        # Randomly assign fraud label based on adjusted probability
        fraud_labels.append(1 if random.random() < fraud_probability else 0)

    data = {
        "transaction_id": transaction_ids,
        "user_id": user_ids,
        "product_id": product_ids,
        "category": category_col,
        "sub_category": subcategory_col,
        "price": prices,
        "quantity": quantities,
        "total_amount": total_amounts,
        "payment_method": payment_col,
        "timestamp": timestamps,
        "user_device": devices,
        "user_city": cities,
        "user_age": ages,
        "user_gender": genders,
        "user_income": incomes,
        "fraud_label": fraud_labels,
    }

    return pd.DataFrame(data)

def generate_ecommerce_data_with_trends(num_rows=1_000_000, seed=42, n_cores=None, base_fraud_probability=0.05):
    """
    Generate synthetic e-commerce data with fraud trends using multiprocessing.
    """
    if n_cores is None:
        n_cores = cpu_count()

    categories = {
        "Electronics": ["Smartphones", "Laptops", "Headphones", "Wearables"],
        "Clothing": ["Men_Shirts", "Women_Dresses", "Shoes", "Accessories"],
        "Home": ["Furniture", "Kitchen", "Decor", "Garden"],
        "Beauty": ["Skincare", "Makeup", "Fragrance", "Haircare"],
        "Sports": ["Gym_Equipment", "Outdoor", "Sportswear", "Footwear"],
        "Books": ["Fiction", "NonFiction", "Comics", "Textbooks"],
        "Automotive": ["Car_Accessories", "Motorcycle_Parts", "Car_Electronics"],
    }
    category_list = list(categories.keys())

    payment_methods = ["CreditCard", "DebitCard", "PayPal", "CashOnDelivery"]
    user_devices = ["Desktop", "Mobile", "Tablet"]
    user_genders = ["M", "F", "Other"]
    user_cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "San Antonio", "San Diego", "Dallas", "San Jose", "Austin"
    ]

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    chunk_size = num_rows // n_cores
    remainder = num_rows % n_cores

    tasks = []
    for i in range(n_cores):
        size = chunk_size + (1 if i < remainder else 0)
        tasks.append(
            (i, size, seed, start_date, end_date, category_list, categories,
             payment_methods, user_devices, user_cities, user_genders, base_fraud_probability)
        )

    with Pool(processes=n_cores) as pool:
        df_chunks = pool.starmap(generate_chunk_with_trends, tasks)

    return pd.concat(df_chunks, ignore_index=True)


def main():
    import sys
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce CSV data in parallel.")
    parser.add_argument("--rows", type=int, default=10_000_000,
                        help="Number of rows to generate.")
    parser.add_argument("--output", type=str, default="ecommerce_data.csv",
                        help="Output CSV file name.")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility.")
    parser.add_argument("--cores", type=int, default=12,
                        help="Number of CPU cores to use (default: all).")
    parser.add_argument("--fraud_probability", type=float, default=0.05,
                        help="Probability of transaction being fraud (0.0 - 1.0).")

    args = parser.parse_args()

    print(f"Generating {args.rows} rows with multiprocessing...")
    print(f"Using {args.cores if args.cores else cpu_count()} core(s).")

    start_time = time.time()
    df = generate_ecommerce_data_with_trends(
        num_rows=args.rows,
        seed=args.seed,
        n_cores=args.cores,
        base_fraud_probability=args.fraud_probability
    )

    # Save to CSV
    df.to_csv(args.output, index=False)
    elapsed = time.time() - start_time

    print(f"Data generation complete. Saved to {args.output}")
    print(f"Time taken: {elapsed:.2f} seconds")


if __name__ == "__main__":
    # On Windows, multiprocessing requires this guard
    main()