#!/usr/bin/env python3

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
def generate_chunk(chunk_id, chunk_size, seed, start_date, end_date, category_list, categories,
                   payment_methods, user_devices, user_cities, user_genders, fraud_probability):

    """
    Generates a chunk (subset) of the synthetic e-commerce data.
    Returns a Pandas DataFrame.
    """

    # Important: set random seeds inside each process for reproducibility
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
        global_index = chunk_id * chunk_size + i + 1  # unique transaction_id across chunks

        transaction_ids.append(global_index)

        # user_id (multiple transactions per user)
        user_ids.append(random.randint(1, chunk_size // 10))

        # pick category/subcategory
        cat = random.choice(category_list)
        subcat = random.choice(categories[cat])
        category_col.append(cat)
        subcategory_col.append(subcat)

        # generate product_id
        product_ids.append("PID_" + "".join(random.choices(string.digits, k=8)))

        # price
        price = round(random.uniform(1.0, 999.99), 2)
        prices.append(price)

        # quantity
        qty = random.randint(1, 5)
        quantities.append(qty)

        # total_amount
        total_amounts.append(round(price * qty, 2))

        # payment method
        payment_col.append(random.choice(payment_methods))

        # random timestamp
        random_second = random.randint(0, int(delta.total_seconds()))
        tx_time = start_date + timedelta(seconds=random_second)
        timestamps.append(tx_time.strftime("%Y-%m-%d %H:%M:%S"))

        # user device
        devices.append(random.choice(user_devices))

        # city
        cities.append(random.choice(user_cities))

        # user age
        ages.append(random.randint(18, 70))

        # user gender
        genders.append(random.choice(user_genders))

        # user income
        incomes.append(random.randint(20000, 200000))

        # fraud label
        if random.random() < fraud_probability:
            fraud_labels.append(1)
        else:
            fraud_labels.append(0)

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


def generate_ecommerce_data_mp(num_rows=1_000_000, seed=42, n_cores=None, fraud_probability=0.05):
    """
    Generate synthetic e-commerce data using multiprocessing.
    :param num_rows: Total number of rows/transactions.
    :param seed: Base random seed.
    :param n_cores: Number of processes to spawn. Defaults to cpu_count() if None.
    :param fraud_probability: Probability of transaction being fraud (0..1).
    :return: Pandas DataFrame containing all generated data.
    """

    # If n_cores not specified, use all available CPU cores
    if n_cores is None:
        n_cores = cpu_count()

    # Basic parameters for data generation
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

    # Determine chunk size for each process
    chunk_size = num_rows // n_cores
    remainder = num_rows % n_cores

    # Prepare list of (chunk_id, chunk_size) to feed each process
    tasks = []
    for i in range(n_cores):
        # Distribute remainder among first few chunks
        size = chunk_size + (1 if i < remainder else 0)

        tasks.append(
            (i, size, seed, start_date, end_date, category_list, categories,
             payment_methods, user_devices, user_cities, user_genders, fraud_probability)
        )

    # Use a Pool of processes to generate chunks in parallel
    with Pool(processes=n_cores) as pool:
        df_chunks = pool.starmap(generate_chunk, tasks)

    # Concatenate chunks
    df_final = pd.concat(df_chunks, ignore_index=True)
    return df_final


def main():
    import sys
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce CSV data in parallel.")
    parser.add_argument("--rows", type=int, default=1_000_000,
                        help="Number of rows to generate.")
    parser.add_argument("--output", type=str, default="ecommerce_data.csv",
                        help="Output CSV file name.")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility.")
    parser.add_argument("--cores", type=int, default=None,
                        help="Number of CPU cores to use (default: all).")
    parser.add_argument("--fraud_probability", type=float, default=0.05,
                        help="Probability of transaction being fraud (0.0 - 1.0).")

    args = parser.parse_args()

    print(f"Generating {args.rows} rows with multiprocessing...")
    print(f"Using {args.cores if args.cores else cpu_count()} core(s).")

    start_time = time.time()
    df = generate_ecommerce_data_mp(
        num_rows=args.rows,
        seed=args.seed,
        n_cores=args.cores,
        fraud_probability=args.fraud_probability
    )

    # Save to CSV
    df.to_csv(args.output, index=False)
    elapsed = time.time() - start_time

    print(f"Data generation complete. Saved to {args.output}")
    print(f"Time taken: {elapsed:.2f} seconds")


if __name__ == "__main__":
    # On Windows, multiprocessing requires this guard
    main()
