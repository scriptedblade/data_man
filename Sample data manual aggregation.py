#code is designed to simulate a large dataset with 100 million rows and perform manual aggregation of sales data.


import numpy as np
import pandas as pd
import polars as pl

# Step 1: Generate a large dataset (10 million rows)
num_rows = 100_000_000
categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Toys", "Groceries"]
products = ["Laptop", "Shirt", "Microwave", "Novel", "Toy Car", "Cereal"]

data = {
    "Date": pd.date_range(start="2022-01-01", periods=num_rows // 100, freq="min").repeat(100),
    "Category": np.random.choice(categories, num_rows),
    "Product": np.random.choice(products, num_rows),
    "Sales": np.random.randint(100, 1000, num_rows),
    "Quantity": np.random.randint(1, 20, num_rows),
}

# Convert to Polars DataFrame
df = pl.DataFrame(data)

# Step 2: Manual aggregation for total sales per category
unique_categories = df["Category"].unique()

manual_aggregates = []
for category in unique_categories:
    total_sales = df.filter(pl.col("Category") == category)["Sales"].sum()
    manual_aggregates.append({"Category": category, "Total Sales": total_sales})

# Convert the result into a Polars DataFrame
result_df = pl.DataFrame(manual_aggregates)

# Step 3: Add a Total Revenue column (Sales * Quantity)
df = df.with_columns(
    (pl.col("Sales") * pl.col("Quantity")).alias("Total Revenue")
)

# Display the results
print("\nManual Aggregation Results (Total Sales per Category):")
print(result_df)

print("\nSample of the dataset with Total Revenue:")
print(df.head(10))
