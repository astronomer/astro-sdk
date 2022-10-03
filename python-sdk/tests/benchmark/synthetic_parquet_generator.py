"""
Function to generate PARQUET files of various sizes
"""
import pandas as pd
from faker import Faker

fake = Faker()


def generate_data(file_name, start, end):
    # Declare an empty dictionary
    customer = []
    # Iterate the loop based on the input value and generate fake data
    for n in range(start, end):
        cust = {}
        cust["id"] = str(n)
        cust["name"] = fake.name()
        cust["address"] = fake.address()
        cust["email"] = str(fake.email())
        cust["phone"] = str(fake.phone_number())
        customer.append(cust)

    df = pd.DataFrame(data=customer)
    print(df)

    df.to_parquet(file_name, index=None)

    print(f"File {file_name} has been created.")


five_gb = "/fake_dataset/parquet/five_gb.parquet"
five_gb_rows = 8849701
generate_data(five_gb, 0, five_gb_rows)
#

# create one_gb parquet file
one_gb = "/fake_dataset/parquet/ten_gb/sixth.parquet"
one_gb_rows = 9697180
generate_data(one_gb, 8080983, one_gb_rows)

# create ten gb parquet files
one_gb = "/fake_dataset/parquet/ten_gb/seventh.parquet"
one_gb_rows = 11313377
generate_data(one_gb, 9697182, one_gb_rows)

two_gb = "/fake_dataset/parquet/ten_gb/eighth.parquet"
two_gb_rows = 12929574
generate_data(two_gb, 11313379, two_gb_rows)

two_gb = "/fake_dataset/parquet/ten_gb/ninth.parquet"
two_gb_rows = 14545771
generate_data(two_gb, 12929576, two_gb_rows)

two_gb = "/fake_dataset/parquet/ten_gb/tenth.parquet"
two_gb_rows = 16161968
generate_data(two_gb, 14545773, two_gb_rows)
