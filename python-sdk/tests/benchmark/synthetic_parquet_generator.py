"""
Function to generate PARQUET files of various sizes
"""
import pandas as pd
from faker import Faker

fake = Faker()


def generate_data(file_name, records):
    # Declare an empty dictionary
    customer = {}
    # Iterate the loop based on the input value and generate fake data
    for n in range(0, records):
        customer[str(n)] = {}
        customer[str(n)]["id"] = str(n)
        customer[str(n)]["name"] = fake.name()
        customer[str(n)]["address"] = fake.address()
        customer[str(n)]["email"] = str(fake.email())
        customer[str(n)]["phone"] = str(fake.phone_number())

    df = pd.DataFrame(data=customer)
    df.to_parquet(file_name)

    print(f"File {file_name} has been created.")


# create ten_kb parquet file
ten_kb = "/fake_dataset/parquet/ten_kb.parquet"
ten_kb_rows = 13
# generate_data(ten_kb,ten_kb_rows)

# create hundred_kb parquet file
hundred_kb = "/fake_dataset/parquet/hundred_kb.parquet"
hundred_kb_rows = 140
# generate_data(hundred_kb,hundred_kb_rows)


# create hundred_mb json file
hundred_mb = "/fake_dataset/parquet/hundred_mb.parquet"
hundred_mb_rows = 15295
generate_data(hundred_mb, hundred_mb_rows)

# create ten_mb json file
ten_mb = "/fake_dataset/parquet/ten_mb.parquet"
ten_mb_rows = 14061
generate_data(ten_mb, ten_mb_rows)

# create one_mb json file
one_mb = "/fake_dataset/parquet/one_mb.parquet"
one_mb_rows = 1506
# generate_data(one_mb,one_mb_rows)


# create one_gb json file
one_gb = "/fake_dataset/parquet/one_gb.parquet"
one_gb_rows = 1616194
generate_data(one_gb, one_gb_rows)

# create two_gb json file
two_gb = "/fake_dataset/parquet/two_gb.parquet"
two_gb_rows = 3312389
generate_data(two_gb, two_gb_rows)
