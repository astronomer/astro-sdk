import random
import string

num_columns = 5

generate_row_value = lambda a: "".join(
    random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
)

with open("test_data_m.csv", "w") as file:
    file.write(",".join([f"column_{i}" for i in range(num_columns)]) + "\n")
    for i in range(500000):
        column_values = ",".join([generate_row_value(x) for x in range(num_columns)])
        file.write(column_values + "\n")
