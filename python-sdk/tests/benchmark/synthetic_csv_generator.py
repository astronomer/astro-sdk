"""
Function to generate csv files of various sizes
"""
import csv
import random

from faker import Faker


def generate_data(row_size, file_name):
    row_size = 11000000

    faked_obj: object = Faker("en_GB")

    file_obj = open(file_name, "w")
    write_obj = csv.writer(file_obj)
    write_obj.writerow(("id", "name", "address", "college", "company", "dob", "age"))
    for i in range(row_size):
        write_obj.writerow(
            (
                i + 1,
                faked_obj.name(),
                faked_obj.address(),
                random.choice(["psg", "sona", "amirta", "anna university"]),
                random.choice(["CTS", "INFY", "HTC"]),
                (
                    random.randrange(1950, 1995, 1),
                    random.randrange(1, 13, 1),
                    random.randrange(1, 32, 1),
                ),
                random.choice(range(0, 100)),
            )
        )
    file_obj.close()


generate_data(11000000, "one_gb.csv")
