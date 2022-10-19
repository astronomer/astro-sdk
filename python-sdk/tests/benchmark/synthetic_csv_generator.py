"""
Function to generate csv files of various sizes
"""
import csv
import random

from faker import Faker


def generate_data(row_size, file_name):

    faked_obj: object = Faker("en_GB")

    file_obj = open(file_name, "w")
    write_obj = csv.writer(file_obj)
    write_obj.writerow(("id", "name", "address", "college", "company", "dob", "age"))
    for i in range(row_size):
        write_obj.writerow(
            (
                i + 1,
                faked_obj.name(),
                str(faked_obj.address()).replace("\n", " "),
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


generate_data(125, "/fake_dataset/csv/ten_kb.csv")
generate_data(1100, "/fake_dataset/csv/hundred_kb.csv")
generate_data(107430, "/fake_dataset/csv/ten_mb.csv")
generate_data(1074300, "/fake_dataset/csv/hundred_mb.csv")
generate_data(11000000, "/fake_dataset/csv/one_gb.csv")
generate_data(11000000 * 2, "/fake_dataset/csv/two_gb.csv")
generate_data(11000000 * 5, "/fake_dataset/csv/five_gb.csv")
generate_data(11000000 * 2, "/fake_dataset/csv/five_gb/second.csv")
generate_data(11000000 * 2, "/fake_dataset/csv/five_gb/first.csv")
generate_data(11000000, "/fake_dataset/csv/five_gb/third.csv")
generate_data(11000000 * 2, "/fake_dataset/csv/ten_gb/second.csv")
generate_data(11000000 * 2, "/fake_dataset/csv/ten_gb/first.csv")
generate_data(11000000, "/fake_dataset/csv/ten_gb/third.csv")
