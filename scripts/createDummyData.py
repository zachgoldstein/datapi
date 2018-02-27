import json
from datetime import datetime
import random

from faker import Faker
fake = Faker()

def write_data(num_data, open_file):
  id = 0
  for i in range(num_data):
    data_object = {
      "id": i + 1000000,
      "name":fake.name(),
      "date": datetime.now().isoformat(),
      "total_plumbuses":random.randint(1, 1000000),
      "has_existential_identity_crisis":random.choice([True,False])
    }
    possible_data = {
      "address":fake.address(),
      "text":fake.text(),
      "job":fake.job(),
      "phone_number": fake.phone_number(),
      "favorite_color": fake.safe_color_name(),
      "company": fake.company(),
      "company_catch_phrase": fake.catch_phrase(),
      "company_bs": fake.bs(),
      "username": fake.user_name()
    }
    for _ in range(5):
      key = random.choice(possible_data.keys())
      data_object[key] = possible_data[key]

    open_file.write(json.dumps(data_object)+" \n")
    print("wrote data %s", i)

with open("data.jsonfiles", "w") as f:
  write_data(1000, f)
