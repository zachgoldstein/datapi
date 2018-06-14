import json
from datetime import datetime
import random
import numpy

from faker import Faker
fake = Faker()

def write_data(num_data, open_file):
  id = 0
  buffer = ''
  for i in xrange(num_data):
    data_object = {
      "id": i + 1000000,
      "name":fake.name(),
      "date": datetime.now().isoformat(),
      "total_plumbuses":numpy.random.randint(1, 1000000),
      "distance": numpy.random.random(),
      "has_existential_identity_crisis":random.choice([True,False])
    }
    possible_data = {
      "address":fake.address(),
      "text":fake.paragraphs(nb=100),
      "job":fake.job(),
      "phone_number": fake.phone_number(),
      "favorite_color": fake.safe_color_name(),
      "company": fake.company(),
      "company_catch_phrase": fake.catch_phrase(),
      "company_bs": fake.bs(),
      "username": fake.user_name()
    }
    for _ in xrange(5):
      keys = possible_data.keys()
      key = keys[numpy.random.randint(0, len(keys) - 1)]
      data_object[key] = possible_data[key]

    open_file.write(json.dumps(data_object)+" \n")
    print("wrote data %s", i)

with open("data.jsonfiles", "w") as f:
  write_data(100, f)
