from faker import Faker
import random
from utils.db_utils import connect_to_db, load_to_db
from const.exceptions import DBConnectionError
from const.const import DB_CONFIG

fake = Faker('id_ID')
emails = ["gmail", "outlook", "yahoo"]

def generate_user():
    #generate user name
    mail = random.choices(
        emails,
        weights=[75, 20, 5], k=1)[0]
    rand_num = random.randint(1, 99)
    first_name = fake.unique.first_name()
    last_name = fake.unique.last_name()
    full_name = first_name+" "+last_name
    
    #generate email variants
    email1 = f"{first_name.lower()}_{last_name.lower()}@{mail}.com"
    email2 = f"{last_name.lower()}{rand_num}@{mail}.com"
    email_variant = [email1,email2]
    email = random.choices(
        email_variant,
        weights=[50, 50], k=1)[0]
    user = {"name":full_name,"email":email}
    return user

conn = connect_to_db(DB_CONFIG)
if conn is None:
    raise DBConnectionError("Postgress Connection Failed")
else:
    print("Generate User...")
    user = generate_user()
    load_to_db(
        """
            INSERT INTO users (name, email)
            VALUES (%s, %s)
        """,(user["name"],user["email"]),conn
    )
    print("Successfully generated user and load into db...")

#FOR TESTING
    # i=1

    # while i <= 10:
    #     i+=1
    #     generate_user()

