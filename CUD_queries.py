import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from datetime import datetime


cred = credentials.Certificate('certificate.json')

firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://assignment-2-e7286-default-rtdb.firebaseio.com/'
})


# -------------------------------------------------------------------------------------------- #
# Inserting an actor
# SQL
'''
INSERT INTO actor(actor_id, first_name, last_name, last_update)
VALUES (1000, 'John', 'Smith', '2013-05-26 14:47:57.620000')
'''


# Firebase
# 1st version: The actor_id is given to the function
def insert_actor_with_id(actor_id,first_name, last_name, last_update):
    # Preparing the new entry
    entry = {
        'actor_id': actor_id,  # Assuming the IDs are 1-indexed
        'first_name' : first_name,
        'last_name' : last_name,
        'last_update' : last_update
             }

    # The insert operation
    db.reference('actor').child(str(actor_id)).set(entry)


# 2nd version: The actor_id is inferred from the size of the "table"
def insert_actor(first_name, last_name, last_update):
    # This is for having a manual id
    all_actors = dict(db.reference("actor").get())
    size = len(all_actors)

    # Preparing the new entry
    entry = {
        'actor_id': size + 1,  # Assuming the IDs are 1-indexed
        'first_name' : first_name,
        'last_name' : last_name,
        'last_update' : last_update
             }

    # The insert operation
    db.reference('actor').child(str(size+1)).set(entry)


# -------------------------------------------------------------------------------------------- #
# Updating the store id of staff members in a certain store
# (A store is being closed and all the staff members are being relocated)
# SQL
'''
UPDATE staff 
SET store_id = 55 
WHERE store_id = 43
'''


# Firebase
def update_store(old_store_id,new_store_id):
    print(old_store_id)
    # Getting all staff members that work in the store with id 'old_store_id'
    staff = db.reference('staff').order_by_child('store_id').equal_to(old_store_id).get()
    
    # Converting to the appropriate format
    staff = dict(staff)
    staff = list(staff.values())

    # Updating them one by one
    for x in staff:
        print(x)
        db.reference('staff').child(str(x['staff_id'])).child('store_id').set(new_store_id)


# -------------------------------------------------------------------------------------------- #
# Remove a store (works in conjunction with the above update query)
# SQL
'''
DELETE FROM store
WHERE store_id = 43
'''


# Firebase
def delete_store(store_id):
    # Simple and easy
    db.reference('store').child(str(store_id)).delete()


print("Let's test some queries.")
dummy = input("Press enter to go to the next query")

print("Making an insertion.")
print("Data :  (1000, 'John', 'Smith', '2013-05-26 14:47:57.620000')")
print("\n")
print(" SQL equivalent : ")
print('''
INSERT INTO actor(actor_id, first_name, last_name, last_update)
VALUES (1000, 'John', 'Smith', '2013-05-26 14:47:57.620000')
''')
print("\n")
before = datetime.now()
insert_actor_with_id(1000, "John", "Smith", '2013-05-26 14:47:57.620000')
after = datetime.now()
delta = after - before
print("The query took " + str(delta.total_seconds()) + " seconds.")

dummy = input("Press enter to go to the next query")

print("Making an update.")
print("\n")
print(" SQL equivalent : ")
print('''
UPDATE staff 
SET store_id = 2
WHERE store_id = 1
''')
print("\n")
before = datetime.now()
update_store(1, 2)
after = datetime.now()
delta = after - before
print("The query took " + str(delta.total_seconds()) + " seconds.")

dummy = input("Press enter to go to the next query")

print("Making a deletion.")
print("\n")
print(" SQL equivalent : ")
print('''
DELETE FROM store
WHERE store_id = 1
''')
print("\n")
before = datetime.now()
delete_store(1)
after = datetime.now()
delta = after - before
print("The query took " + str(delta.total_seconds()) + " seconds.")
