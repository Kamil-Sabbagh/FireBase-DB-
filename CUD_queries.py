
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from datetime import datetime
import timeit

cred = credentials.Certificate('certificate.json')

firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://assignment-2-e7286-default-rtdb.firebaseio.com/'
})


# Inserting an actor
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


# Updating the store id of staff members in a certain store
# (A store is being closed and all the staff members are being relocated)
def update_store(old_store_id,new_store_id):
    # Getting all staff members that work in the store with id 'old_store_id'
    staff = db.reference('staff').order_by_child('store_id').equal_to(old_store_id).get()

    staff = list(staff)

    # Updating them one by one
    for x in staff:
        db.reference('staff').child(x['staff_id']).child('store_id').set(new_store_id)


# Remove a store (works in conjunction with the above update query)
def delete_store(store_id):
    db.reference('store').child(store_id).delete()
