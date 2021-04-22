import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from datetime import datetime

cred = credentials.Certificate('certificate.json')

firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://assignment-2-e7286-default-rtdb.firebaseio.com/'
})


def query_1_local():
    # Acquiring all tables
    payments = list(db.reference('payment').order_by_child('rental_id').get())
    rentals = list(db.reference('rental').order_by_child('rental_id').get())

    payments = payments[1:]
    rentals = rentals[1:]
    #print(payments[1])
    # The tables are sorted, so we can use 2 pointers to match the rental_id
    p_i = 0
    joint_table = []
    for i in rentals:
        while payments[p_i]['rental_id'] < i['rental_id']:
            p_i += 1
        if payments[p_i]['rental_id'] == i['rental_id']:
            tmp = payments[p_i]
            for j in i:
                tmp[j] = i[j]
            joint_table.append(tmp)

    # Making the aggregation
    payments_by_amount = list(db.reference('payment').order_by_child('amount').get())
    payments_by_amount = payments_by_amount[1:]
    for i in joint_table:

        # Binary search the answer
        low = 0
        high = len(payments_by_amount)
        while low < high:
            if low < 0:
                low = -1
                break
            mid = (low + high) // 2
            if payments_by_amount[mid]['amount'] < i['amount']:
                low = mid + 1
            else:
                high = mid
        i['count_smaller_pay'] = low

    return joint_table





# The next function is just an example of a slow way to perform join
def query_1_join_operation_slow():
    payments = list(db.reference('payment').get())
    counter = 3
    for i in payments:
        print(i)
        counter -= 1
        if not counter:
            break
    res = []

    #The slow part : getting the rental information directly from the database
    for i in payments:
        rental = list(dict(db.reference('rental').order_by_child('rental_id').equal_to(i['rental_id']).get()).values())
        tmp = i
        # In case there is no corresponding rental
        if len(rental) == 0:
            continue

        # We are assuming that there is only one rental for each payment
        for j in rental[0]:
            tmp[j] = rental[0][j]
        res.append(tmp)
    return res


# This is the fastest version of the function, using denormalization
def query_1_with_extra_table():
    table = db.reference('payment_rental_smaller_pay').get()
    if table is None:
        print("The table doesn't exist. Going to compute the table content...")
        new_table = query_1_local()
        db.reference('payment_rental_smaller_pay').set(new_table)
        return new_table
    return table

print("Executing query 1")
before = datetime.now()
result = query_1_with_extra_table()
after = datetime.now()
delta = after - before
print(result)
print("Total time to complete the query : " + str(delta.total_seconds()))


# This is an example on how to add a listener of the database
# This can be used to listen to value changes in the database
# This function updates the joint table of rentals_and_payments
def cb(something):
    # print("A change was made to the database.")
    # print(something.event_type)
    # print(something.path)
    # print(something.data)
    if something.path == '/':
        return
    if something.event_type == 'put':
        print("An entry was added to the database.")
        rental = db.reference('rental').order_by_child('rental_id').equal_to(something.data['rental_id']).get()
        if rental is None:
            return
        tmp = something.data
        for x in rental:
            tmp[x] = rental[x]
        db.reference('payment_rental_smaller_pay').child(something.path[1:]).set(something.data)


# We are going to set the listener to listen to new payments
# db.reference('payment').listen(cb)
