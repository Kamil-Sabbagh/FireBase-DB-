
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from datetime import datetime
import timeit

cred = credentials.Certificate('certificate.json')

firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://assignment-2-e7286-default-rtdb.firebaseio.com/'
})


def query_1_local():

    # Making a joint table
    payments = list(db.reference('payment').order_by_child('rental_id').get())
    rentals = list(db.reference('rental').order_by_child('rental_id').get())

    p_i = 0
    res = []
    for i in rentals:
        while payments[p_i]['rental_id'] < i['rental_id']:
            p_i += 1
        if payments[p_i]['rental_id'] == i['rental_id']:
            tmp = payments[p_i]
            for j in i:
                tmp[j] = i[j]
            res.append(tmp)

    # Making the aggregation
    payments_by_amount = list(db.reference('payment').order_by_child('amount').get())
    for i in res:
        print("Doing b search")
        # Binary search the answer

        low = 0
        high = len(payments_by_amount)
        while(low<high):
            #print(str(low) + ' -> ' + str(high))
            if low<0:
                low = -1
                break
            mid = (low+high)//2
            if payments_by_amount[mid]['amount'] < i['amount']:
                low = mid+1
            else:
                high = mid
        i['count_smaller_pay'] = low

    return res

# This can be used to listen to value changes in the database
'''def cb(something):
    print("A change was made to the database.")
    print(something.event_type)
    print(something.path)
    #print(something.data)'''






def query1():
    payments = list(db.reference('payment').get())
    counter = 3
    for i in payments:
        print(i)
        counter-=1
        if not counter:
            break
    res = []

    for i in payments:
        rental = list(dict(db.reference('rental').order_by_child('rental_id').equal_to(i['rental_id']).get()).values())
        tmp = i
        if len(rental)>1:
            print('Found more then one corresponding rental')
            break

        if len(rental)==0:
            print('We have a problem, rental is empty')
            continue

        #print(rental[0])
        for j in rental[0]:
            tmp[j] = rental[0][j]
        #print(tmp)
        res.append(tmp)

    return res


before = datetime.now()
result = query_1_local()
after = datetime.now()
delta = after - before
print(result)
print("Total time to complete the query : " + str(delta.total_seconds()))

#print(timeit.timeit(query1))
db.reference().listen(cb)
