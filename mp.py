import pymongo
from bson.code import Code
	
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["arth34"]
mycol = mydb["orders"]

mylist = [
{ "_id": 1, "cust_id": "Ant O. Knee",  "price": 25, "items": [ { "sku": "oranges", "qty": 5, "price": 2.5 }, { "sku": "apples", "qty": 5, "price": 2.5 } ], "status": "A" },
{ "_id": 2, "cust_id": "Ant O. Knee",  "price": 70, "items": [ { "sku": "oranges", "qty": 8, "price": 2.5 }, { "sku": "chocolates", "qty": 5, "price": 10 } ], "status": "A" },
{ "_id": 3, "cust_id": "Busby Bee",  "price": 50, "items": [ { "sku": "oranges", "qty": 10, "price": 2.5 }, { "sku": "pears", "qty": 10, "price": 2.5 } ], "status": "A" },
{ "_id": 4, "cust_id": "Busby Bee",  "price": 25, "items": [ { "sku": "oranges", "qty": 10, "price": 2.5 } ], "status": "A" },
{ "_id": 5, "cust_id": "Busby Bee",  "price": 50, "items": [ { "sku": "chocolates", "qty": 5, "price": 10 } ], "status": "A"},
{ "_id": 6, "cust_id": "Cam Elot",  "price": 35, "items": [ { "sku": "carrots", "qty": 10, "price": 1.0 }, { "sku": "apples", "qty": 10, "price": 2.5 } ], "status": "A" },
{ "_id": 7, "cust_id": "Cam Elot",  "price": 25, "items": [ { "sku": "oranges", "qty": 10, "price": 2.5 } ], "status": "A" },
{ "_id": 8, "cust_id": "Don Quis",  "price": 75, "items": [ { "sku": "chocolates", "qty": 5, "price": 10 }, { "sku": "apples", "qty": 10, "price": 2.5 } ], "status": "A" },
{ "_id": 9, "cust_id": "Don Quis",  "price": 55, "items": [ { "sku": "carrots", "qty": 5, "price": 1.0 }, { "sku": "apples", "qty": 10, "price": 2.5 }, { "sku": "oranges", "qty": 10, "price": 2.5 } ], "status": "A" },
{ "_id": 10, "cust_id": "Don Quis",  "price": 25, "items": [ { "sku": "oranges", "qty": 10, "price": 2.5 } ], "status": "A" }
]
		
x = mycol.insert_many(mylist)

for x in mycol.find():
	print(x)

mapFunction1 = Code("function() { emit(this.cust_id, this.price);}")
reduceFunction1 = Code("function(keyCustId, valuesPrices) {return Array.sum(valuesPrices)}")
result = mycol.map_reduce(mapFunction1, reduceFunction1, "result")

rescol = mydb["result"]
for y in rescol.find():
	print(y)
































