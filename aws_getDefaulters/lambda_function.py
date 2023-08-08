import os
from pymongo import MongoClient
import json
from datetime import datetime

client = MongoClient(host=os.environ.get("ATLAS_URI"))
db = client.test 
limit = 100

def lambda_handler(event, context):
    invoices = db.invoices

    res_set = set()

    result = invoices.aggregate([
        {"$facet": {
            "unpaid_invoices": [
                {"$match": {"status": {"$ne": "paid"}}},
                # {"$limit": 100},
                {"$unwind": "$dues"},
                {"$lookup": {
                    "from": "dues",
                    "localField": "dues",
                    "foreignField": "_id",
                    "as": "due"
                }},
                {"$unwind": "$due"},
                {"$match": {"due.due_date": {"$lt": datetime.now()}}},
                {"$project": {"student": 1}},
            ],
            "paid_invoices": [
                {"$match": {"status": "paid"}},
                {"$unwind": "$payments"},
                # {"$limit": 100},
                {"$lookup": {
                    "from": "payments",
                    "localField": "payments",
                    "foreignField": "_id",
                    "as": "payment"
                }},
                {"$unwind": "$payment"},
                {"$project": {"student": 1, "payment.due_date": 1, "payment.timestamp": 1}},
                {"$match": {"payment.due_date": {"$lt": "$payment.timestamp"}}},
            ]
        }},
        {"$project": {"result": {"$setUnion": ["$unpaid_invoices.student", "$paid_invoices.student"]}}}
    ])

    if result.alive:
        invoices = result.next()['result'];
        for student_id in invoices:
            student = db.students.find_one({"_id": student_id})
            if student :
                res_set.add(student['name'])

    response = {
        "statusCode": 200,
        "body": json.dumps(list(res_set))
    }
    return response