import pymongo
import lambda_function
import json

if __name__ == '__main__':
    print("welcome")
    print(lambda_function.lambda_handler(None, None))
    print('done')