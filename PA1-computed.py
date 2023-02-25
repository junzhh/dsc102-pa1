from cProfile import run
from dask.distributed import Client, LocalCluster
import time
import json
import dask.dataframe as dd
import itertools
import dask.array as da


def PA1(user_reviews_csv,products_csv):
    start = time.time()
    client = Client('127.0.0.1:8786')
    client = client.restart()
    print(client)
        
    #######################
    # YOUR CODE GOES HERE #
    ####################### 
    review_di = {"reviewerID": str, "asin": str, "reviewerName": str, "helpful": str, "reviewText": str, "overall": float, "summary": str, "unixReviewTime": float, "reviewTime": str}
    product_id = {"asin":str, "salesRank": str, "imUrl": str, "categories": str, "title": str, "description": str, "price": float, "related": str, "brand": str}
    review = dd.read_csv(user_reviews_csv,dtype=review_di)
    product = dd.read_csv(products_csv,dtype=product_id)
    df = product[["asin","price"]].merge(review[["asin","overall"]],how="inner",on="asin").set_index("asin")
    def slice(x):
        try:
            a = ""
            a+=x
            index_1 = a.index("'")     
            index_2 = a[index_1+1:].index("'")
            return a[index_1+1:index_2+1]
        except ValueError:
            return x
    task1_reviews = (review.isnull().sum() * 100 / len(review))
    task1_products =(product.isnull().sum() * 100 / len(product))
    task2 = df['price'].corr(df['overall'],method="pearson")
    task3 = product["price"].describe()
    task4 = product["categories"].dropna().str.strip('][').apply(slice).value_counts()
    task1_reviews, task1_products, task2, task3, task4 = da.compute(task1_reviews, task1_products, task2, task3, task4)
    task1_reviews = task1_reviews.round(2)
    task1_products = task1_products.round(2)
    task2 = round(task2,2)
    task3  = task3.round(2)
    task3 = task3[['mean','std','50%','min', 'max']]
    task4 = task4.sort_values(ascending=False)
    roduct_keys = product.asin.values
    product_asin_hashmap = dict.fromkeys(product['asin'],1)
    related_ids = review['asin']
    q5_flag = 0
    for i in related_ids:
        if i not in product_asin_hashmap.keys():
            q5_flag = 1
            break
    def check(elem):
        try:
            keys = ['also_bought', 'also_viewed', 'buy_after_viewing','bought_together']
            for k in keys:
                if isinstance(elem[k], list):
                    for i in elem[k]:
                        if i not in product_asin_hashmap.keys():
                            return 1
                else:
                    if elem[k] not in product_asin_hashmap.keys():
                        return 1
        except KeyError:
            pass
    related = product['related'].dropna()
    related = related.dropna().apply(lambda x: json.loads(x.replace("'",'"')),meta="dict")
    q6_flag = 0
    for elem in related:
        if check(elem) == 1:
            q6_flag = 1
            break
    
#     print(task1_reviews)
#     print(task1_products)
    
#     q1_reviews = (review.isnull().sum() * 100 / len(review))
#     q1_products = (product.isnull().sum() * 100 / len(product))
#     q2 = df.corr(method="pearson")
#     q3 = 

#     q4 = None
#     q5 = None
#     q6 = None
#     end = time.time()
#     runtime = end-start

#     # Write your results to "results_PA1.json" here
#     with open('OutputSchema_PA1.json','r') as json_file:
#         data = json.load(json_file)
#         print(data)

#         data['q1']['products'] = json.loads(q1_reviews.to_json())
#         data['q1']['reviews'] = json.loads(q1_products.to_json())
#         data['q2'] = q2
#         data['q3'] = json.loads(q3.to_json())
#         data['q4'] = json.loads(q4.to_json())
#         data['q5'] = q5
#         data['q6'] = q6
    
#     # print(data)
#     with open('results_PA1.json', 'w') as outfile: json.dump(data, outfile)


#     return runtime
