from cProfile import run
from dask.distributed import Client, LocalCluster
import time
import json
import dask.dataframe as dd
import itertools

def PA1(user_reviews_csv,products_csv):
    start = time.time()
    client = Client('127.0.0.1:8786')
    client = client.restart()
    print(client)
        
    #######################
    # YOUR CODE GOES HERE #
    #######################
    review = dd.read_csv(user_reviews_csv,blocksize='500MB')
    product = dd.read_csv(products_csv,blocksize='500MB')
#     product1 = product.set_index(product.index, sorted=True)
    
    price=product["price"].describe()
    print(price.compute())
#     descrip = ['mean', 'std', '50%', 'min', 'max']
#     def slice(x):
#         try:
#             a = ""
#             a+=x
#             index_1 = a.index("'")     
#             index_2 = a[index_1+1:].index("'")
#             return a[index_1+1:index_2+1]
#         except ValueError:
#             return x
    
#     def related_IDS(s):
#         s=s.replace("'", '"')
#         return list(itertools.chain.from_iterable(json.loads(s).values()))
#     ids = product['related'].dropna().apply(lambda x: related_IDS(x),meta='list').compute()
#     id_set = set(itertools.chain.from_iterable(ids))
    

#     q1_reviews = (review.isnull().sum() * 100 / len(review)).compute()
#     q1_products = (proudct.isnull().sum() * 100 / len(product)).compute()
#     q2 = product["price"].corr(user["overall"], method="pearson").compute()
#     q3 = price.compute()
#     q4 = product["categories"].dropna().str.strip('][').apply(lambda x: slice(x),meta='string').value_counts().compute()
#     q5 = int(review.merge(product,how="inner").compute().shape[0] != product.compute().shape)
#     q6 = int(product['asin'].isin(id_set).sum().compute() > 0)
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
    
#     print(data)
#     with open('results_PA1.json', 'w') as outfile: json.dump(data, outfile)


#     return runtime