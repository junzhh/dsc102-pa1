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
   review = dd.read_csv(user_reviews_csv,dtype={"reviewerID": str, "asin": str, "reviewerName": str, "helpful": str, "reviewText": str, "overall": float, "summary": str, "unixReviewTime": float, "reviewTime": str})
#     print('review',review.shape.compute())
#     review = review.set_index(review.index,ascending=True,n=100)
    product = dd.read_csv(products_csv,dtype={"asin":str, "salesRank": str, "imUrl": str, "categories": str, "title": str, "description": str, "price": float, "related": str, "brand": str})
#     print('product',product.shape[0].compute())
#     product = product.set_index(product.index,ascending=True,n=100)
    review = review.repartition(npartitions=200)
    review = review.reset_index(drop=True)
    product = product.repartition(npartitions=200)
    product = product.reset_index(drop=True)
    df = product[["asin","price"]].merge(review[["asin","overall"]],how="inner",on="asin").set_index("asin")
    price=product["price"].describe(percentiles=[0.5])
    
    def slice(x):
        try:
            a = ""
            a+=x
            index_1 = a.index("'")     
            index_2 = a[index_1+1:].index("'")
            return a[index_1+1:index_2+1]
        except ValueError:
            return x
    
    def related_IDS(s):
        s=s.replace("'", '"')
        return list(itertools.chain.from_iterable(json.loads(s).values()))
    ids = product['related'].dropna().apply(related_IDS,meta='list')
    id_set = set(itertools.chain.from_iterable(ids))
  

    q1_reviews = (review.isnull().sum() * 100 / len(review))
    q1_products = (product.isnull().sum() * 100 / len(product))
    q2 = df.corr(method="pearson")
    q3 = product["price"].describe(percentiles=[0.5]).loc["mean","std","min","50%","max"]
#     print(q3)
# #     q1_reviews,q1_products=da.compute(q1_reviews,q1_products)
    q4 = product["categories"].dropna().str.strip('][').apply(slice).value_counts()
    q5 = int(review.merge(product,how="inner").shape[0] != product.shape[0])
    q6 = int(product['asin'].isin(id_set).sum() > 0)
#     print(q4)
    
    end = time.time()
    runtime = end-start

#     # Write your results to "results_PA1.json" here
    with open('OutputSchema_PA1.json','r') as json_file:
        data = json.load(json_file)
        print(data)

        data['q1']['products'] = json.loads(q1_reviews.to_json())
        data['q1']['reviews'] = json.loads(q1_products.to_json())
        data['q2'] = q2
        data['q3'] = json.loads(q3.to_json())
        data['q4'] = json.loads(q4.to_json())
        print(1)
        data['q5'] = q5
        data['q6'] = q6
    
    print(data)
    with open('results_PA1.json', 'w') as outfile: json.dump(data, outfile)


    return runtime
