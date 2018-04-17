
# coding: utf-8

# ## Calculate the total expenditure per customer id
# 
# #### SAMPLE 
# | CUSTOMER_ID | ITEM_CODE |  EXPENDITURE | 
# |---|---|---|---|---|---|---|
# |43|1087|92.88|


from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('customer expenditure')

# use it when run as script
sc = SparkContext(conf=conf)

data = sc.textFile('../datasets/customer-orders.csv')

data.sample(False,0.001).collect()

expenditure_agg = data.map(lambda line: (int(line.split(',')[0]),float(line.split(',')[2]))).reduceByKey(lambda x,y : round(x+y),2).map(lambda x : (x[1],x[0]))

print(expenditure_agg.sortByKey())


expenditure_agg = data.map(lambda line: (int(line.split(',')[0]),float(line.split(',')[2]))) \
                        .reduceByKey(lambda x,y : round(x+y,2)).map(lambda x: (x[1],x[0]))

results=expenditure_agg.sortByKey(ascending=False).collect()

for result in results:
    print("%.2f  -- %d" %  result)


