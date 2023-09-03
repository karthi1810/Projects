import pandas as pd
import pymongo
import json
from pymongo import MongoClient, InsertOne
import requests
from bs4 import BeautifulSoup as soup

headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0'}
productName=[]
salePrice=[]
prodRating=[]
prodReviewCount=[]
featurelist=[]
prodFeature = []

for var in range(1,7):
    html_contents= requests.get("https://www.costco.ca/televisions.html?currentPage=%i&pageSize=24"%(var),headers=headers)
    page_cnt=html_contents.content
    page_soup=soup(page_cnt,"html.parser")
    container=page_soup.find_all("div",{"class":"col-xs-6 col-lg-4 col-xl-3 product"})

    #print(container)
    con=container[0]
    #print(soup.prettify(con))

    # Product name
    for co in container:
        prodName=co.div.img["alt"]
        productName.append(prodName)


    #Sale Price
    for co in container:
        price=co.find_all("div",{"class":"price"})
        salePrice.append(price[0].text.strip())

    #Rating
    for co in container:
        rating=co.find("meta",{"itemprop":"ratingValue"})
        prodRating.append(float(rating["content"])if rating else 0)

    #Review
    for co in container:
        reviewCount=co.find("meta",{"itemprop":"reviewCount"})
        #print(reviewCount["content"]if reviewCount else "No Reviews")
        prodReviewCount.append(int(reviewCount["content"])if reviewCount else 0)

    #Features
    for co in container:
        productFeatures=co.find_all("ul",{"class":"product-features hidden-xs hidden-sm"})
        each = productFeatures[0].text.strip().split('\n\n')
        if len(each) > 1:
            prodFeature.append(each)
        else:
            prodFeature.append(each[0].split('\n'))


data = {
    "Product Name": productName,
    "Price": salePrice,
    "Rating": prodRating,
    "Review Count": prodReviewCount,
    "Product Features":prodFeature

}

df = pd.DataFrame.from_dict(data, orient='index')
df = df.transpose()
df.to_json('ProductTelevision.json', orient='records', lines=True)

# Loading JSON Data to MongoDB
conn_str = "mongodb+srv://Karthi:Arunith0520@cluster0.iwu5kws.mongodb.net/test"
try:
	client = pymongo.MongoClient(conn_str)
except Exception:
	print("Error:" + Exception)


db = client.BDM1113Project
collection = db.ProductTelevision

requesting = []
with open("ProductTelevision.json","r") as f:
     for jsonObj in f:
        myDict = json.loads(jsonObj)
        requesting.append(InsertOne(myDict))

result = collection.bulk_write(requesting)
client.close()





