from pprint import pprint

import pymongo
import re

feature_dict={"1":"Amazon Alexa",
              "2":"Motion xcelerator",
              "3":"QLED",
              "4":"Quantum Processor",
              "5":"Bluetooth audio",
              "6":"Dolby Atmos",
              "7":"laser technology",
              "8":"Apple Airplay",
              "9":"Google Assistant",
              "10":"Quantum HDR 24X",
              "11":"Multiple Voice Assistant",
              "12":"Gaming"
              }

tv_dict={"1":"Samsung",
        "2":"Sony",
        "3":"LG",
        "4":"Hisense"
        }

print("Get a Personalized Television Recommendation")
print("--------------------------------------------")
print("Choose a Brand\n1. Samsung\n2. Sony\n3. LG\n4. Hisense")
brand = input("Preferred Brand :")
print("Budget Range")
try:
    minBudget = int(input("Minimum Price : "))
except:
    minBudget=0

try:
    maxBudget = int(input("Maximum Price : "))
except:
    maxBudget = 10000
try:
    rating = int(input("User Rating : "))
except:
    rating = 4

print("Choose Preferred Features (choose multiple options as 1,2,....)")
print("1. Amazon Alexa\n2. Motion xcelerator\n3. QLED\n4. Quantum Processor\n5. Bluetooth audio\n6. Dolby Atmos\n7. laser Technology")
print("8. Apple Airplay\n9. Google Assistant\n10. Quantum HDR 24X\n11. Multiple Voice Assistant\n12. Gaming")
feature = input("Choose your selection : ")
print("!!!Please Sit back and Relax, we will show you some suggestions!!!")
print("Here is your list")
print("*******************")

tvname=tv_dict[brand]

featureKey= feature.split(",")
featurestr=""
for item in featureKey:
    featurestr+=feature_dict[item]+"|"
featurelist=featurestr.rstrip(featurestr[-1])



featureRegx=re.compile(featurelist,re.IGNORECASE)

regx = re.compile(tvname, re.IGNORECASE)
conn_str = "mongodb+srv://Karthi:Arunith0520@cluster0.iwu5kws.mongodb.net/test"
try:
	client = pymongo.MongoClient(conn_str)
except Exception:
	print("Error:" + Exception)


db = client.BDM1113Project
collection = db.ProductTelevision


filter={
    'Rating': {
        '$gt': rating
    },
    'Price': {
        '$gt': minBudget,
        '$lt': maxBudget
    },
    'Product Name': regx,
    'Product Features': featureRegx

}

result = client['BDM1113Project']['ProductTelevision'].find(
  filter=filter
)

for doc in result:
    #pprint(doc)
    print("Product Name :",doc['Product Name'])
    print("Price :",doc['Price'])
    print("Rating :",doc['Rating'])
    print("Review Count :",doc['Review Count'])
    finalfeature=[]
    for item in doc['Product Features']:
        finalfeature.append(item)
    print("Product Feature :")
    for feat in finalfeature:
        print(feat)

    print("\n---------------------------------------------")

client.close();












