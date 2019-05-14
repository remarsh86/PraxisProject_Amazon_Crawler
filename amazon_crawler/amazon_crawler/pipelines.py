#import pymysql
from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

class AmazonCrawlerPipeline(object):

    # conn = None
    # cur = None


    def process_item(self, item, spider):
        # save product

        try:
            #self.cur.execute(sql, vals)
            #self.conn.commit()
            #print("Inserted into Test Database!")
            if item["productTitle"] != "None" and item["price"] != "None"  and item['processorBrand'] != "None" and item['brandName'] != "None" and item['chipsetBrand'] != "None" and item['hardDriveType'] != "None" :
                e = {"asin": item["asin"], "productTitle": str(item["productTitle"]), "price": float(item["price"]), "displaySize": item['screenSize'],
                      "screenResoultionSize": (item['maxScreenResolution_X'], item['maxScreenResolution_Y']), "processorSpeed": item['processorSpeed'],
                      "processorType": str(item['processorType']), "processorCount": item['processorCount'], "processorBrand": str(item['processorBrand']),
                      "ram": item['ram'],"brandName": str(item['brandName']), "hardDriveType": item['hardDriveType'],"hddSize": item['hddSize'],"ssdSize": item['ssdSize'], "graphicsCoprocessor": str(item['graphicsCoprocessor']),
                     "chipsetBrand": str(item['chipsetBrand']), "operatingSystem": item['operatingSystem'], "itemWeight": item['itemWeight'],
                      "averageBatteryLife": item['averageBatteryLife'],
                     "productDimension": (item['productDimension_X'], item['productDimension_Y'], item['productDimension_Z']) ,
                     "color": item['color'], "imagePath": item['imagePath'], "avgRating": item['avgRating']}

                res = es.index(index='amazon', doc_type='laptop', body=e)

                print(res)
        except Exception as e:
            f = open("error.txt", "a+")
            #f.write("asin : "+item["asin"]+'\n'+repr(e)+"\n")
            #print(repr(e))


    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    @staticmethod
    def convert_date(date_string):
        date_object = datetime.strptime(date_string, '%B %d, %Y')
        return date_object
