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
            e = {"asin": item["asin"], "productTitle": item["productTitle"], "price": item["price"], "displaySize": item['screenSize'],
                  "screenResoultionSize": (item['maxScreenResolution_X'], item['maxScreenResolution_Y']), "processorSpeed": item['processorSpeed'],
                  "processorType": item['processorType'], "processorCount": item['processorCount'], "processorBrand": item['processorBrand'],
                  "ram": item['ram'],"brandName": item['brandName'], "hardDriveType": item['hardDriveType'],"hardDriveSize": item['hardDriveSize'], "graphicsCoprocessor": item['graphicsCoprocessor'],
                 "chipsetBrand": item['chipsetBrand'], "operatingSystem": item['operatingSystem'], "itemWeight": item['itemWeight'],
                 "memoryType": item['memoryType'], "averageBatteryLife": item['averageBatteryLife'],
                 "productDimension": (item['productDimension_X'], item['productDimension_Y'], item['productDimension_Z']) ,
                 "color": item['color'], "imagePath": item['imagePath'], "avgRating": item['avgRating']}
            res = es.index(index='amazon', doc_type='laptop', body=e)
            print(res)
        except Exception as e:
            f = open("error","a+")
            f.write(e)
            print(e)

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    @staticmethod
    def convert_date(date_string):
        date_object = datetime.strptime(date_string, '%B %d, %Y')
        return date_object
