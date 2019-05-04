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
            #self.conn.rollback()
            print(e)
            print(item)

            #for key in item :
            #    if item[key] is None:
            #        print(key,"is None in ",item["asin"])
            #if item["asin"] == "B07D6TSMC9" :
            #    print(e["price"])


        # saving reviews
        #reviews = item["reviews"]

        '''
        for review in reviews:
            review_date = review["review_date"]
            review_date = self.convert_date(review_date)
            sql = "insert into reviews (review_id, product_id, subject, user_rating, review_text, review_date) " \
                  "VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % \
                  (review["review_id"], item["product_id"], review["subject"], review["user_rating"],
                   review["review_text"], review_date)
            try:
                self.cur.execute(sql)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(repr(e))
                print(item["book_id"])
        '''

    # def open_spider(self, spider):
    #     self.conn = pymysql.connect(host="localhost",  # your host
    #                          user="alfred",  # username
    #                          passwd="mysqlmysql",  # password
    #                          db="test_db")  # name of the database
    #
    #     # Create a Cursor object to execute queries.
    #     self.cur = self.conn.cursor()

    # def close_spider(self, spider):
    #     self.cur.close()
    #     self.conn.close()

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    @staticmethod
    def convert_date(date_string):
        date_object = datetime.strptime(date_string, '%B %d, %Y')
        return date_object
