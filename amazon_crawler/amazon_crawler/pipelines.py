#import pymysql
from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

class AmazonCrawlerPipeline(object):

    # conn = None
    # cur = None

    def process_item(self, item, spider):
        # save product
        '''
        sql = "insert into test_products (asin, productTitle, price, brandName, screenSize, maxScreenResolution_X, maxScreenResolution_Y," \
              "processorSpeed, processorType, processorBrand, processorCount, ram, hardDrive, graphicsCoprocessor, chipsetBrand," \
              "operatingSystem, itemWeight, memoryType, averageBatteryLife, productDimension_X, productDimension_Y, productDimension_Z," \
              "color, imagePath, avgRating) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        vals = (item["asin"], item["productTitle"], item["price"], item['brandName'],  item['screenSize'], item['maxScreenResolution_X'], item['maxScreenResolution_Y'],
               item['processorSpeed'], item['processorType'], item['processorBrand'], item['processorCount'], item['ram'], item['hardDrive'],
               item['graphicsCoprocessor'], item['chipsetBrand'], item['operatingSystem'], item['itemWeight'], item['memoryType'], item['averageBatteryLife'],
               item['productDimension_X'], item['productDimension_Y'], item['productDimension_Z'], item['color'], item['imagePath'], item['avgRating'])
        #print(sql)
        '''
        try:
            #self.cur.execute(sql, vals)
            #self.conn.commit()
            #print("Inserted into Test Database!")
            e = {"asin": item["asin"], "productTitle": item["productTitle"], "price": float(item["price"]), "displaySize": float(item['screenSize']),
                  "screenResoultionSize": (int(item['maxScreenResolution_X']), int(item['maxScreenResolution_Y'])), "processorSpeed": float(item['processorSpeed']),
                  "processorType": item['processorType'], "processorCount": float(item['processorCount']), "brand": item['processorBrand'],
                  "ram": item['ram'], "hardDrive": item['hardDrive']}
            res = es.index(index='testxy', doc_type='xy', body=e)
            print(res)
        except Exception as e:
            #self.conn.rollback()
            print(repr(e))

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
