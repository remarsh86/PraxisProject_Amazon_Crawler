import scrapy
from amazon_crawler.amazon_crawler.items import AmazonScrapItem
from scrapy import signals
from urllib import request
import re
import os
from amazon_crawler.amazon_crawler import proxy_manager as pm
from fake_useragent import UserAgent
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process, Queue
from twisted.internet import reactor
from scrapy.selector import Selector


#ua = UserAgent()
class ProductSpider(scrapy.Spider):
    name = "product-scraper" #identifies the spider

    def __init__(self, *args, **kwargs):
        super(ProductSpider, self).__init__(*args, **kwargs)

    #start_urls = open('amazon_crawler/spiders/remaining_laptops.txt', 'r').readlines()
    #start_urls = [url[:url.find('#')] for url in start_urls]# if 'offer-listing' not in url]
    #start_urls = set(start_urls)
    start_urls = []
    limit = 0




    for file in os.listdir("../product_xml_files"):
    #for file in os.listdir("product_xml_files"):
        if limit < 100:
            #start_urls.append("file:///Users/rebeccamarsh/Documents/PraxisProject_Amazon_Crawler/amazon_crawler/product_xml_files/" + str(file))	#replace with your local path

            #Relative Path for all to use:
            start_urls.append("file://" + os.path.realpath("../product_xml_files") + "/" + str(file))	#replace with your local path

        else:
            break
        limit += 1




    #products_already_scrapped = list()
    #product_scraped = list()


    # parse() function called to handle the response downloaded for each of the Requests made (during scraping)
    # The response parameter is an instance of TextResponse
    # The parse() method usually parses the response, extracting the scraped data as dicts and also finding new URLs to
    # follow and creating new requests (Request) from them.
    def parse(self, response):
        sel = Selector(text=response.body)

        product = AmazonScrapItem()
        product["asin"] = self.getASIN(sel)
        #asin = self.getASIN(response)
        #if product["asin"] not in self.product_scraped:
        #    self.writeXML(response, asin)
        #    self.product_scraped.append(product["asin"])
        #else:
        #    print("Product with ASIN {} is already crawled!".format(product["asin"]))

        product["productTitle"] = self.getProductTitle(sel)
        product["price"] = self.getPrice(sel)
        product["brandName"] = self.getBrandName(sel)
        product["screenSize"] = self.getScreenSize(sel)
        product["maxScreenResolution_X"] = self.getMaxScreenResolution_X(sel)
        product["maxScreenResolution_Y"] = self.getMaxScreenResolution_Y(sel)
        product["processorSpeed"] = self.getProcessorSpeed(sel)
        product["processorType"] = self.getProcessorType(sel)
        product["processorBrand"] = self.getProcessorBrand(sel)
        product["processorCount"] = self.getProcessorCount(sel)
        product["ram"] = self.getRAM(sel)
        product["hardDrive"] = self.getHardDrive(sel)
        product["graphicsCoprocessor"] = self.getGraphicsCoprocessor(sel)
        product["chipsetBrand"] = self.getChipsetBrand(sel)
        product["operatingSystem"] = self.getOperatingSystem(sel)
        product["itemWeight"] = self.getItemWeight(sel)
        product["memoryType"] = self.getMemoryType(sel)
        product["averageBatteryLife"] = self.getAverageBatteryLife(sel)
        prodDimensions = self.getProductDimensions(sel)
        if prodDimensions is None:
            product["productDimension_X"] = None
            product["productDimension_Y"] = None
            product["productDimension_Z"] = None
        else:
            product["productDimension_X"] = prodDimensions[0]
            product["productDimension_Y"] = prodDimensions[1]
            product["productDimension_Z"] = prodDimensions[2]
        product["color"] = self.getColor(sel)
        #downloadedImage = self.downloadImage(response, asin)
        product["imagePath"] = self.downloadImage(sel)#self.getImage(downloadedImage)
        product["avgRating"] = self.getAvgRating(sel)
        
        #reviews = self.getReviews(response)
        #product["review"] = reviews
        #if product["asin"] not in self.products_already_scrapped:
        print("Product", product)
        yield product
        #self.products_already_scrapped.append(product["asin"])
            

        '''
        more_products = self.get_more_products(response)
        for prod in more_products:
            prod_link = prod[0]
            prod_asin = prod[1]
            #if prod_asin not in self.products_already_scrapped:
            #if prod_asin not in self.product_scraped:
            yield scrapy.Request(prod_link, callback=self.parse, meta={'proxy': pm.ProxyPool().get_random_proxy()})
        '''

    @staticmethod
    def downloadImage(sel):
        path = sel.xpath('//img/@data-old-hires').get()
        return path
        #try:
        #    f = open('product_images/' + str(asin) + '.jpg', 'wb')
        #    f.write(request.urlopen(path).read())
        #    f.close()
        #    return 'product_images/' + str(asin) + '.jpg'
        #except:
        #    return None

    @staticmethod
    def writeXML(sel, asin):
        content = sel.body.decode('utf-8')
        try:
            f = open('product_xml_files/' + str(asin) + '.xml', 'w')
            f.write(content)
            f.close()
        except:
            return None

    @staticmethod
    def getASIN(sel):
        asin = sel.xpath('//div[@id="cerberus-data-metrics"]/@data-asin').get()
        return asin

    @staticmethod
    def getProductTitle(sel):
        productTitle = sel.xpath('//span[@id="productTitle"]/text()').get().strip()
        return productTitle

    @staticmethod
    def getPrice(sel):
        price = sel.xpath('//div[@id="cerberus-data-metrics"]/@data-asin-price').get()
        return price

    @staticmethod
    def getBrandName(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Brand Name":
                    brandName = tr.xpath('.//td/text()').get().strip()
                    return brandName
            except:
                pass

        return None

    @staticmethod
    def getScreenSize(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Screen Size":
                    screenSize = tr.xpath('.//td/text()').get().strip()
                    screenSize = float(screenSize[:screenSize.find('inches')-1])    # inches
                    return screenSize
            except:
                pass

        return None

    @staticmethod
    def getMaxScreenResolution_X(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Max Screen Resolution":
                    maxScreenResolution_X = tr.xpath('.//td/text()').get().strip()
                    w = int(maxScreenResolution_X[:maxScreenResolution_X.find('x')].strip())
                    return w                                                     # width
            except:
                pass

        return None

    @staticmethod
    def getMaxScreenResolution_Y(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Max Screen Resolution":
                    maxScreenResolution_Y = tr.xpath('.//td/text()').get().strip()
                    h = maxScreenResolution_Y[maxScreenResolution_Y.find('x')+1:].strip()
                    if 'pixels' in h:
                        h = int(re.sub('pixels', '',  h).strip())
                    else:
                        h = int(h)
                    return h                                                      # height
            except:
                pass

        return None

    @staticmethod
    def getProcessorSpeed(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor":
                    processor = tr.xpath('.//td/text()').get().strip()
                    processorSpeed = float(processor[:processor.find('GHz')].strip())       # GHz
                    return processorSpeed
            except:
                pass

        return None

    @staticmethod
    def getProcessorType(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor":
                    processor = tr.xpath('.//td/text()').get().strip()
                    processorType = processor[processor.find('GHz')+3:].strip()
                    return processorType
            except:
                pass

        return None

    @staticmethod
    def getProcessorBrand(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor Brand":
                    processorBrand = tr.xpath('.//td/text()').get().strip()
                    return processorBrand
            except:
                pass

        return None

    @staticmethod
    def getProcessorCount(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor Count":
                    processorCount = int(tr.xpath('.//td/text()').get().strip())
                    return processorCount
            except:
                pass

        return None

    @staticmethod
    def getRAM(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "RAM":
                    ram = tr.xpath('.//td/text()').get().strip()
                    ram = float(ram[:ram.find('GB')-1].strip())
                    return ram
            except:
                pass

        return None

    @staticmethod
    def getHardDrive(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    if 'GB' in hd:
                        hd = int(hd[:hd.find('GB')-1].strip())
                    elif 'TB' in hd:
                        hd = int(hd[:hd.find('TB')-1].strip())*1000
                    return hd                                                   # GB
            except:
                pass

        return None

    @staticmethod
    def getGraphicsCoprocessor(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Graphics Coprocessor":
                    gc = tr.xpath('.//td/text()').get().strip()
                    return gc
            except:
                pass

        return None

    @staticmethod
    def getChipsetBrand(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Chipset Brand":
                    cb = tr.xpath('.//td/text()').get().strip()
                    return cb
            except:
                pass

        return None

    @staticmethod
    def getOperatingSystem(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Operating System":
                    os = tr.xpath('.//td/text()').get().strip()
                    return os
            except:
                pass

        return None

    @staticmethod
    def getItemWeight(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Item Weight":
                    weight = tr.xpath('.//td/text()').get().strip()
                    weight = float(weight[:weight.find('pounds')-1].strip())
                    return weight
            except:
                pass

        return None

    @staticmethod
    def getMemoryType(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Computer Memory Type":
                    memoryType = tr.xpath('.//td/text()').get().strip()
                    return memoryType
            except:
                pass

        return None

    @staticmethod
    def getAverageBatteryLife(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Average Battery Life (in hours)":
                    batteryLife = tr.xpath('.//td/text()').get().strip()
                    batteryLife = int(batteryLife[:batteryLife.find('hours')-1].strip())
                    return batteryLife
            except:
                pass

        return None

    @staticmethod
    def getProductDimensions(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Product Dimensions":
                    productDimensions = tr.xpath('.//td/text()').get().strip()
                    delim1 = productDimensions.find('x')-1
                    delim2 = productDimensions.find('x', productDimensions.find('x')+1)
                    productDimension_X = float(productDimensions[:delim1].strip())
                    productDimension_Y = float(productDimensions[delim1+3:delim2-1].strip())
                    productDimension_Z = float(productDimensions[delim2+1:productDimensions.find('inches')-1].strip())
                    return productDimension_X, productDimension_Y, productDimension_Z
            except:
                pass

        return None

    @staticmethod
    def getColor(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Color":
                    color = tr.xpath('.//td/text()').get().strip()
                    return color
            except:
                pass

        return None

    @staticmethod
    def getImage(downloadedImage):
        imageDownloaded = downloadedImage
        if(imageDownloaded):
            return imageDownloaded

    @staticmethod
    def getAvgRating(sel):
        try:
            rating = sel.xpath('//span[@id="acrPopover"]/@title').get()
            avgRating = float(rating[:rating.find('out of 5 stars') - 1].strip())
            return avgRating
        except:
            pass

        return None

    def spider_opened(self):
        print("\n\n\nSpider Opened\n\n\n")

        #import pymysql

        #conn = pymysql.connect(host="localhost",  # your host
        #                       user="alfred",  # username
        #                       passwd="mysqlmysql",  # password
        #                       db="test_db")  # name of the database

        # Create a Cursor object to execute queries.
        #cur = conn.cursor()
        #cur.execute("SELECT asin FROM test_products order by id")
        #asin_ids = list()
        #for row in cur.fetchall():
        #    asin_ids.append(row[0])

        #print("parsed asin_ids", asin_ids)
        #self.products_already_scrapped = asin_ids

        #cur.close()
        #conn.close()
        '''
        scrapedProducts = list()
        for xmlFile in os.listdir('product_xml_files'):
            xmlASIN = xmlFile[:xmlFile.find('.')]
            scrapedProducts.append(xmlASIN)

        self.product_scraped = scrapedProducts
        '''

    #@staticmethod
    #def getReviews(response):
    #    reviews = list()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ProductSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

'''
    @staticmethod
    def get_more_products(response):

        start = '/dp/'
        end = '/'
        product_links = []

        try:
            for tmp in response.xpath("//th[@class='comparison_image_title_cell comparable_item1']"):
                ref1 = tmp.xpath('.//a/@href').extract()[0]
                ref1 = "https://www.amazon.com" + ref1
                asin1 =ref1[ref1.find(start) + len(start):ref1.rfind(end)]
                p1 = (ref1, asin1)
                product_links.append(p1)
        except:
            pass

        try:
            for tmp in response.xpath("//th[@class='comparison_image_title_cell comparable_item2']"):
                ref2 = tmp.xpath('.//a/@href').extract()[0]
                ref2 = "https://www.amazon.com" + ref2
                asin2 = ref2[ref2.find(start) + len(start):ref2.rfind(end)]
                p2 = (ref2, asin2)
                product_links.append(p2)
        except:
            pass

        try:
            for tmp in response.xpath("//th[@class='comparison_image_title_cell comparable_item3']"):
                ref3 = tmp.xpath('.//a/@href').extract()[0]
                ref3 = "https://www.amazon.com" + ref3
                asin3 = ref3[ref3.find(start) + len(start):ref3.rfind(end)]
                p3 = (ref3, asin3)
                product_links.append(p3)
        except:
            pass

        try:
            for tmp in response.xpath("//th[@class='comparison_image_title_cell comparable_item4']"):
                ref4 = tmp.xpath('.//a/@href').extract()[0]
                ref4 = "https://www.amazon.com" + ref4
                asin4 = ref4[ref4.find(start) + len(start):ref4.rfind(end)]
                p4 = (ref4, asin4)
                product_links.append(p4)
        except:
            pass

        return product_links
    '''
'''
def run_spider(spider):
    def f(q):
        try:
            process = CrawlerProcess({
                'USER_AGENT': ua.random
            })
            deferred = process.crawl(spider)
            deferred.addBoth(lambda _: reactor.stop())
            reactor.run()
            q.put(None)
        except Exception as e:
            q.put(e)

    q = Queue()
    p = Process(target=f, args=(q,))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result

print('first run:')
run_spider(ProductSpider)


process = CrawlerProcess({
                'USER_AGENT': ua.random
            })
process.crawl(ProductSpider)
process.start()
'''
