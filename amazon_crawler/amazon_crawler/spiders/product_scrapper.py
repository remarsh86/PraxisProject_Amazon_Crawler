import scrapy
from amazon_crawler.items import AmazonScrapItem
from scrapy import signals
from urllib import request
import re
import os
from amazon_crawler import proxy_manager as pm
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

    start_urls = []
    limit = 0




    for file in os.listdir("product_xml_files"):

         if limit < 7000:

             start_urls.append(
                 "file://" + os.path.realpath("product_xml_files") + "/" + str(file))  # replace with your local path

         else:
             break
         limit += 1
    #start_urls.append("file://" + os.path.realpath("product_xml_files") + "/B075TG56TT.xml")  # replace with your local path
         #Generates a dictionary and yields it after parsing as
    def parse(self, response):
        sel = Selector(text=response.body)

        product = AmazonScrapItem()
        product["asin"] = self.getASIN(sel)

        product["productTitle"] = self.getProductTitle(sel)

        product["processorSpeed"] = self.getProcessorSpeed(sel)
        product["processorType"] = self.getProcessorType(sel)
        product["processorBrand"] = self.getProcessorBrand(sel,product["processorType"])
        product["processorCount"] = self.getProcessorCount(sel)

        product["ram"] = self.getRAM(sel)

        product["hardDriveType"] = self.getHardDriveType(sel)
        product["hardDriveSize"] = self.getHardDriveSize(sel)

        product["operatingSystem"] = self.getOperatingSystem(sel)

        product["graphicsCoprocessor"] = self.getGraphicsCoprocessor(sel)
        product["chipsetBrand"] = self.getChipsetBrand(sel, product['graphicsCoprocessor'])


        product["brandName"] = self.getBrandName(sel, product['productTitle'])

        product["price"] = self.getPrice(sel)

        product["screenSize"] = self.getScreenSize(sel)
        product["maxScreenResolution_X"] = self.getMaxScreenResolution_X(sel)
        product["maxScreenResolution_Y"] = self.getMaxScreenResolution_Y(sel)

        product["itemWeight"] = self.getItemWeight(sel)

        product["memoryType"] = self.getMemoryType(sel)

        product["averageBatteryLife"] = self.getAverageBatteryLife(sel)



        prodDimensions = self.getProductDimensions(sel)
        if prodDimensions is None:
            product["productDimension_X"] = 0
            product["productDimension_Y"] = 0
            product["productDimension_Z"] = 0
        else:
            product["productDimension_X"] = prodDimensions[0]
            product["productDimension_Y"] = prodDimensions[1]
            product["productDimension_Z"] = prodDimensions[2]

        product["color"] = self.getColor(sel)
        product["imagePath"] = self.downloadImage(sel)
        product["avgRating"] = self.getAvgRating(sel)

        #print("Product", product)
        yield product

    @staticmethod
    def downloadImage(sel):
        path = sel.xpath('//img/@data-old-hires').get()
        return path

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
    def getScreenSize(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Screen Size" or tr.xpath('.//th/span/text()').get().strip() == "Screen Size" :
                    screenSize = tr.xpath('.//td/text()').get().strip()
                    screenSize = float(screenSize[:screenSize.find('inches')-1])    # inches
                    #Fallback 1
                    if screenSize is None :
                        screenSize = float(screenSize[:screenSize.find('in')-1])
                    return screenSize
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
                    processorType = None
                    if "GHz" in processor :
                        processorType = processor[processor.find('GHz')+3:].strip()
                    else :
                        #In case it doesn't have the GHz part in the name.
                        processorType = processor
                    return processorType.title()
            except:
                pass

        return None

    @staticmethod
    def getProcessorBrand(sel,processorType):
        #This is much faster than parsing.
        processors = ["nvidia","intel","amd","mediatek"]
        if processorType is not None :
            for processor in processors :
                if processor in processorType.lower() :
                    return processor.title()

        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor Brand":
                    processorBrand = tr.xpath('.//td/text()').get().strip()
                    for processor in processors :
                        if processor in processorBrand.lower() :
                            return processor.title()
                    if "radeon"  in processorBrand.lower() or "ryzen"  in processorBrand.lower() :
                        return "Amd"

                    return processorBrand.title()
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
    def getHardDriveType(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    if 'GB' in hd:
                        return hd[hd.find("GB")+2:]
                    elif 'TB' in hd:
                        return hd[hd.find("GB")+2:]
                    return hd                                                   # GB
            except:
                pass

        return None

    @staticmethod
    def getHardDriveSize(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    if 'GB' in hd:
                        return hd[:hd.find('GB')-1].strip()
                    elif 'TB' in hd:
                        return int(hd[:hd.find('TB')-1].strip())*1000 #GB

            except:
                pass
        #Fallback
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Flash Memory Size":
                    hd = tr.xpath('.//td/text()').get().strip()
                    if 'GB' in hd:
                        return hd[:hd.find('GB')-1].strip()
                    elif 'TB' in hd:
                        return int(hd[:hd.find('TB')-1].strip())*1000 #GB
                    else :
                        return hd[:hd.find('.')]
            except:
                pass
        return None

    @staticmethod
    def getOperatingSystem(sel):
        operatingSystems = ["windows 10","windows 8.1","windows 8","windows 7","linux","mac os","windows vista","windows xp","dos","chrome","android","windows"]
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Operating System":
                    os = tr.xpath('.//td/text()').get().strip()
                    for operatingSystem in operatingSystems:
                        if operatingSystem.lower().strip() in os.lower().strip():
                            return operatingSystem.title()
                        if "mac" in os.lower().strip() :
                            return "Mac Os"
                    return os.title()
            except:
                pass

        return None

    @staticmethod
    def getGraphicsCoprocessor(sel):
        brands = ['intel', 'amd', 'nvidia']
        #search first for the gpu in the specification table
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Graphics Coprocessor":
                    gc = tr.xpath('.//td/text()').get().strip()
                    return gc.title()
            except:
                pass

        #search then in the pruduct description
        for p in sel.xpath('//div[@id="productDescription"]/p/b'):
            try:
                if p.xpath('.//text()').get().strip() == "Graphics:":
                    gc = p.xpath(".//following-sibling::text()[1]").get().strip()
                    return gc.title()
            except:
                pass

        #keep searching the descriptipn box
        for br in sel.xpath('//div[@id="productDescription"]/p/text()[preceding-sibling::br and following-sibling::br]'):
            try:
                if 'graphics' in br.get().lower():
                    return br.get().strip().title()
            except:
                pass
        return None

    @staticmethod
    def getChipsetBrand(sel, gpu):
        brands = ['intel' , 'amd' , 'nvidia']
        for tr in sel.xpath('//tr'):
            try:
                # if tr.xpath('.//th/text()').get().strip() == "Chipset Brand":
                #     cb = tr.xpath('.//td/text()').get().strip()
                for brand in brands:
                    if brand in gpu.lower():
                        return brand.title()
            except:
                pass
        if gpu is not None :
            return gpu.title()
        return None

#    <span class="a-size-base a-color-base">Screen Size</span>

    @staticmethod
    def getPrice(sel):
        price = sel.xpath('//span[@id="priceblock_ourprice"]/text()').get()
        if price is not None:
            return price.replace(',', '').replace('$','')
        return price

    @staticmethod
    def getBrandName(sel, title):
        brands =["Dell", "HP", "Lenovo", "Acer", "Asus", "Apple", "Samsung", "MSI", "Alienware", "Razer", "Huawai", "LG",
                 "Hyundai", "Latitude","PANASONIC", "XPS", "jumper", "Notebook flexx", "Proscan"]
        for brand in brands:
            if brand.lower() in title.lower():
                if brand == "XPS":
                    return "Dell"
                else :
                    return brand.title()

        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Brand Name":
                    brandName = tr.xpath('.//td/text()').get().strip()
                    return brandName
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
