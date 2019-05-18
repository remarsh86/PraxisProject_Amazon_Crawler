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
from colour import Color


#ua = UserAgent()
class ProductSpider(scrapy.Spider):
    name = "product-scraper" #identifies the spider

    def __init__(self, *args, **kwargs):
        super(ProductSpider, self).__init__(*args, **kwargs)

    start_urls = []
    limit = 0




    for file in os.listdir("product_xml_files"):

         # if limit < 1000:
         #
         #     start_urls.append(
         #         "file://" + os.path.realpath("product_xml_files") + "/" + str(file))  # replace with your local path
         #
         # else:
         #     break
         # limit += 1
         #

         start_urls.append(
             "file://" + os.path.realpath("product_xml_files") + "/" + str(file))  # replace with your local path

    def parse(self, response):
        sel = Selector(text=response.body)

        product = AmazonScrapItem()
        product["asin"] = self.getASIN(sel)

        product["productTitle"] = self.getProductTitle(sel)

        product["screenSize"] = self.getScreenSize(sel,product["productTitle"])
        product["ram"] = self.getRAM(sel,product["productTitle"])
        product["brandName"] = self.getBrandName(sel, product['productTitle'])
        product["operatingSystem"] = self.getOperatingSystem(sel,product["productTitle"])

        product["itemWeight"] = self.getItemWeight(sel,product["productTitle"])

        product["price"] = self.getPrice(sel)

        product["processorSpeed"] = self.getProcessorSpeed(sel,product["productTitle"])
        product["processorType"] = self.getProcessorType(sel,product["productTitle"])
        product["processorBrand"] = self.getProcessorBrand(sel,product["processorType"],product["productTitle"])
        product["processorCount"] = self.getProcessorCount(sel)


        product["hardDriveType"] = self.getHardDriveType(sel,product["productTitle"])

        if product["hardDriveType"] == "SSD" :
            product["hddSize"] = 0
            product["ssdSize"] = self.getSsdSize(sel,product["productTitle"])
        elif product["hardDriveType"] == "HDD" :
            product["hddSize"] = self.getHddSize(sel,product["productTitle"])
            product["ssdSize"] = 0
        elif product["hardDriveType"] == "Hybrid" :
            product["hddSize"] = self.getHddSize(sel,product["productTitle"])
            product["ssdSize"] = self.getSsdSize(sel,product["productTitle"])
        else :
            product["hddSize"] = 0
            product["ssdSize"] = 0


        product["graphicsCoprocessor"] = self.getGraphicsCoprocessor(sel)
        product["chipsetBrand"] = self.getChipsetBrand(sel, product['graphicsCoprocessor'],product["processorBrand"])


        product["maxScreenResolution_X"] = self.getMaxScreenResolution_X(sel)
        product["maxScreenResolution_Y"] = self.getMaxScreenResolution_Y(sel)

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
    def getScreenSize(sel,productTitle):

        result = None
        #Search in Technical details 1
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Screen Size" or tr.xpath('.//th/span/text()').get().strip() == "Screen Size" :
                    screenSize = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.getScreenSizeFromString(screenSize)
                    if result is not None :
                        return result
            except:
                pass

        #Search in Technical details 1
        for div in sel.xpath('//div'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Screen Size" or tr.xpath('.//th/span/text()').get().strip() == "Screen Size" :
                    screenSize = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.getScreenSizeFromString(screenSize)
                    if result is not None :
                        return result
            except:
                pass

        #Search in Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip()
            result = ProductSpider.getScreenSizeFromString(bulletedItem)
            if result is not None :
                return result

        #Search in Product title
        if productTitle is not None :
            result =  ProductSpider.getScreenSizeFromString(productTitle)
            if result is not None :
                return result

        return None

    @staticmethod
    def getScreenSizeFromString(string) :
        string = string.lower().replace(" ","")

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}inch",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("inch")])

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}\"",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find('"')])

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}in",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find('in')])




    @staticmethod
    def getProcessorSpeed(sel,productTitle):

        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor":
                    processor = tr.xpath('.//td/text()').get().strip()
                    if "ghz" in processor.lower() :
                        processorSpeed = float(processor[:processor.find('GHz')].strip())       # GHz
                        return processorSpeed
            except:
                pass

        if productTitle is not None :
            result = ProductSpider.getProcessorSpeedFromString(productTitle)
            if result is not None and result != "None" and not result.isspace() :
                return result

        #Search in Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip()
            result = ProductSpider.getProcessorSpeedFromString(bulletedItem)
            if result is not None and result != "None" and not result.isspace() :
                return result

        return None

    @staticmethod
    def getProcessorSpeedFromString(string) :
        if string is not None :
            tokens = string.split(",")

            for token in tokens :
                #Intel ---------------
                modelMatch = re.findall("i[3|5|7]{1}-[0-9]{4}[h|q|u]{0,2}[0-9]{1}\.[0-9]{1}ghz",token.lower().replace(" ",""))
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 :
                    return modelMatch[0][modelMatch[0].find(".")-1:modelMatch[0].find("ghz")]
                #Intel ---------------
                modelMatch = re.findall("intelcorei[3|5|7]{1}[0-9]{0,4}[h|q|u]{0,2}[0-9]{1}\.[0-9]{1}ghz",token.lower().replace(" ",""))
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 :
                    return modelMatch[0][modelMatch[0].find(".")-1:modelMatch[0].find("ghz")]
                modelMatch = re.findall("intelcorei[3|5|7]{1}[0-9]{1}\.[0-9]{1}ghz",token.lower().replace(" ",""))
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 :
                    return modelMatch[0][modelMatch[0].find(".")-1:modelMatch[0].find("ghz")]
                modelMatch = re.findall("corei[3|5|7]{1}[0-9]{1}\.[0-9]{1}ghz",token.lower().replace(" ",""))
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 :
                    return modelMatch[0][modelMatch[0].find(".")-1:modelMatch[0].find("ghz")]
                modelMatch = re.findall("[0-9]{1}\.[0-9]{1,2}ghz",token.lower().replace(" ",""))
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 :
                    return modelMatch[0][:modelMatch[0].find("ghz")]
                modelMatch = re.findall("[0-9]{1}ghz",token.lower().replace(" ",""))
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 :
                    return modelMatch[0][:modelMatch[0].find("ghz")]

        return None


    @staticmethod
    def getProcessorType(sel,productTitle):

        processors = { 'intel' :["hd graphics","integrated","gma","520","620","500","intel","hd_graphics","graphics","hd","i7","i5","i3","celeron","pentium","x86","atom","xeon"]
        , "qualcom" : ["qualcom"]
        , "rockchip" : ["rockchip","rock"]
        ,'amd' : ["radeon","ryzen","amd","radion"]
        , 'nvidia' : ["tegra"]
        , 'arm mali' :["arm","mali"]
        , "mediatek" :["mediatek"]
        ,"dmx" :["dmx"]
        , "via" :["via"]

        }
        result = None

        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor":
                    processor = tr.xpath('.//td/text()').get().strip()
                    processorType = None
                    if processor is not None :
                        if "GHz" in processor :
                            result = processor[processor.find('GHz')+3:].strip()
                        else :
                            #In case it doesn't have the GHz part in the name.
                            result = processor

            except:
                pass

        if result is None or result.isdigit() or result.isspace():
            #print(productTitle)
            if productTitle is not None :
                result = ProductSpider.getProcessorTypeFromString(productTitle)
                if result is not None and not result.isdigit() and not result.isspace():
                    return result

            #Search in Features tab
            for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
                bulletedItem = sentence.get().strip()
                result = ProductSpider.getProcessorTypeFromString(bulletedItem)
                if result is not None and not result.isdigit() and not result.isspace() :
                    return result

        for intelKeyword in processors["intel"] :
            if result is not None and  intelKeyword in result.lower() :
                result = ProductSpider.getNormalizedIntelProcessor(result)

        if result is None or result == "None" or result.isspace():
            result = ProductSpider.getProcessorBrand(sel,None,productTitle)


        return result
    @staticmethod
    def getNormalizedIntelProcessor(string) :
        result = "Intel"
        match = re.findall("i[3|5|7]{1}",string)

        if len(match) > 0 :
            return result +" core "+match[0]

        if  "intel" not in string.lower() :
            return "Intel "+string


    @staticmethod
    def getProcessorTypeFromString(string) :
        #Try 1
        if string is not None :
            tokens = string.split(",")

            model = ""
            brand = ""

            for token in tokens :
                #Intel ---------------
                modelMatch = re.findall("i[3|5|7]{1}-[0-9]{4}[H|Q|U]{0,2}",token)
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 and model != "":
                    model ="Intel core "+ modelMatch[0]
                #Intel ---------------
                modelMatch = re.findall("intel core i[3|5|7]{1}[0-9]{0,4}[h|q|u]{0,2}",token.lower())
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 and model == "":
                    model = "Intel core "+modelMatch[0][modelMatch[0].find("intel core ")+11:]
                modelMatch = re.findall("intel core i[3|5|7]{1}",token.lower())
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 and model == "":
                    model = "Intel core "+modelMatch[0][modelMatch[0].find("intel core ")+11:]
                modelMatch = re.findall("core i[3|5|7]{1}",token.lower())
                #speedMatch = re.findall("[0-9]{1}\.[0-9]{0,1}ghz",token.replace(" ","").lower())
                if len(modelMatch) > 0 and model == "":
                    model = "Intel " + modelMatch[0]
            if model == "" :
                return None


            return model
        return None

    @staticmethod
    def getProcessorBrand(sel,processorType,productTitle):
        #This is much faster than parsing.
        processors = { 'intel' :["hd graphics","integrated","gma","520","620","500","intel","hd_graphics","graphics","hd","i7","i5","i3","celeron","pentium","x86","atom","xeon"]
        , "qualcom" : ["qualcom"]
        , "rockchip" : ["rockchip","rock"]
        ,'amd' : ["radeon","ryzen","amd","radion"]
        , 'nvidia' : ["tegra"]
        , 'arm mali' :["arm","mali"]
        , "mediatek" :["mediatek"]
        ,"dmx" :["dmx"]
        , "via" :["via"]

        }

        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Processor Brand":
                    processorBrand = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractPropertUsingKeywordsDict(processors,processorBrand)
                    if result is not None :
                        return result.title()
            except:
                pass
        #Extract from Processor type
        if processorType is not None :
            result = ProductSpider.extractPropertUsingKeywordsDict(processors,processorType)
            if result is not None :
                return result.title()
        #Extract from Product title
        if productTitle is not None :
            result = ProductSpider.extractPropertUsingKeywordsDict(processors,productTitle)
            if result is not None :
                return result.title()

        #Search in Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            if bulletedItem is not None :
                result = ProductSpider.extractPropertUsingKeywordsDict(processors,productTitle)
                if result is not None :
                    return result.title()


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
    def getRAM(sel,productTitle):
        possibleValues = [1,2,3,4,6,8,12,16,20,24,32,64,128]

        #Search in Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "RAM":
                    ram = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.getRamFromString("ram:"+ram)
                    if result is not None and result in possibleValues:
                        return result
            except:
                pass

        #Search in Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.getRamFromString(bulletedItem)
            if result is not None and result in possibleValues:
                return result

        #Search in Product title
        result = ProductSpider.getRamFromString(bulletedItem)
        if result is not None and result in possibleValues:
            return result

        return None

    @staticmethod
    def getRamFromString(string) :
        string = string.lower().replace(" ","")

        matches = re.findall("ram:[0-9]+",string)

        if len(matches) >0 :
            return int(matches[0][matches[0].find("ram:")+4:])
        return None



        matches = re.findall("[0-9]+gbram",string)

        if len(matches) >0 :
            return int(matches[0][:matches[0].find("gbram")])



        matches = re.findall("[0-9]+gbsdram",string)

        if len(matches) >0 :
            return int(matches[0][:matches[0].find("gbsdram")])

        matches = re.findall("[0-9]+gbddr",string)

        if len(matches) >0 :
            return int(matches[0][:matches[0].find("gbddr")])
        return None

    @staticmethod
    def getHardDriveType(sel,productTitle):

        isHDD = ProductSpider.isHdd(sel,productTitle)
        isSSD = ProductSpider.isSsd(sel,productTitle)
        isHybrid = isHDD and isSSD

        if isHybrid :
            return "Hybrid"
        if isHDD :
            return "HDD"
        if isSSD :
            return "SSD"


        return None
    @staticmethod
    def isHdd(sel,productTitle) :
        keywords = {"HDD":["hdd","rpm","mechanical"]}
        #Extract from Product title
        if productTitle is not None :
            result = ProductSpider.extractPropertUsingKeywordsDict(keywords, productTitle)
            if result is not None :
                return True

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.extractPropertUsingKeywordsDict(keywords, bulletedItem)
            if result is not None :
                return True

        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractPropertUsingKeywordsDict(keywords, hd)
                    if result is not None :
                        return True                                               # GB
            except:
                pass



        return False

    @staticmethod
    def isSsd(sel,productTitle) :
        keywords = {"SSD" :["ssd","flash","solidstate","emmc","nvme","pcie"]}
        #Extract from Product title
        if productTitle is not None :
            result = ProductSpider.extractPropertUsingKeywordsDict(keywords, productTitle)
            if result is not None :
                return True

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.extractPropertUsingKeywordsDict(keywords, bulletedItem)
            if result is not None :
                return True


        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip().replace(" ","").lower() == "solidstatedrive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractPropertUsingKeywordsDict(keywords, hd)
                    if result is not None :
                        return True                                               # GB
            except:
                pass
        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip().replace(" ","").lower() == "flashmemorysize":
                    hd = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractPropertUsingKeywordsDict(keywords, hd)
                    if result is not None :
                        return True                                               # GB
            except:
                pass
        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractPropertUsingKeywordsDict(keywords, hd)
                    if result is not None :
                        return True                                               # GB
            except:
                pass

        return False

    @staticmethod
    def getHddSize(sel,productTitle):

        #Extract from Proudct title
        if productTitle is not None :
            result = ProductSpider.getHDDSizeFromString(productTitle)
            if result is not None :
                return result

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.getHDDSizeFromString(bulletedItem)
            if result is not None :
                return result

        #Extract from Technical Details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip().replace(" ","")
                    result = ProductSpider.getHDDSizeFromString("harddrive"+hd)
                    if result is not None :
                        return result

            except:
                pass



        return None

    @staticmethod
    def getHDDSizeFromString(string) :

        string = string.lower().replace(" ","").replace(":","").replace("_","").replace("-","")

        string = re.sub("ddr[0-9]","",string)
        matches = re.findall("[0-9]+tbhdd",string)


        if len(matches) >0 :
            return float(matches[0][:matches[0].find("tbhdd")])*1000

        matches = re.findall("[0-9]+gbhdd",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("gbhdd")])

        matches = re.findall("memory[0-9]+",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("memory")+6:])


        matches = re.findall("hdd[0-9]+tb",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("hdd")+3:matches[0].find("tb")])*1000

        matches = re.findall("hdd[0-9]+gb",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("hdd")+3:matches[0].find("gb")])


        matches = re.findall("hdd[0-9]+",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("hdd")+3:])

        matches = re.findall("harddrive[0-9]+tb",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:matches[0].find("tb")])*1000

        matches = re.findall("harddrive[0-9]+gb",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:matches[0].find("gb")])

        matches = re.findall("harddrive[0-9]+",string)
        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:])



        return None


    @staticmethod
    def getSsdSize(sel,productTitle):



        #Extract from Proudct title
        if productTitle is not None :
            result = ProductSpider.getSSDSizeFromString(productTitle)
            if result is not None :
                return result

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.getSSDSizeFromString(bulletedItem)
            if result is not None :
                return result

        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip().replace(" ","").lower() == "solidstatedrive":
                    hd = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.getSSDSizeFromString("memory"+hd)
                    if result is not None :
                        return result                                            # GB
            except:
                pass
        #Extract from Technical Details 1
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Flash Memory Size":
                    hd = tr.xpath('.//td/text()').get().strip().replace(" ","")

                    result = ProductSpider.getSSDSizeFromString("memory"+hd)
                    if result is not None :
                        return result
            except:
                pass

        #Extract from Technical Details 2
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Hard Drive":
                    hd = tr.xpath('.//td/text()').get().strip().replace(" ","")
                    result = ProductSpider.getSSDSizeFromString("memory"+hd)
                    if result is not None :
                        return result

            except:
                pass


        return None
    @staticmethod
    def getSSDSizeFromString(string) :
        string = string.lower().replace(" ","").replace(":","").replace("_","").replace("-","")
        #This is usually right before the size of ssd
        string = re.sub("ddr[0-9]","",string)
        string = string.replace("pcie","").replace("nvme","")


        matches = re.findall("[0-9]+tbssd",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("tbssd")])*1000

        matches = re.findall("[0-9]+gbssd",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("gbssd")])


        matches = re.findall("emmc[0-9]+tb",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("emmc")+4:matches[0].find("tb")])*1000

        matches = re.findall("emmc[0-9]+gb",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("emmc")+4:matches[0].find("gb")])


        matches = re.findall("[0-9]+tbemmc",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("tbemmc")])*1000

        matches = re.findall("[0-9]+gbemmc",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("gbemmc")])

        matches = re.findall("[0-9]+emmc",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("emmc")])

        matches = re.findall("harddrive[0-9]+tbssd",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:matches[0].find("tbssd")])*1000

        matches = re.findall("harddrive[0-9]+gbssd",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:matches[0].find("gbssd")])

        matches = re.findall("harddrive[0-9]+ssd",string)
        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:matches[0].find("ssd")])

        matches = re.findall("harddrive[0-9]+",string)
        if len(matches) >0 :
            return float(matches[0][matches[0].find("harddrive")+9:])

        matches = re.findall("ssd[0-9]+",string)

        if len(matches) >0 :
            return float(matches[0][matches[0].find("ssd")+3:])

        matches = re.findall("emmc[0-9]+",string)


        return None


    @staticmethod
    def getOperatingSystem(sel,productTitle):
        operatingSystemsDict = {
        "windows 10" :["windows 10","window 10","win 10","w10"]
        ,"windows 8.1":["windows 8.1","window 8.1","win 8.1","w8.1"]
        ,"windows 8" :["windows 8","window 8","win 8","w8"]
        ,"windows 7":["windows 7","window 7","win 7","w7"]
        ,"linux" : ["linux","ubuntu"]
        ,"windows vista":["vista"]
        ,"windows xp" : ["windows xp","window xp","win xp","wxp"]
        ,"dos" :["dos"]
        ,"chrome" :["chrome"]
        ,"android" : ["android"]
        ,"windows" : ["pc","windows","window","win"]
        ,"unix" : ["unix"]
        ,"thin pro" :["thinpro","hp","thin pro"]
        ,"mac os" :["mac os","os x","ios","os"]
        }

        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Operating System":
                    os = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractPropertUsingKeywordsDict(operatingSystemsDict,os)
                    if result is not None :
                        return result.title()
            except:
                pass
        #Extract from Product title
        if productTitle is not None :
            result = ProductSpider.extractPropertUsingKeywordsDict(operatingSystemsDict,productTitle)
            if result is not None :
                return result.title()
        #Extract from Feature tabs
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.extractPropertUsingKeywordsDict(operatingSystemsDict,bulletedItem)
            if result is not None :
                return result.title()

        return "None"

    @staticmethod
    def getGraphicsCoprocessor(sel):
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
        chipsetBrand = ProductSpider.getChipsetBrand(sel, None, None)
        if chipsetBrand is not None :
            return chipsetBrand
        return None

    @staticmethod
    def getChipsetBrand(sel, gpu,cpu):
        brands = { 'amd' : ["radeon","ryzen","amd"]
        , 'nvidia' : ["geoforce","nvidia","gtx","rtx","mx","gx"]
        , 'arm mali' :["armmali"]
        , "mediatek" :["mediatek"]
        ,'intel' :["hd graphics","integrated","gma","520","620","500","intel","hd_graphics","graphics","hd","i7","i5","i3"]

        }
        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Chipset Brand":
                    cb = tr.xpath('.//td/text()').get().strip()
                    if cb is not None  :
                        for brand in brands :
                            for brand_keyword in brands[brand] :
                                if brand_keyword in cb.lower() :
                                    return brand.title()
            except:
                pass
        #Extract from GPU
        if gpu is not None :
            for brand in brands:
                for brand_keyword in brands[brand] :
                    if brand_keyword in gpu.lower() :
                        return brand.title()

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            if bulletedItem is not None :
                for brand in brands:
                    for brand_keyword in brands[brand] :
                        if brand_keyword in bulletedItem.lower() and brand_keyword != brand :
                            return brand.title()
        #If no GPU, and cpu is intel then GPU is intel
        if cpu is not None and cpu.lower() == "intel" :
            return "Intel"
        if cpu is not None and cpu.lower() == "nvidia" :
            return "Nvidia"
        if cpu is not None and cpu.lower() == "amd" :
            return "Amd"

        return None

#    <span class="a-size-base a-color-base">Screen Size</span>

    @staticmethod
    def getPrice(sel):
        price = sel.xpath('//span[@id="priceblock_ourprice"]/text()').get()
        if price is not None:
            return price.replace(',', '').replace('$','')
        return price

    @staticmethod
    def getBrandName(sel, productTitle):
        brandsList =["Dell", "HP", "Lenovo", "Acer", "Asus", "Apple", "Samsung", "MSI", "Alienware", "Razer", "Huawai", "LG",
                 "Hyundai", "Latitude","PANASONIC", "XPS", "jumper", "Notebook flexx", "Proscan","Google","Microsoft"]


        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Brand Name":
                    brandName = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.extractBrandNameFromString(brandsList,brandName)
                    if result is not None :
                        return result.title()
            except:
                pass

        #Extract from Product title
        if productTitle is not None :
            result = ProductSpider.extractBrandNameFromString(brandsList,productTitle)
            if result is not None :
                return result.title()

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.extractBrandNameFromString(brandsList,bulletedItem)
            if result is not None :
                return result

        return None

    @staticmethod
    def extractBrandNameFromString(brandList,string) :

        string = string.lower().replace(" ","")
        for brand in brandList :
            if brand.lower() in string :
                return brand.title()

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
    def getItemWeight(sel,productTitle):

        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Item Weight":
                    weight = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.getItemWeightFromString(weight)
                    if result is not None :
                        return result
                    if weight.isdigit() :
                        return float(weight)
            except:
                pass

        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            result = ProductSpider.getItemWeightFromString(bulletedItem)
            if result is not None :
                return result

        #Extract from Product title
        if productTitle is not None :
            result = ProductSpider.getItemWeightFromString(productTitle)
            if result is not None :
                return result
        #Not the best result, but that's worst case so that we dont lose the product
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Shipping Weight":
                    weight = tr.xpath('.//td/text()').get().strip()
                    result = ProductSpider.getItemWeightFromString(weight)
                    if result is not None :
                        return result
                    if weight.isdigit() :
                        return float(weight)
            except:
                pass

        return None

    @staticmethod
    def getItemWeightFromString(string) :
        string = string.lower().replace(" ","")

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}pounds",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("pounds")])

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}kg",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("kg")]) *2.2

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}kilograms",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("kilograms")]) *2.2

        matches = re.findall("[0-9]{1,2}\.{0,1}[0-9]{0,2}grams",string)

        if len(matches) >0 :
            return float(matches[0][:matches[0].find("grams")]) *2.2
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
        possibleFieldValues = ["productdimensions","packagedimensions","itemdimensions","dimensions"]

        #Extract from Technical details
        for tr in sel.xpath('//tr'):
            try:
                for possibleValue in possibleFieldValues :
                    if possibleValue in tr.xpath('.//th/text()').get().strip().replace(" ","").lower() :
                        productDimensions = tr.xpath('.//td/text()').get().strip().replace(" ","")
                        delim1 = productDimensions.find('x')
                        delim2 = productDimensions.find('x', productDimensions.find('x')+1)
                        productDimension_X = float(productDimensions[:delim1].strip())
                        productDimension_Y = float(productDimensions[delim1+1:delim2].strip())
                        productDimension_Z = float(productDimensions[delim2+1:productDimensions.find('inches')].strip())
                        if productDimension_X != 0 and productDimension_Y != 0 and productDimension_Z != 0 :
                            return productDimension_X, productDimension_Y, productDimension_Z
            except:
                pass
        #Extract from Features tab
        for sentence in sel.xpath('//span[@class="a-list-item"]/text()') :
            bulletedItem = sentence.get().strip().lower().replace(" ","")
            for possibleValue in possibleFieldValues :
                if possibleValue in bulletedItem :
                    productDimensions = bulletedItem
                    delim1 = productDimensions.find('x')
                    delim2 = productDimensions.find('x', productDimensions.find('x')+1)
                    productDimension_X = float(productDimensions[:delim1].strip())
                    productDimension_Y = float(productDimensions[delim1+1:delim2].strip())
                    productDimension_Z = float(productDimensions[delim2+1:productDimensions.find('inches')].strip())
                    if productDimension_X != 0 and productDimension_Y != 0 and productDimension_Z != 0 :
                        return productDimension_X, productDimension_Y, productDimension_Z

        return None


    @staticmethod
    def getColor(sel):
        for tr in sel.xpath('//tr'):
            try:
                if tr.xpath('.//th/text()').get().strip() == "Color":
                    color = tr.xpath('.//td/text()').get().strip()
                    if color == "Gray":
                        color = "Grey"
                    Color(color.replace(" ", ""))
                    return color.title()
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

    #Helper method
    @staticmethod
    def extractPropertUsingKeywordsDict(valuesToKeywordsDict,toExtractString) :
        toExtractString = toExtractString.replace("-","").replace(" ","").lower().replace("_","")
        for value in valuesToKeywordsDict :
            for keyword in valuesToKeywordsDict[value] :
                if keyword.lower() in toExtractString :
                    return value.title()
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
