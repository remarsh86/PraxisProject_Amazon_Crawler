import scrapy


class AmazonScrapItem(scrapy.Item):
    asin = scrapy.Field()
    itemWeight = scrapy.Field()
    avgRating = scrapy.Field()

    productTitle = scrapy.Field()

    processorSpeed = scrapy.Field()
    processorType = scrapy.Field()
    processorBrand = scrapy.Field()
    processorCount = scrapy.Field()

    ram = scrapy.Field()

    hardDriveType = scrapy.Field()
    hddSize = scrapy.Field()
    ssdSize = scrapy.Field()

    operatingSystem = scrapy.Field()

    graphicsCoprocessor = scrapy.Field()
    chipsetBrand = scrapy.Field()

    brandName = scrapy.Field()

    price = scrapy.Field()

    screenSize = scrapy.Field()
    maxScreenResolution_X = scrapy.Field()
    maxScreenResolution_Y = scrapy.Field()
    
    averageBatteryLife = scrapy.Field()

    productDimension_X = scrapy.Field()
    productDimension_Y = scrapy.Field()
    productDimension_Z = scrapy.Field()

    color = scrapy.Field()
    imagePath = scrapy.Field()



    '''
    review = scrapy.Field()
    series = scrapy.Field()
    formFactor = scrapy.Field()
    displayResolutionMax = scrapy.Field()
    processorSocket = scrapy.Field()
    hardDriveDescription = scrapy.Field()
    hardDriveInterface = scrapy.Field()
    graphicsCoProcessor = scrapy.Field()
    graphicsCardDescription = scrapy.Field()
    connectivityType = scrapy.Field()
    wirelessType = scrapy.Field()
    numberUSBPorts = scrapy.Field()
    numberHDMIPorts = scrapy.Field()
    numberEthernetPorts = scrapy.Field()
    batteryDescription = scrapy.Field()
    lithiumBatteryEnergyContent = scrapy.Field()
    '''
