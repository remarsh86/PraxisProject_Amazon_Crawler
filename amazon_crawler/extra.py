import os
crawled = []
for asin in os.listdir('product_xml_files'):
	asin = asin[:asin.find('.')]
	crawled.append(asin)

laps = open('laptops.txt', 'r').readlines()
laps = [l[:-1] for l in laps]
laps_asins = {la[la.find('/dp/')+4:]:la for la in laps}
remain_list = [laps_asins[el]+'\n' for el in laps_asins if el not in crawled]
newFile = open('remaining_laptops.txt', 'w')
for elem in remain_list:
	newFile.write(elem)

newFile.close()

