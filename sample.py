from bs4 import BeautifulSoup as Bs
# from pandas import DataFrame as df
import requests
import logging
import base64
import urllib
import os
import sys
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
import multiprocessing
# from joblib import Parallel, delayed
# from functools import partial
import csv
from argparse import ArgumentParser
import cloudinary
import cloudinary.uploader
import cloudinary.api
import re
import time

CLOUDINARY_URL="cloudinary://633978742621558:Xj9RkbPCT1NajlH1BQCzVZVfy2k@ninekm"

cloudinary.config( 
  cloud_name = "ninekm", 
  api_key = "633978742621558", 
  api_secret = "Xj9RkbPCT1NajlH1BQCzVZVfy2k" 
)

parser = ArgumentParser()
parser.add_argument("--test", dest="test", default=False, action='store_true')
parser.add_argument("--log", default="INFO", dest='log')

args = parser.parse_args()

class Scrapper:
    URL = 'https://www.flipkart.com/search?q='
    PRODUCT_CLASS_DICT = {
        'name': '_3wU53n',
        'href_class': '_31qSD5',
        'rating': 'hGSR34 _2beYZw',
        'rating2': 'hGSR34 _1x2VEC',
        'rating3': 'hGSR34 _1nLEql',
        'specs': 'vFw0gD', 
        'full_specs': 'MocXoX',
        'breadcrumb': '_1joEet',
        'price': '_1vC4OE _3qQ9m1',
        'mrp': '_3auQ3N _2GcJzG',
        'short_desc': '_3cpW1u',
        'full_desc': '_38NXIU'
    }
    BOX_PRODUCT_CLASS_DICT = {
        'name': '_2cLu-l',  # <a> class
        'href_class': '_2cLu-l',
        'rating': 'hGSR34 _2beYZw',
        'rating2': 'hGSR34 _1x2VEC',
        'rating3': 'hGSR34 _1nLEql',
        'breadcrumb': '_1joEet',
        'specs': '_1rcHFq',  # <div> class
        'full_specs': 'MocXoX',
        'price': '_1vC4OE _3qQ9m1',
        'short_desc': '_3cpW1u',
        'full_desc': '_38NXIU'
    }

    LINK_CLASS = '_2cLu-l'  # This class is to read the product link and click on it to get more description
    IMAGES_CLASS_LIST = {
        'block': '_1CUCUJ _2uAjEK',
        'side_thumbnail_ul': 'keS6DZ LzhdeS',
        'side_thumbnail_li':
        '_4f8Q22',  # Iterate through list and click on it for bigger image
        'side_thumbnail_img':
        '_2_AcLJ',  # Read style atr get img from backgrou-image:(url)
        'main_img':
        '_2rDnao _3BTv9X _3iN4zu _1ov7-N'  # Read images from srcset atr (1x, 2x) split by ','
    }

    PRODUCT_DESCRIPTION = 'bhgxx2 col-12-12 _1y9a40 _3la3Fn _1zZOAc p'  # Read text

    CLOUDINARY_IMAGE_URL="https://res.cloudinary.com/ninekm/image/upload"

    def __init__(self, searchterm):
        loglevel = args.log
        numeric_level = getattr(logging, loglevel.upper(), None)
        logging.basicConfig(filename='logs', level=numeric_level)

        self.searchterm = searchterm
        self.url = self.URL + self.create_url(self.searchterm)
        self.num_cors = multiprocessing.cpu_count()
        self.create_csv_file()

    def create_url(self, searchterm):
        string_list = searchterm.split(' ')
        new_string = ''
        for i in string_list:
            new_string = new_string + i + '+'
        return new_string[:-1]

    def initialize(self):  # main Url validation
        logging.info('Checking Url: ' + self.url)
        try:
            response = requests.get(self.url)
            if response.status_code == 200:
                logging.info('Url is Valid, initiating scraping')
                print('Scraping initiated for search: ', self.searchterm)
                return self.get_number_of_results(response)
            else:
                logging.error(response.status_code)
                print('Request timed out, Poor connection.Try again.')        
        except ConnectionError:
            logging.error('Invalid Url or no connection')
            print('Request timed out, Poor connection.Try again.')
        except Exception:
            logging.error("Oops!", sys.exc_info()[1], "occured.")
            exit()

    def create_csv_file(self):
        try:           
            rowHeaders = ["name", "product_description", "product_full_description", 
            "specifications", "brand_name", "company_name",
            "weight", "sub_category", "parent_category", "family", "SKU", "MRP", "Barcode", "product_images", "images_storage_path"]
            self.file_csv = open(self.searchterm + '_data.csv', mode='w', encoding='utf-8')
            self.csv = csv.DictWriter(self.file_csv, fieldnames=rowHeaders)
            self.csv.writeheader()
        except Exception:
            logging.error("Oops!", sys.exc_info()[1], "occured.")
            exit()

    def get_number_of_results(self, response):
        raw_html = response.content
        soup = Bs(raw_html, 'html.parser')
        klass = '_2yAnYN'
        try:
            raw_results = soup.find('span', {'class': klass}).get_text()
            if raw_results is None:
                logging.error("No Results found for <h1> class: " + klass)
                exit()
            else:
                start = raw_results.index('of')
                end = raw_results.index('results')
                no_of_results = int(raw_results[start + 3:end - 1].replace(
                    ',', ''))
                logging.info('Number of results for ' + self.searchterm + ':' +
                             str(no_of_results))
                if no_of_results > 10000:
                    print('Too many' + '(' + str(no_of_results) +
                          ')results for ' + self.searchterm + '.\
 Please extend your search term.')
                    print(
                        'Do you still want to continue, it will take a lot of time.(Y/N)'
                    )
                    choice = input()
                    if choice == 'Y' or choice == 'y':
                        return self.get_max_page(soup)
                    elif choice == 'N' or choice == 'n':
                        exit()
                    else:
                        logging.error('invalid choice, exiting')
                        exit()
                else:
                    logging.debug('No of results: ' + str(no_of_results))
                    return self.get_max_page(soup)
        except AttributeError:
            logging.error(
                "screen format different for this search result, cant continue"
                + self.searchterm)
            return self.handle_different_screen_format()

    def get_max_page(self, soup):
        klass = '_2zg3yZ'
        try:
            raw_results = soup.find('div', {
                'class': klass
            }).select_one('span').get_text()
            start = raw_results.index('of')
            if args.test:
                no_of_pages = 1
            else:
                no_of_pages = int(raw_results[start + 3:].replace(' ', ''))
        except AttributeError:
            no_of_pages = 1
            logging.info('Only first page found')
        return self.create_page_urls(soup, no_of_pages)

    def create_page_urls(self, soup, no_of_pages):
        pages_url_list = list()
        for i in range(1, no_of_pages + 1):
            url = self.url + '&page=' + str(i)
            pages_url_list.append(url)
        return self.validate_page_urls(soup, pages_url_list)

    def validate_page_urls(self, soup, pages_url_list):
        return self.check_diplay_type(soup, pages_url_list)

    def check_diplay_type(self, soup, valid_page_url_list):
        # class = '_1HmYoV _35HD7C col-10-12' --> box format
        # _1HmYoV hCUpcT
        isValidDisplay = False
        displayType = ""
        try:
            for var in soup.find_all("div", class_='bhgxx2 col-12-12'):
                if var.find("a", class_= '_2cLu-l') is not None:
                    logging.info('Box type screen structure found')
                    isValidDisplay = True
                    displayType="box"
                    break
                elif var.find("div", class_= self.PRODUCT_CLASS_DICT['name']) is not None:
                    isValidDisplay = True
                    displayType="list"
                    break                    
        except AttributeError:
            logging.error('Wrong class name in check_display_type()')
        
        if isValidDisplay == True:
            self.get_product_info(soup, valid_page_url_list, displayType)
        else:
            logging.error('screen type cannot be recognized')

    def get_product_info(self, soup, valid_page_url_list, displayType):
        print("Processing data for: " + self.searchterm + ", total pages: " + str(len(valid_page_url_list)))
        if displayType == "box":
            print("page: 1" + " started at: " + time.ctime(time.time()))
            self.parallel_process_box_info(soup)
            print("page: 1" + " finished at: " + time.ctime(time.time()))
        else:
            print("page: 1" + " started at: " + time.ctime(time.time()))
            self.parallel_process_info(soup)
            print("page: 1" + " finished at: " + time.ctime(time.time()))
        if len(valid_page_url_list) > 1:                       
            for i in range(len(valid_page_url_list[1:])):
                print("page: " + str(i + 2) + " started at: " + time.ctime(time.time()))
                time.sleep(2)
                url = valid_page_url_list[i]
                response = requests.get(url)
                raw_html = response.content
                soup = Bs(raw_html, 'html.parser')
                if displayType == "box":
                    self.parallel_process_box_info(soup)
                else:
                    self.parallel_process_info(soup)
                print("page: " + str(i + 2) + " finished at: " + time.ctime(time.time()))   
    
    def parallel_process_box_info(self, soup):        
        box = 0
        logging.debug("Processing box type display")
        try:
            for var in soup.find_all("div", class_='bhgxx2 col-12-12'):

                if var.find("div", class_='_3O0U0u') is None:
                    continue
                                
                for product in var.find_all("div", class_='_3liAhj _1R0K0g'):

                    if args.test:
                        if box == 1:
                            continue
                        box += 1

                    self.processProductData(var, self.BOX_PRODUCT_CLASS_DICT)

            logging.debug('Scraping...please wait...')
        except AttributeError:
            logging.error('Class name is different')
        except:
            logging.error("Line: 286 - Oops!", sys.exc_info()[1], "occured.")

    def parallel_process_info(self, soup):
        box = 0
        logging.debug("Processing info")
        try:
            for var in soup.find_all("div", class_='bhgxx2 col-12-12'):

                if var.find("div", class_="_3SQWE6") is None:
                    logging.debug("Log-0: Skiping")
                    continue

                logging.debug("Log-1: Finding product thumbnail image")

                if args.test:
                    if box == 1:
                        continue
                    box += 1

                self.processProductData(var, self.PRODUCT_CLASS_DICT)

            logging.debug('Scraping...please wait...')
        except AttributeError:
            logging.error('Class name is different')
        except:
            logging.error("Line: 329 - Oops!", sys.exc_info()[1], "occured.")

    def processProductData(self, response, classList):
        name = ""
        desc = ""
        full_desc = ""
        image_urls = ""
        local_images_path = ""
        price = ""
        specs = ""
        category = ""
        sub_category = ""
        brand = ""
        if response.find(
                "a",
                class_=classList['href_class']) is not None:
            logging.debug("Processing data")
            try:
                product_link = response.find(
                    "a", class_=classList['href_class'])['href']
                href = "https://www.flipkart.com" + product_link
                logging.debug("Log-3: Clicking on product link " + href)

                # self.driver.get(href)

                logging.debug("Log-4: Waiting pageload")
                time.sleep(1)
                res = requests.get(href)
                # product_html = self.driver.page_source
                product_soup = Bs(res.content, 'html.parser')

                name = product_soup.find("h1", class_="_9E25nV").get_text()
                
                descContent = None
                if product_soup.find("div", class_=classList['short_desc']) is not None:
                    descContent = product_soup.find("div", class_=classList['short_desc'])
                elif product_soup.find("div", class_="_3u-uqB") is not None:
                    descContent = product_soup.find("div",class_="_3u-uqB")                
                if descContent is not None:
                    desc = descContent.prettify().replace('\n', '').replace('\xa0', ' ')

                fullDescContent = None
                if product_soup.find("div", class_=classList['full_desc']) is not None:
                    fullDescContent = product_soup.find_all("div", class_=classList['full_desc'])
                if fullDescContent is not None:
                    for fullContent in fullDescContent:
                        full_desc += fullContent.prettify().replace('\n', '').replace('\xa0', ' ')

                if product_soup.find("div", class_=classList['price']) is not None:
                    price = product_soup.find("div", class_=classList['price']).get_text()[1:].replace(',', '')

                if product_soup.find('div', class_=classList['full_specs']) is not None:
                    specContent = product_soup.find('div', class_=classList['full_specs'])
                    specs = specContent.prettify().replace('\n', '').replace('\xa0', ' ')

                if product_soup.find('div', class_=classList['breadcrumb']) is not None:
                    crumb = product_soup.find('div', class_=classList['breadcrumb'])
                    breadcrumbs = crumb.find_all("div", class_="_1HEvv0")
                    category = breadcrumbs[1].get_text()
                    for breadcrumb in breadcrumbs[2:(len(breadcrumbs) - 2)]:
                        if sub_category != "":
                            sub_category +=  ">" + breadcrumb.get_text()
                        else:
                            sub_category = breadcrumb.get_text()
                    brand = breadcrumbs[len(breadcrumbs) - 2].get_text()
                
                # local_images_path = "images/" + self.searchterm + "/" + name
                # if not os.path.exists(local_images_path):
                #     os.makedirs(local_images_path)
                imageList = product_soup.find_all("li", class_="_4f8Q22")
                for li in imageList:
                    time.sleep(0.5)
                    logging.debug("Log-5: Reading from LI ")
                    # 78, 312, 416, 832
                    # 78, 612,
                    bkImg = li.find("div", class_="_2_AcLJ")['style']
                    bkUrl = bkImg.split("url(")[1]
                    bkUrl = bkUrl.replace(")", "")
                    img_1 = bkUrl
                    img_2 = bkUrl.replace('/128/128/', '/416/416/')
                    img_3 = bkUrl.replace('/128/128/', '/832/832/')
                    if img_3 in image_urls:
                        continue
                    image_urls += img_3 + ", "
                    if "http" in img_3:
                        img_name = img_3[img_3.rindex('/') +
                                            1:img_3.rindex('?')]
                        logging.debug("Image Name: " + img_name)
                        subCat = sub_category.split(">")
                        customSubCat = subCat[len(subCat) - 1]
                        publicId = category + "/" + customSubCat + "/" + brand + "/" + img_name.replace('.jpeg', '').replace('.jpg', '')
                        publicId = publicId.lower().replace(" ", "_")
                        publicId = re.sub('[?&#\%<>]', '', publicId).replace('__', '_')
                        local_images_path += self.CLOUDINARY_IMAGE_URL + "/" + publicId + ", "
                        logging.debug(local_images_path)
                        self.upload_files(img_3, publicId)
            except Exception:
                logging.error("Line: 383 - Oops!", sys.exc_info()[1], "occured.")

        self.csv.writerow({
            "name": name,
            "product_description": desc,
            "product_full_description": full_desc,
            "specifications": specs,
            "brand_name": brand, 
            "company_name": '', 
            "weight": '', 
            "sub_category": sub_category, 
            "parent_category": category,
            "family": '', 
            "SKU": '',
            "MRP": price,
            "Barcode": '',
            "product_images": image_urls,
            "images_storage_path": local_images_path
        })

    def handle_different_screen_format(self):
        logging.error(
            'Screen format is different, this functionality will soon be incorporated'
        )

    def upload_files(self, imgUrl, publicId):  
        logging.debug("File uploading to cloudinary")
        async_option = {"async": True}
        response = cloudinary.uploader.upload(
            file=imgUrl,
            public_id=publicId,
            width=832,
            height=832,
            **async_option
        )

    def tearDown(self):
        logging.info("teardown")
        # Here driver.quit function is used to close chromedriver
        # self.driver.quit()
        # Here we also need to close Csv file which I generated above
        # self.csv.close()
