import cfscrape
from scrapy.selector import Selector
import time
import pandas as pd
from datetime import datetime
import requests
import sys
import logging
from logging.handlers import RotatingFileHandler
import traceback

rfh = RotatingFileHandler(
    filename='fgirl_ch.log',
    mode='a',
    maxBytes=20*1024*1024,
    backupCount=2,
    encoding=None,
    delay=0
  )

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s %(name)-2s {%(pathname)s:%(lineno)d} %(levelname)-4s %(message)s",
  datefmt="%y-%m-%d %H:%M:%S",
  handlers=[
    rfh
  ]
)
logger = logging.getLogger('main')

class FgirlScrapper:
    scraper = cfscrape.create_scraper()
    base_url = "https://www.en.fgirl.ch"
    unique_entry_set = set()
    # location__url_list = [
    #     'https://www.en.fgirl.ch/filles/geneve/', 'https://www.en.fgirl.ch/filles/vaud/',
    #     'https://www.en.fgirl.ch/filles/valais/', 'https://www.en.fgirl.ch/filles/neuchatel/',
    #     'https://www.en.fgirl.ch/filles/jura/', 'https://www.en.fgirl.ch/filles/fribourg/',
    #     'https://www.en.fgirl.ch/filles/berne/', 'https://www.en.fgirl.ch/filles/zurich/',
    #     'https://www.en.fgirl.ch/filles/suisse-alemanique/'
    # ]
    location__url_list = [
        'https://www.fgirl.ch/filles/suisse/transsexuel-travesti/', 
        'https://www.fgirl.ch/filles/suisse/sm/',
        'https://www.fgirl.ch/salons/'
    ]

    def getAllProfilesData(self):
        csv_file = f'fgirl_{datetime.now().strftime("%Y-%m-%d_%H_%M_%S")}.csv'
        # create empty csv
        columns = [
            "url", "category", "name", "title", "description",
            "telephone", "offers", "address", 'url_pictures'
        ]
        df = pd.DataFrame(list(), columns=columns)
        df.to_csv(csv_file, index=False)
        for loc_url in self.location__url_list:
            try:
                logger.info(f"Calling location url : {loc_url}")
                res = self.scraper.get(loc_url)
                if res.status_code == 200:
                    

                    # fetch first page data
                    # data = self.getPageAllProfileLink(res.text)
                    
                    # loop for the remaining pages
                    sel = Selector(text=res.text)
                    all_page_li = sel.xpath("//ul[contains(@class, 'pagination')]/li")
                    if len(all_page_li) > 3:
                        secnd_li = all_page_li[-2]
                        total_pages = secnd_li.css('a::text').get()
                    else:
                        secnd_li = all_page_li[-2]
                        total_pages = secnd_li.css('span::text').get()
                    logger.info(f"Total Pages Found : {total_pages}")
                    for i in range(1, int(total_pages)):
                        # profile url
                        logger.info(f'Page Number: {i}')
                        page_url = f"{loc_url}?page={i}"
                        logger.info(f'Page url : {page_url}')
                        try:
                            page_res = self.scraper.get(page_url)
                            if page_res.status_code == 200:
                                data = self.getPageAllProfileLinkData(page_res.text)
                                df_obj = pd.DataFrame(data)
                                df_obj.to_csv(csv_file, mode='a', header=False, index=False)
                            elif page_res.status_code == 404:
                                logger.error(f'Page Url Api request error code: {page_res.status_code}')    
                            else:
                                logger.error(f'Page Url Api request error code: {page_res.status_code}')
                                logger.error(f'Page Url Api request error response:  {page_res.text}')
                                sys.exit()
                        except requests.exceptions.HTTPError as errh:
                            logger.error(f'Page Url Http Error: {errh}')
                            sys.exit()
                        except requests.exceptions.ConnectionError as errc:
                            logger.error(f'Page Url Coonection Error: {errc}')
                            sys.exit()
                        except requests.exceptions.Timeout as errt:
                            logger.error(f'Page Url Timeout Error: {errt}')
                            sys.exit()
                        except requests.exceptions.RequestException as rerr:
                            logger.error(f'Page Url RequestException Error: {rerr}')
                            sys.exit()    
                        except Exception as ex:
                            logger.error(f'Page Url Other Exception Error: {ex}')
                            traceback.print_exception(type(ex), ex, ex.__traceback__)
                            sys.exit()         
                        # time.sleep(0.3)    
                else:
                    logger.error(f'getAllProfilesData Api request error code: {res.status_code}')
                    logger.error(f'getAllProfilesData Api request error response:  {res.text}')
            except requests.exceptions.HTTPError as errh:
                logger.error(f'Http Error: {errh}')
                sys.exit()
            except requests.exceptions.ConnectionError as errc:
                logger.error(f'Coonection Error: {errc}')
                sys.exit()
            except requests.exceptions.Timeout as errt:
                logger.error(f'Timeout Error: {errt}')
                sys.exit()
            except requests.exceptions.RequestException as rerr:
                logger.error(f'RequestException Error: {rerr}')
                sys.exit()    
            except Exception as ex:
                logger.error(f'AllFiles API URL Other Exception Error: {ex}')
                traceback.print_exception(type(ex), ex, ex.__traceback__)
                sys.exit()
            # time.sleep(0.3)           
    
    def getPageAllProfileLinkData(self, response):
        output = []
        sel = Selector(text=response)
        profile_divs = sel.xpath("//*[@id='profile-list']/div/div[2]/div[1]/div")
        for div in profile_divs:
            try:
                profile_path = div.xpath('div').css('a::attr(href)').extract_first()
                logger.info(f"getPageAllProfileLinkData Profile path : {profile_path}")
                profile_url = self.base_url + profile_path
                logger.info(f"getPageAllProfileLinkData Profile Url : {profile_url}")
                if profile_path not in self.unique_entry_set:
                    res = self.scraper.get(profile_url)
                    if res.status_code == 200:
                        # get profile data
                        profile_data_dict = self.getProfileData(res.text, profile_url)
                        output.append(profile_data_dict)
                        self.unique_entry_set.add(profile_path)
                    else:
                        logger.error(f'Profile API request error code: {res.status_code}')
                        logger.error(f'Profile API request error response:  {res.text}')
                        sys.exit()
            except requests.exceptions.HTTPError as errh:
                logger.error(f'Profile API URL Http Error: {errh}')
                sys.exit()
            except requests.exceptions.ConnectionError as errc:
                logger.error(f'Profile API URL Coonection Error: {errc}')
                sys.exit()
            except requests.exceptions.Timeout as errt:
                logger.error(f'Profile API URL Timeout Error: {errt}')
                sys.exit()
            except requests.exceptions.RequestException as rerr:
                logger.error(f'Profile API URL RequestException Error: {rerr}')
                sys.exit()    
            except Exception as ex:
                logger.error(f'Profile API URL Other Exception Error: {ex}')
                traceback.print_exception(type(ex), ex, ex.__traceback__)
                sys.exit()
            # time.sleep(0.3)
        return output           

    def getProfileData(self, response, profile_url):
        d = dict()
        sel = Selector(text=response)
        name = sel.xpath('//*[@id="body"]/div/div[3]/div[1]/div/div/div/h2/text()').extract_first()
        video_tag = sel.xpath('//div[contains(@class, "embed-responsive-4by3")]')
        if video_tag:
            category_xpath = '//*[@id="body"]/div/div[3]/div[2]/div[1]/div[3]/div/div/div/div[2]/text()'
            title_xpath = '//*[@id="body"]/div/div[3]/div[2]/div[1]/div[4]/div/div/div/div[2]/div/div[2]/text()'
            location_xpath = '//*[@id="body"]/div/div[3]/div[2]/div[3]/div[1]/div/div/p[1]/text()'
            desc_xapth = '//*[@id="body"]/div/div[3]/div[2]/div[4]/div/p/text()'
        else:
            category_xpath = '//*[@id="body"]/div/div[3]/div[2]/div[1]/div[2]/div/div/div/div[2]/text()'
            title_xpath = '//*[@id="body"]/div/div[3]/div[2]/div[1]/div[3]/div/div/div/div[2]/div/div[2]/text()'
            location_xpath = '//*[@id="body"]/div/div[3]/div[2]/div[3]/div[1]/div/div/p[1]/text()'
            desc_xapth = '//*[@id="body"]/div/div[3]/div[2]/div[4]/div/p/text()'

        category = sel.xpath(category_xpath).extract()
        location_raw = sel.xpath(location_xpath).extract()
        description_list = sel.xpath(desc_xapth).extract()
        offers = sel.xpath('//ul[contains(@class, ("services-list"))]/li/text()').extract()
        pictures_links = sel.xpath('//div[contains(@id, "escort-pictures-gallery")]/div/a').css('img::attr(src)').extract()
        selfies_link = sel.xpath('//div[contains(@id, "escort-selfies-gallery")]/div/a').css('img::attr(src)').extract()
        if selfies_link:
            images_link = [f"{self.base_url}{link}" for link in pictures_links + selfies_link ]
        else:
            images_link = [f"{self.base_url}{link}" for link in pictures_links]
        title_raw = sel.xpath(title_xpath).extract()

        d["url"] = profile_url
        d["category"] = ''.join([i.strip() for i in category if i.strip() != ''])
        d["name"] = name.strip()
        d["title"] = ''.join([i.strip() for i in title_raw if i.strip() != ''])
        d["description"] = ", ".join([text for text in description_list])
        d["telephone"] = self.getTelephone(profile_url)
        d["offers"] = '|'.join([i.strip() for i in offers if i.strip() != ''])
        d["address"] = ''.join([i.strip() for i in location_raw if i.strip() != ''])
        d["url_pictures"] = ', '.join(images_link)
        return d
    
    def getTelephone(self, profile_url):
        url = profile_url + 'call/'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        try:
            res = self.scraper.get(url, headers=headers)
            if res.status_code == 200:
                sel = Selector(text=res.text)
                telephone_raw = sel.xpath('//div[contains(@class, "modal-body")]/div/div/div/div').css('a::attr(href)').extract_first()
                telephone = telephone_raw[4:]
            else:
                logger.error(f'Telephone API request error code: {res.status_code}')
                logger.error(f'Telephone API request error response:  {res.text}')
                sys.exit()
        except requests.exceptions.HTTPError as errh:
            logger.error(f'Telephone API URL Http Error: {errh}')
            sys.exit()
        except requests.exceptions.ConnectionError as errc:
            logger.error(f'Telephone API URL Coonection Error: {errc}')
            sys.exit()
        except requests.exceptions.Timeout as errt:
            logger.error(f'Telephone API URL Timeout Error: {errt}')
            sys.exit()
        except requests.exceptions.RequestException as rerr:
            logger.error(f'Telephone API URL RequestException Error: {rerr}')
            sys.exit()    
        except Exception as ex:
            logger.error(f'Telephone API URL Other Exception Error: {ex}')
            traceback.print_exception(type(ex), ex, ex.__traceback__)
            sys.exit()
        return telephone

def main():
    obj = FgirlScrapper()
    obj.getAllProfilesData()

if __name__=='__main__':
  main()
