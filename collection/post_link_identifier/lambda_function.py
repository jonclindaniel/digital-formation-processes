from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.remote_connection import LOGGER as seleniumLogger
from urllib3.connectionpool import log as urllibLogger
import re
import math
import json
import time
import logging
import boto3

# Set the threshold for selenium to WARNING
seleniumLogger.setLevel(logging.WARNING)
# Set the threshold for urllib3 to WARNING
urllibLogger.setLevel(logging.WARNING)

logging.disable(logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s -  %(levelname)s -  %(message)s')

# Prior to February 2, 2023, five cities had a different site design
# and this required a separate scraping workflow in the functions below.
# When this changed, we commented out these "ALT_CITIES" and replaced them with
# 'none' to collect all data in the same way moving forward
ALT_CITIES = {'none'} #{'atlanta', 'chicago', 'miami', 'minneapolis', 'raleigh'}

class element_has_text(object):
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, browser):
        elements = browser.find_elements(*self.locator)
        for e in elements:
            if e.text:
                return e
        return False


def launch_browser():
    path = '/opt/chromedriver/chromedriver'
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('user-agent='
                        + 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' \
                        + ' AppleWebKit/537.36 (KHTML, like Gecko)' \
                        + ' Chrome/87.0.4280.88' \
                        + ' Safari/537.36')
    options.add_argument("--no-sandbox")
    options.add_argument("window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--no-zygote")
    options.add_argument("--single-process")
    options.add_argument("--user-data-dir=/tmp/chromium")
    options.binary_location = '/opt/chromium/chrome'


    browser = webdriver.Chrome(service=Service(path),
                               options=options,
                               service_log_path='/tmp/chromedriver.log')

    return browser


def get_n_pages(browser, city):
    # Indicate # of posts in two ways
    # In Atlanta, Chicago, Miami, Minneapolis, Raleigh (before Feb. 2, 2023):
    # //span[contains(@class, 'total')] or //span[contains(@class, 'totalcount')]
    # Everywhere else:
    # //div[contains@class, 'result-count'] 
    # or //span[contains(@class, 'cl-page-number')]
    try:
        if city in ALT_CITIES:
            xpath = "//span[contains(@class, 'totalcount')]"
            wait = WebDriverWait(browser, 10)
            wait.until(element_has_text((By.XPATH, xpath)))

            n_posts = browser.find_elements(By.XPATH, xpath)[0].text
        else:
            xpath = "//span[contains(@class, 'cl-page-number')]"
            wait = WebDriverWait(browser, 10)
            count_tally = wait.until(element_has_text((By.XPATH, xpath))).text

            n_posts = re.findall('.+of >*([0-9]+,*[0-9]*)', count_tally)[0]

        # compute number of pages based on number of posts (120/page)
        n_pages = math.ceil(int(n_posts.replace(',', '')) / 120)

        logging.debug(
            'Got Total Page Count for {}. Total Pages: {}, Total Posts: {}' \
                .format(city, str(n_pages), str(n_posts)))

    except:
        # manually set n_pages to 1 if something goes wrong and log it
        n_pages = 1
        logging.debug(
            'Something went wrong identifying the # of posts for {}.'.format(
                city)
            + ' Setting number of pages to scrape to 1.')

    return n_pages


def get_pg_post_links(browser, city):
    pg_post_links = []
    if city in ALT_CITIES:
        xpath = "//a[contains(@class, 'result-title hdrlnk')]"
        wait = WebDriverWait(browser, 10)
        wait.until(element_has_text((By.XPATH, xpath)))

        a_posts = browser.find_elements(By.XPATH, xpath)
        for a in a_posts:
            pg_post_links.append(a.get_attribute('href'))

    else:
        xpath = "//a[contains(@class, 'cl-app-anchor text-only posting-title')]"
        wait = WebDriverWait(browser, 10)
        wait.until(element_has_text((By.XPATH, xpath)))

        a_posts = browser.find_elements(By.XPATH, xpath)
        for a in a_posts:
            pg_post_links.append(a.get_attribute('href'))

    logging.debug(
        'Got one Page of Post Links for {}, Number Post Links Gathered: {}' \
            .format(city, str(len(pg_post_links))))

    return pg_post_links


def click_next(browser, cur_page, n_pages, city):
    # If there are still additional pages, click to the next page
    if city in ALT_CITIES:
        wait = WebDriverWait(browser, 10)
        element = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT,
                                                        'next')))
        element.click()

    else:
        xpath = "//button[contains(@class, 'bd-button cl-next-page icon-only')]"
        wait = WebDriverWait(browser, 10)
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        element.click()

    logging.debug(
    'Clicked Next Button for {} on page {} of {} total pages.'.format(
                                                        city,
                                                        str(cur_page),
                                                        str(n_pages)))


def upload_batches_to_s3(lst,
                         cities,
                         batch_size=10,
                         s3_bucket='craigslist-post-links',
                         json_key='post_links'):
    '''
    Convert list into batches of size `batch_size` and upload them as separate
    JSON files to the specific S3 Bucket in S3. Each JSON will have a unique
    S3 Key, based on the Month, Day, Year, Hour, and City that it was collected
    (as well as the batch number): e.g. 01_25_2023/02/atlanta/00001.json for
    post links collected at 2:00am UTC on January 25th, 2023 in Atlanta as a
    part of the first batch of links (data collected from multiple cities will
    be denoted by "_" -- e.g. "boston_chicago").

    For batch_size=2 and lst=[1, 2, 3, 4], function will first upload
    the following JSON (as batch '00001') with `json_key` as the key:
    {'post_links': [1, 2]}
    
    Then, it will upload JSON (as batch '00002'):
    {'post_links': [3, 4]}
    '''
    # Instantiate S3 Client
    s3_client = boto3.client('s3')

    # Create a unique identifier for this set of link batches (by time 
    # and city links accessed); precision can match hourly run time:
    day = time.strftime("%Y_%m_%d")
    hour = time.strftime("%H")
    cities = '_'.join(cities)

    s3_key_list = []
    for i, p in enumerate(range(0, len(lst), batch_size), 1):
        # Generate batch of data based on post # `p`
        data = {json_key: lst[p:p + batch_size]}

        # Convert batch number `i` to a consistently-sized string: 1 -> '00001'
        batch_num = str(i).zfill(5)

        # Construct S3 Key from unique identifying elements + batch # "i+1"
        s3_key = '{}/{}/{}/{}.json'.format(day, hour, cities, batch_num)
        s3_key_list.append(s3_key)

        # Convert data to JSON string and write to S3 with S3 key name
        data_string = json.dumps(data)

        s3_client.put_object(Body=data_string,
                             Bucket=s3_bucket,
                             Key=s3_key)

        logging.debug('Uploaded {} to S3 Bucket {}'.format(s3_key, s3_bucket))

    return s3_key_list


def lambda_handler(event, context):
    browser = launch_browser()

    logging.debug('Launched Browser')

    city_links = event['city_links']

    logging.debug('City-level Craigslist Link(s) Successfully Loaded')

    post_links = []
    cities = []
    for l in city_links:
        # extract name of city from link
        city = re.findall('https://(.*).craigslist', l)[0]
        cities.append(city)
        logging.debug('Accessing {} data from {}...'.format(city, l))

        # Go to link
        browser.get(l)

        logging.debug('Successfully went to {}'.format(l))

        n_pages = get_n_pages(browser, city)
        for p in range(1, n_pages + 1):
            # get post links on page
            pg_post_links = get_pg_post_links(browser, city)
            post_links.extend(pg_post_links)

            if p < n_pages:
                click_next(browser, p, n_pages, city)

    logging.debug('Finished collecting a total of {} post_links'.format(
        str(len(post_links))))

    # once post links are identified, quit browser session
    browser.quit()
    logging.debug('Quit Browser Session')

    # upload batches of post links to S3 and return keys for further processing
    s3_key_list = upload_batches_to_s3(post_links,
                                       cities,
                                       batch_size=10,
                                       s3_bucket='craigslist-post-links',
                                       json_key='post_links')

    return s3_key_list