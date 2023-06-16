from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.remote_connection import LOGGER as seleniumLogger
from urllib3.connectionpool import log as urllibLogger
import re
import random
import requests
import time
import logging
import json
import csv
import boto3

# Set the threshold for selenium to WARNING
seleniumLogger.setLevel(logging.WARNING)
# Set the threshold for urllib3 to WARNING
urllibLogger.setLevel(logging.WARNING)

logging.disable(logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s -  %(levelname)s -  %(message)s')

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


def extract_text_data(browser, pl):
    data = {}

    # First, extract non-image data
    regex = 'https://(.+).craigslist.org.*/([a-z]+)/d/.+/([0-9]+).html'
    data['city'], data['category'], data['post_id'] = re.findall(regex, pl)[0]
    update_times = [t.get_attribute("datetime") 
                        for t in browser.find_elements(By.XPATH,
                            "//time[contains(@class, 'date timeago')]")]
    data['time_posted'] = update_times[0] # first posted
    data['last_updated'] = update_times[-1] # last update
    data['time_downloaded'] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    data['title'] = browser.find_element(By.XPATH,
                                "//h1[contains(@class, 'postingtitle')]"
                                ).text
    data['body_text'] = browser.find_element(By.XPATH,
                            "//section[contains(@id, 'postingbody')]"
                            ).text \
                            .replace('\n', ' ')
    data['latitude'] = browser.find_element(By.XPATH,
                                "//div[contains(@id, 'map')]") \
                                .get_attribute("data-latitude")
    data['longitude'] = browser.find_element(By.XPATH,
                            "//div[contains(@id, 'map')]") \
                            .get_attribute("data-longitude")

    logging.debug('Got all non-image data {} for {} in {}.'.format(
        data, pl, data['city']))
    
    return data


def find_images(browser):
    # identify images in post and return links to where they are stored
    try:
        # If there are images, need to identify figure class
        # to determine how to download the image(s)
        if browser.find_element(By.XPATH, "//figure") \
                    .get_attribute("class") == 'iw oneimage':
            image_links = [browser.find_element(By.XPATH,
                    "//img[contains(@alt, '1')]").get_attribute("src")]
        elif browser.find_element(By.XPATH, "//figure") \
                    .get_attribute("class") == 'iw multiimage':
            image_links = [i.get_attribute("href")
                            for i in browser.find_elements(By.XPATH,
                                "//a[contains(@class, 'thumb')]")]
        else:
            # should be covered by the above if images exist, but if not, ignore
            image_links = [] 
    except:
        # if exception, just assume no images included in post
        image_links = []
    
    logging.debug('Found {} image links.'.format(
        str(len(image_links))))
    
    return image_links


def upload_images_to_s3(s3_client, s3_bucket, image_links):
    # Uploads images to S3 and records S3 keys so that we can store them in
    # data dictionary for cross-referencing
    all_s3_keys = []
    for il in image_links:
        try:
            r = requests.get(il)
            s3_key = re.findall('https://images.craigslist.org/(.*)',
                                    il)[0]
            s3_key = 'images/' + re.sub(r'[^\w_. -]', '_', s3_key)
        
            s3_client.put_object(Body=r.content,
                                 Bucket=s3_bucket,
                                 Key=s3_key)

            logging.debug('Uploaded {} to S3 Bucket {}'.format(s3_key, 
                                                               s3_bucket))

            all_s3_keys.append(s3_key)
        except Exception as err:
            logging.debug('Failed on {} due to {}: {}'.format(il, 
                type(err).__name__, err))

        # Wait a bit before downloading next image
        time.sleep(random.uniform(0.5, 2))

    logging.debug('Downloaded images all images: {}.'.format(str(all_s3_keys)))

    return all_s3_keys


def lambda_handler(event, context):
    # launch browser and set implicit wait condition for all "finds"
    # in this function
    browser = launch_browser()
    browser.implicitly_wait(10)

    logging.debug('Launched Browser')

    # Get post links from S3 (batch key located at 0th position in payload)
    s3_client = boto3.client('s3')
    s3_input_bucket = 'craigslist-post-links'
    s3_input_key = event['Items'][0]
    s3_obj = s3_client.get_object(Bucket=s3_input_bucket, 
                                  Key=s3_input_key)
    
    logging.debug('Downloaded S3 Object with key: {}'.format(str(s3_input_key)))

    # Data Structured as: {'post_links': ['link1', 'link2', ...]}
    post_links = json.loads(s3_obj['Body'].read())['post_links']

    # Go through all post links, get data from posts, upload to output bucket
    full_data = []
    s3_output_bucket = 'craigslist-post-data'
    for pl in post_links:
        try:
            # try to go to post (if it is still there)
            browser.get(pl)

            # extract all text/metadata from post
            data = extract_text_data(browser, pl)

            # identify whether there are images in the post and how many
            image_links = find_images(browser)

            # upload any images to S3 and link s3 keys to post data
            if len(image_links) > 0:
                data['image_s3_keys'] = upload_images_to_s3(s3_client,
                                                            s3_output_bucket,
                                                            image_links)
            else:
                data['image_s3_keys'] = []

            logging.debug('Finished Collecting Post Data: {}'.format(str(data)))
            
            full_data.append(data)
        except Exception as err:
            # if post data can't be collected anymore, log it and go on to
            # next post in list
            print(err)
            logging.debug('Data Collection failed for {} due to {}'.format(pl,
                                                                           err))


    # Done with Selenium, so quit browser session
    browser.quit()
    logging.debug('Quit Browser Session')

    if len(full_data) > 0:
        # Create a unique identifier for this set of data (with unique ID drawn
        # from first post id in the batch); precision can match hourly run time:
        day = time.strftime("%Y_%m_%d")
        hour = time.strftime("%H")
        sample_data = full_data[0]
        city = sample_data['city']
        category = sample_data['category']
        uniq_id = sample_data['post_id']
        s3_output_key = '{}/{}/{}/{}/{}/{}.csv'.format('data',
                                                        category,
                                                        day,
                                                        hour,
                                                        city,
                                                        uniq_id)

        # Write data out to CSV and upload to S3
        with open('/tmp/data.csv', 'w', newline='') as file:
            field_names = list(sample_data.keys())
            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            for i in full_data:
                writer.writerow(i)
        
        # upload CSV to S3 at unique output key location
        s3_client.upload_file('/tmp/data.csv', s3_output_bucket, s3_output_key)

        # Mark status successful
        status_code = 200
        logging.debug('Wrote data to S3 with key {}'.format(s3_output_key))
    else:
        # Flag status as unsuccessful in collecting requested data
        status_code = 404
        logging.debug('Data collection failed for post links provided')


    return {'status_code': status_code}