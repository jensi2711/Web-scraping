import os
import re
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC


load_dotenv()   

# Logging configuration
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

INPUT_FILE = os.getenv("INPUT_FILE")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
DRIVER_PATH = os.getenv("DRIVER_PATH")


class LoopnetScraperForSale:
    def __init__(self, url):
        self.url = url
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Cookie': '.AspNet.PendingCookies=l5GD7BKfZ1cyLLNGTuDu6nW5RXKfvDI2uPQFBEQgarTEK2lidvz1WqEWBdJJFHsQ8zOcD-EUWWZckbJJiJ03nXy3GPTaPcl2-nqWWg0m0-FD8Ae0SS-7uFhASIvMsUsIVcYiUly_SDX7AsaLPHEQjOXoFUZgK3kj8Sg4xkgpo-COQ97vJdXvzhtOGYkQpR_5FWneYDLgvpecPkZB170cAqQRWUBCuZ41Ry1WX4Iz8lpNdJaX6e7DO8Yw6_9GDKWNw6daBuIjgZblCN7row-qXs4nZD_dlVG1nnbsAF2dPS3lnmjABCKQdzWBXOc8Ll53__KQWKAhzIPVnWHPXC5ZLwIByEE; LNUniqueVisitor=ac63d3da-3b4a-4677-810d-6b973671b861; SessionFarm_GUID=b97442b0-4a63-4bb6-8614-ab29f7bfdd6b; bm_s=YAAQ1YwsMXPcPUmVAQAAs86hiQPG7DqUmLK20DB8+TbQwDpX2t2Qbh2HTFl2H+HZ6mJZgAYcsMJ7Yoi/hmOi9ZoJflbB/YrTItHOOeyJVGz1DHdn8WtQzADErYm2bjYqwSYhDZUV9Car9WSR2gBmB/lBNr6gcKzabWv1NIhEP+aLr5TJC3AwWi0/uxLxK3mGby9Omqk/AMLiQP5592TzRj+zNAmR4oz1kpjCUPv+lSdFwfuYaGLvfn4NT4V/LWpnEM/FhtLpf28fQPmlUyDBd1UsljzkbmdMixf8zxpjWVlQVkjb0yGKRAso5EgYaxjGp15EiHNOpc8o11UO3chSz5Oi/dj52ZDp/t5qPNElDECXGDieK7DzlirRnIICIO+SFlzY42YYpy00ERIPQGnpe/QOyNgo6DKEPVMBqqhp+pTtcbqz8GL/e9bDUyHkedqfC04VLdomJA==; bm_so=AD4611E58FD485D9D856907A6C4EA6CEBB41F407330F506A0A8C5FFE1DCC4920~YAAQ1YwsMXTcPUmVAQAAs86hiQL+zwSvJH0mllgyyymd6weaVIidkvLYgWI8hoKN5aqNkEHCzETf8p2OzdasgdilDvI6ZtYTFwAaAUxKgSOPt5TEtDACYZh9S+b1tKah528qiONlobjyVK/H2CQuhFcvNyGnntzAM0UJZN8O3qnVOuEi/9hCKE+3JdQJdMyn3uHg+g/d0hovBLR7RHdIwJdCRjJ5oAngWubfmSYwjlTAqQ8SYlC/gsZZG+qytrZHpgaI0j2TTrh8EN+Imrz7VBV1UU/c70TlcqHDV12r3bd++E92+U7qZ7Pe04ENnS2PguTh7ZJiUuSTLKd1cG5dbFwB+0QrLzTgEnAGm17KNfwvjSc9cTSiTilxx7jfXEz9d9jNyGS0t3cZhmikUssaOCRi4ZIARAXA0eUC54cK55ZOJBRbVu4Yw0dc5R+BsLDwHwHAfVj67nnCLDnkm7s=; bm_ss=ab8e18ef4e; gip=%7b%22Display%22%3a%22United+States%22%2c%22GeographyType%22%3a15%2c%22Address%22%3a%7b%22City%22%3a%22San+Francisco%22%2c%22PostalCode%22%3a%2294111%22%2c%22State%22%3a%22CA%22%2c%22CountryCode%22%3a%22US%22%2c%22Location%22%3a%7b%22y%22%3a37.7749295%2c%22x%22%3a-122.4194155%7d%2c%22ListingGeoCoded%22%3afalse%2c%22Country%22%3a%22US%22%2c%22ShapeTypeForL3%22%3a0%7d%2c%22Location%22%3a%7b%22y%22%3a21.15%2c%22x%22%3a72.79%7d%2c%22MatchType%22%3a0%2c%22SelectedCurrencyType%22%3a0%2c%22SelectedMeasurementConversion%22%3a0%2c%22lc%22%3a%22en-US%22%2c%22cy%22%3a%22USD%22%2c%22tgeo%22%3a%7b%22cc%22%3a%22IN%22%2c%22sc%22%3a%2209%22%2c%22ct%22%3a%22Surat%22%2c%22pc%22%3a%22395007%22%2c%22ly%22%3a21.15%2c%22lx%22%3a72.79%2c%22lc%22%3a%22en-US%22%2c%22cy%22%3a%22USD%22%7d%7d; lnwbprddc=04; __RequestVerificationToken=9K22IxRBEfvVXlW-SE2qZISgslhJ5XeMrxSXFHwpZi2S_8BAhhVp-WPxDuEt-NGc1gdoQIRC81wvcAPQcze9K38jgYI1'
        }
        self.all_data = list()
        self.error_log = list()
        self.url_list = list()

        try:
            print(DRIVER_PATH)
            options = Options()
            DRIVER_PATH and os.path.exists(DRIVER_PATH)
            service = Service(executable_path=DRIVER_PATH)
            self.driver = webdriver.Chrome(service=service, options=options)

        except Exception as e:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.maximize_window()

    def start_browser(self, url):
        """Starts the browser and opens the given URL."""
        if self.driver:
            self.driver.get(url)
        else:
            print("Driver is not Found")

    def quit_browser(self):
        """Quits the browser and closes all associated windows."""
        if self.driver:
            try:
                self.driver.quit()
                logging.info("Browser session successfully closed.")
            except Exception as e:
                logging.error(f"Error while quitting browser: {e}")

    def fetch_page(self, url):
        """Fetch the HTML content of a given URL."""
        try:
            response = requests.get(url, headers=self.headers)
            # response.raise_for_status()
            logging.info(f"Successfully fetched {url}")
            return response.text
        except requests.RequestException as e:
            error_msg = f"Failed to fetch URL {url}: {e}"
            logging.error(error_msg)
            self.error_log.append(error_msg)
            return None

    def parse_listing(self, url):
        """Extract data from a single listing page."""
        data = dict()
        html = self.fetch_page(url)
        print(f"Fetch data from : {url}")
        if not html:
            return
        soup = BeautifulSoup(html, 'html.parser')

        data['listing_url'] = url
        
        title = soup.select_one(".profile-hero-title")
        data['title'] = ', '.join(list(title.stripped_strings)) if title else None

        company_name = soup.select_one(".company-name")
        data["company_name"] = company_name.text if company_name else None

        phone_element = soup.select_one("span.phone-number")
        data["contact_phone"] = phone_element.text if phone_element else None

        contact_names = [' '.join(list(name.stripped_strings)) for name in soup.select(".contact-name")]
        for idx, name in enumerate(contact_names, start=1):
            data[f"contact_name_{idx}"] = name

        sub_titles_list = soup.select(".profile-hero-sub-title")
        sub_titles = [list(sub_title.stripped_strings) for sub_title in sub_titles_list]
        sub_titles = sub_titles[0] if sub_titles else []

        total_building_size, availability = "", ""

        for building_size in sub_titles:
            if 'SF' in building_size and '$' not in building_size:
                total_building_size = building_size  
            if any(word in building_size for word in ["Leased", "Vacant"]):  
                availability = building_size  
            if total_building_size and availability:
                break  
        
        if not total_building_size:
            table = list(soup.select_one('.listing-features').stripped_strings)
            for building_size in table:
                if 'SF' in building_size and '/' not in building_size:
                    total_building_size = building_size
                if any(word in building_size for word in ["Leased", "Vacant"]):
                    availability = building_size  
                if total_building_size and availability:
                    break  

        price_text = soup.find("td", string=re.compile(r"\bPrice\b\s*", re.IGNORECASE)) or soup.find("div", string=re.compile(r"\bPrice\b\s*", re.IGNORECASE))
        price = price_text.parent.select_one("span").text if price_text else ""

        price_sf = soup.find("td", string=re.compile(r"\bPrice\s+Per\s+SF\b", re.IGNORECASE)) or soup.find("div", string=re.compile(r"\bPrice\s+Per\s+SF\b", re.IGNORECASE))
        price_per_sf = list(price_sf.find_next_sibling().stripped_strings)[0] if price_sf else ""

        listing_date_selector = soup.select_one('.property-timestamp')
        listing_date = listing_date_selector.find('span', string='Date on Market: ').parent.text.split(':')[-1].strip()

        data["total_building_size"] = total_building_size
        data["availability"] = availability
        data["price"] = price
        data["price/SF"] = price_per_sf
        data['listing_date'] = listing_date
        self.all_data.append(data)


    def scrape_urls(self):
        """Scrape all URLs from LoopNet."""
        if not self.driver:
            return

        self.start_browser(self.url)

        while True:
            try:
                # Wait for listings to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.placard-pseudo a'))
                )
                # Extract listing URLs
                common_class = self.driver.find_elements(By.CSS_SELECTOR, '.placard-pseudo a')
                page_urls = [common.get_attribute('ng-href') for common in common_class]

                # Print each extracted URL
                for url in page_urls:
                    print("Scraped URL:", url)

                self.url_list.extend(page_urls)
                # Check for pagination button
                try:
                    next_page_element = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[aria-label="Go to next page"]'))
                    )
                    next_page_url = next_page_element.get_attribute('href')
                    print(next_page_url)
                    if next_page_url:
                        if 'sk' in self.driver.current_url:
                            current_page_url = self.driver.current_url.split('?')[1]
                            print(current_page_url)
                            next_page_url = f"{next_page_url}" + f'?{current_page_url}' if not next_page_url.startswith("http") or 'sk' not in next_page_url else next_page_url
                        else:
                            next_page_url = f"https://www.loopnet.com{next_page_url}" if not next_page_url.startswith("http") else next_page_url

                        print("Next Page:::", next_page_url)
                        self.driver.get(next_page_url)
                    else:
                        print("No next page URL found.")
                        break

                except Exception:
                    print("No more pages.")
                    logging.info("Pagination ended: No more pages.")
                    break

            except Exception as e:
                print(f"Error scraping page: {e}")
                logging.info(f"Scraping error: {e}")
                break
        self.quit_browser()

        # Scrape data from all listings using multi-threading
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.parse_listing, self.url_list)
        
        logging.info("Preaper For write......")
        self.save_to_csv()
        logging.info("Data Written into CSV File......")

    def save_to_csv(self):
        """Append data to a single CSV file."""
        print('**********************************')
        df = pd.DataFrame(self.all_data)
        if not df.empty:
            df.to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=False)
            logging.info(f"Data appended to {OUTPUT_FILE}. Total entries: {len(df)}")

    def save_error_log(self):
        """Save errors to a separate log file."""
        if self.error_log:
            with open("error_log.txt", "w") as file:
                file.writelines("\n".join(self.error_log))
            logging.info("Errors saved to error_log.txt")

    @staticmethod
    def read_urls_from_file(filename):
        """Read URLs from input file."""
        try:
            with open(filename, 'r') as file:
                return [line.strip() for line in file if line.strip()]
        except FileNotFoundError:
            logging.error(f"File {filename} not found.")
            return []

    @classmethod
    def start_scraping(cls, input_file):
        """Start the scraping process using multi-threading."""
        input_urls = cls.read_urls_from_file(input_file)
        if not input_urls:
            print(f"No URLs found in {input_file}. Exiting.")
            return

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda url: cls(url).scrape_urls(), input_urls)


if __name__ == "__main__":
    LoopnetScraperForSale.start_scraping(INPUT_FILE)