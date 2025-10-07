import csv, json
import requests
from threading import Lock
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

class PerfumesClub:
    def __init__(self, url:str, filename:str) -> None:
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en,en-IN;q=0.9,en-US;q=0.8,hi;q=0.7',
            'cookie': 'ai_user=z+wxr|2025-01-25T04:31:13.835Z; origenUsuario=Sesion=true&retargeting=criteo; _uetsid=3612a7f0dad511efb3b03ddc400b8ea9; _uetvid=3612d970dad511efa5e4ebc2f3ecce0d; PreferenciasCookies=PermiteAnaliticas=true&PermiteMarketing=true; _gcl_au=1.1.293894270.1737780263; _ga=GA1.1.728607699.1737780261; frzbt.user=%7B%22properties%22%3A%7B%22createdAt%22%3A1737780265485%2C%22userLogged%22%3A%220%22%2C%22language%22%3A%22pt%22%7D%2C%22anonymous_id%22%3A%22b01423ff-4ff6-470b-b567-7ccc8ee175cd%22%2C%22distinct_id%22%3A%22b01423ff-4ff6-470b-b567-7ccc8ee175cd%22%7D; frzbt.session=%7B%22session_id%22%3A%22f633d043-c30f-4c63-a07a-776826c128db%22%7D; _clck=1jqsrf1%7C2%7Cfsv%7C0%7C1851; _tt_enable_cookie=1; _ttp=NUU-tGwcc2IaiwI6vaP1fIb2Ebj.tt.1; cto_bundle=yxa_rF84bGFValJWS2ZoNVl5bkJCWWFIOUVDcjg5Y3lQNzFoOFNxYUhVWUVxJTJCZFlQODBPT1ZNdElLJTJGWE9xJTJGbGFaemJHJTJCNGxPZzBhVjR4THJOaE80bWh2JTJGNGhGTjhDTGJySkhpMTlHWGhubGxBaDdwYnIlMkJpaXVIY0JyeFklMkJGRiUyRkxPRWxVQWhnQyUyRkYySG9WV2JMZU5FY3lJRHROa3lNWm4wdXZtUDBvQ1FkRGE0bGVhVHg0UFlQSCUyQk5MUGM2QnlQT0NGTnUycXNsWVlsJTJGWmhCV2w5RVRGWmklMkZnJTNEJTNE; _clsk=fojw8k%7C1737780423361%7C2%7C1%7Co.clarity.ms%2Fcollect; ai_session=ykgsf|1737779476356|1737780482042; _ga_PE4RYHEMM6=GS1.1.1737780261.1.1.1737780551.60.0.0; origenUsuario=Sesion=true&retargeting=criteo',
            'priority': 'u=0, i',
            'referer': 'https://www.perfumesclub.pt/',
            'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
            }
        self.url = url
        self.base_url = "https://www.perfumesclub.pt/"
        self.file_name = filename
        self.products = set()
        self.all_product_data = []
        self.total_products = 0 
        self.lock = Lock()

    def extract_products_urls(self) -> list:
        page = 1
        prev_total_products = 0  #
        
        while True:
            paginated_url = f"{self.url}?pagina={page}"
            print(f"Fetching page {page}: {paginated_url}")
            response = requests.get(paginated_url, headers=self.headers)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                total_results = soup.select_one('#totalItems')
                if total_results:
                    self.total_products = int(total_results.get_text(strip=True))
                    print(f"Total perfumes found: {self.total_products}")
                
                # Collect product URLs on the current page
                product_links = soup.select('[id="ajaxPage"] [class*="productList"] [class="imageProductDouble"]')
                new_products = 0
                
                for product in product_links:
                    product_url = self.base_url + product.attrs['href']
                    if product_url not in self.products:
                        self.products.add(product_url)
                        new_products += 1

                print(f"Found {new_products} new products on page {page}. Total collected: {len(self.products)}")
                
                if new_products == 0 or len(self.products) == self.total_products:
                    print("No more pages or products left to scrape.")
                    break
                
                page += 1
            else:
                print(f"Error: Failed to fetch page {page}. Status code: {response.status_code}")
                break
       
    def extract_features(self, soup:str) -> dict:
        tags = soup.select('[class="tagsAgain active"] dt')
        descriptions = soup.select('[class="tagsAgain active"] dd')

        data = {}
        for tag, desc in zip(tags, descriptions):
            term = tag.get_text(strip=True) 
            if desc.find('a'):
                description = desc.find('a').get_text(strip=True)
            else:
                description = desc.get_text(strip=True)
            data[term]= description
        return data
    
    def extract_variants(self, soup:str) -> dict:
        try:
            variant = soup.select('#listPrices .no-gutters')
            variant_list =  []
            for index, i in enumerate(variant, start=1):
                size = i.select_one('.col-12 .tM1').get_text(strip=True)
                offer = i.select_one('.col-12 .hackOffers').get_text(strip=True)
                off = i.select_one('.col-12 .withDiscount').get_text(strip=True)
                mrp = i.select_one('.col-12 .contTachado').get_text(strip=True)
                price = i.select_one('.col-12 .contPrecioNuevo').get_text(strip=True)
                mrp = mrp.replace("€", "").strip()if mrp else ""
                price = price.replace("€", "").strip()if price else ""
                variant_list.append({f"variant_{index}_size": size, f"variant_{index}_offer": offer, f"variant_{index}_off": off, f"variant_{index}_mrp": mrp, f"variant_{index}_price": price})
            return variant_list
        except:
            return []

    def save_in_file(self,response_text:str,product_url:str) -> None:
        soup = BeautifulSoup(response_text, 'html.parser')
        try: brand = soup.select_one('.titleProduct a').get_text(strip=True)
        except : brand = ""
        try: product = soup.select_one('.titleProduct span').get_text(strip=True)
        except : product = ""
        try: sub_title = soup.select_one('h2[class="titleProduct"]').get_text(strip=True)
        except : sub_title = ""
        try:
            reviews = soup.select_one('.sepStars')
            if reviews:
                reviews = reviews.get_text(strip=True).replace("(", "").replace(")", "")
            else:
                reviews = 0
        except Exception as e:
            reviews = 0
        try : mrp = soup.select_one('.contTachado').get_text(strip=True).replace("€", "")
        except : mrp = ""
        try : price = soup.select_one('.contPrecioNuevo').get_text(strip=True).replace("€", "")
        except : price = ""
        try: desription = soup.select_one('[id="descriptionPFCPropio"]').get_text(strip=True)
        except : desription = ""
        if desription is None or desription == "":
            desription = soup.select_one('.specialContentBody').get_text(strip=True)
        try: howto = soup.select_one('[id="howto"]').get_text(strip=True)
        except : howto = ""
        try: recommendations = soup.select_one('[id="recommendations"]').get_text(strip=True)
        except : recommendations = ""
        try:
            variants =  self.extract_variants(soup=soup)
        except :
            variants = ''
        variant_data = {}
        if variants:
            for index, variant in enumerate(variants, start=1):
                for key, value in variant.items():
                    variant_data[f"variant_{index}_{key.split('_')[-1]}"] = value

        # try: desription = soup.select_one('[id="descriptionPFCPropio"]').get_text(strip=True)
        # except : desription = ""
        try:
            for ii in soup.select('[class="active"]'):
                ean = ii.select_one('dt').get_text()
                ean_value = ii.select_one('dd').get_text()
        except Exception as e:
            ean_value = ""
        try:
            images_list = {f"image_{index}": ii['data-zoom-image'] for index, ii in enumerate(soup.select('#gallery_01 a[class*="miniZ"]'), start=1)}
        except:
            images_list = {}


        features = self.extract_features(soup)
        data = {
        "Product_URL":product_url,
        "Brand": brand,
        "Product": product,
        "Sub_title": sub_title,
        "Reviews": reviews,
        "MRP": mrp,
        "Price": price,
        "Description": desription,
        "EAN":""if ean_value=="EAN" else ean_value,
        "How_to": howto,
        "Recommendations": recommendations,
        **features,
        **images_list,
        **variant_data
    }
        
        # csv_file = f'{self.file_name}.json'
        # with open(csv_file, 'w', encoding='utf-8') as json_file:
        #     json.dump(data, json_file, ensure_ascii=False, indent=4)
        with self.lock: 
            self.all_product_data.append(data) # Use lock for thread-safe file access
        #     file_exists = False
        #     try:
        #         with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
        #             file_exists = True
        #     except FileNotFoundError:
        #         pass

        #     with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
        #         writer = csv.DictWriter(f, fieldnames=data.keys())
        #         if not file_exists:
        #             writer.writeheader()
        #         writer.writerow(data)
            
    def save_all_data(self):
        # Save the accumulated product data into a single JSON file
        with open(f"{self.file_name}.json", 'w', encoding='utf-8') as json_file:
            json.dump(self.all_product_data, json_file, ensure_ascii=False, indent=4)

    def get_products_details(self, product_url):
        print("Product url :-",product_url)
        response = requests.get(product_url, headers=self.headers)
        if response.status_code == 200:
            return self.save_in_file(response.text, product_url)
        else:
            print(f"Error: Failed to fetch the product details. Status code: {response.status_code}")
            return None

    def process_visitors_concurrently(self):
        with ThreadPoolExecutor(max_workers=10) as executor:  
            futures = [executor.submit(self.get_products_details, product_url) for product_url in self.products]
            for future in as_completed(futures):
                try:
                    future.result()  
                except Exception as e:
                    print(f"Error processing Product: {e}")
        
if __name__ == "__main__":
    perfume_club = PerfumesClub(url= "https://www.perfumesclub.pt/pt/perfume/f/", filename="perfumes_3")
    urls = perfume_club.extract_products_urls()
    with open('product_urls.json', 'r') as ff:
        perfume_club.products = json.load(ff)
    perfume_club.process_visitors_concurrently()
    perfume_club.save_all_data()

    
   
        

