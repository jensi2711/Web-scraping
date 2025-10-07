import csv
import json
import time
from utils import headers
from botasaurus.request import Request
from concurrent.futures import ThreadPoolExecutor


class Exhibitors():
    def __init__(self):
        self.url = "https://affiliatesummit.app.swapcard.com/api/graphql"
        self.all_exhibitors_ids = list()

    def fetch_exhibitors(self, end_cursor="") -> None:
        payload = {
        "operationName": "EventExhibitorListViewConnectionQuery",
        "variables": {
        "withEvent": True,
        "viewId": "RXZlbnRWaWV3XzkyMDI5Mg==",
        "eventId": "RXZlbnRfMjA2MTE3OA==",
        "endCursor": end_cursor
        },
        "extensions": {
        "persistedQuery": {
            "version": 1,
            "sha256Hash": "a717703fa8924575e04c9968ef2f441781e9cb8e2d5ca62d9ca9742bd04eac93"
        }
        }
    }
        response = Request().post(self.url,data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            data = response.json()['data']['view']['exhibitors']
            self.all_exhibitors_ids.extend([exhibitor.get('id') for exhibitor in data['nodes']])
            with open('exhibitors.json', 'w') as file:
                json.dump(self.all_exhibitors_ids, file)
            if data.get('pageInfo') and data.get('pageInfo').get('hasNextPage') and data.get('pageInfo').get('endCursor'):
                end_cursor  = data.get('pageInfo').get('endCursor')
                payload['variables']['endCursor'] = end_cursor
                print("Fetching next page...")
                print(end_cursor)
                time.sleep(1) 
                self.fetch_exhibitors(end_cursor=end_cursor)
        else:
            print(f"Error fetching attendees: {response.status_code}")
    
    # def save_data(self, data:dict) -> None:
    #     print(data)
    #     try:
    #         if data and isinstance(data, dict):
    #             first_name = data.get('name', '')
    #             exhibitor_type = data.get('type', '')
    #             logo_url = data.get('logoUrl', '')
    #             website = data.get('websiteUrl', '')
    #             email = data.get('email', '')
    #             information = data.get('description', '')
    #             image_background = data.get('backgroundImageUrl', '')

    #             booths = data.get('withEvent', '').get('booths', '')
    #             location = ' '.join([location['name'] for location in booths])
    #             agency1 = data['withEvent']['fields']
    #             agency_ = [ag['values'] for ag in agency1]
    #             for ag_ in agency_:
    #                 listed_value = list()
    #                 for listed in ag_:
    #                     print(listed)
    #                     get_val = listed.get('text', '')
    #                     listed_value.append(get_val)
    #                 print(listed_value)
    #                 agency = ' ,'.join(listed_value)

    #             data_row = [
    #                 first_name, exhibitor_type, logo_url, website, email, information, image_background, location, agency 
    #             ]
    #             csv_file = "Exhibitors.csv"
    #             headers = [
    #                 'Name', 'Type', 'LogoURL', 'Website', 'Email', 'Description', 'Image_background', 'Location', 'Agency']
    #             try:
    #                 file_exists = False
    #                 try:
    #                     with open(csv_file, "r"):
    #                         file_exists = True
    #                 except FileNotFoundError:
    #                     pass
                    
    #                 with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
    #                     writer = csv.writer(file)
    #                     if not file_exists:
    #                         writer.writerow(headers)
    #                     writer.writerow(data_row)
    #                 print(f"Data written to {csv_file} successfully.")
    #             except Exception as e:
    #                 print(f"Error writing to CSV: {e}")

    #         else:
    #             print("Invalid data provided.")
    #             return None

    #     except Exception as e:
    #         print(f"Error fetching1 data: {e}")
    
    # def fetch_exhibitors_data(self, exhibitors_id:str) -> dict:
    #     payload = {
    #     "operationName": "EventExhibitorDetailsViewQuery",
    #     "variables": {
    #     "withEvent": True,
    #     "skipMeetings": False,
    #     "exhibitorId": "RXhoaWJpdG9yXzIwNjU0NTM=",
    #     "eventId": exhibitors_id
    #     },
    #     "extensions": {
    #     "persistedQuery": {
    #         "version": 1,
    #         "sha256Hash": "f9a01985a3222c9c7b98da9e6fa72422d06016251a48cee9f0849c5539fc4d3e"
    #     }}
    #     }

    #     print(payload)
    #     response = Request().post(self.url, headers=headers, data=json.dumps(payload))
    #     if response.status_code == 200:
    #         response_data =  response.json().get("data").get('exhibitor')
    #         print(response_data)
    #         self.save_data(response_data)
    #     else:
    #         print(f"Error fetching data: {response.status_code}")
    #         return None
    
    # def process_exhibitors_with_threads(self, max_threads=5):
    #     print(f"Processing {len(self.all_exhibitors_ids)} Exhibitors...")
    #     with ThreadPoolExecutor(max_threads) as executor:
    #         executor.map(self.fetch_exhibitors_data, self.all_exhibitors_ids)

if __name__ == "__main__":
    exhibitors = Exhibitors()
    exhibitors.fetch_exhibitors()
    # exhibitors.process_exhibitors_with_threads(max_threads=5) 