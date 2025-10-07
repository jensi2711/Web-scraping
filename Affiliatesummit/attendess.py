import csv
import json
import time
from botasaurus.request import Request
from concurrent.futures import ThreadPoolExecutor
from utils import headers, extract_social_links, field_extractions

class Attendees():
    def __init__(self):
        self.url = "https://affiliatesummit.app.swapcard.com/api/graphql"
        self.all_attendees_ids = list()

    def fetch_attendees(self, end_cursor="") -> None:
        payload = {
            "operationName": "EventPeopleListViewConnectionQuery",
            "variables": {
            "viewId": "RXZlbnRWaWV3XzkyMDI5NQ==",
            "sort": {
                "field": "LAST_NAME"
            },
            "endCursor": end_cursor
            },
            "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "7f6aeac87634ef772c93d5b0b2e89c9e7ed810a19868180507be401b9ab18214"
            }
            }
        }
        
        response = Request().post(self.url,data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            data = response.json()["data"]["view"]["people"]
            print("Total Count:",data.get("totalCount"))
            self.all_attendees_ids.extend([attendee.get('id') for attendee in data['nodes']])
            if data.get('pageInfo') and data.get('pageInfo').get('hasNextPage') and data.get('pageInfo').get('endCursor'):
                end_cursor  = data.get('pageInfo').get('endCursor')
                payload['variables']['endCursor'] = end_cursor
                print("Fetching next page...")
                time.sleep(1) 
                self.fetch_attendees(end_cursor=end_cursor)
        else:
            print(f"Error fetching attendees: {response.status_code}")


    def save_data(self, data:dict) -> None:
        try:
            if data and isinstance(data, dict):
                firstName = data.get('firstName')
                lastName = data.get('lastName')
                photoUrl = data.get('photoUrl')
                jobTitle = data.get('jobTitle')
                organization = data.get('organization')
                biography = data.get('biography')
                email = data.get('email')
                websiteUrl = data.get('websiteUrl')
                mobilePhone = data.get('mobilePhone')
                landlinePhone = data.get('landlinePhone')
                social_media_data = data.get('socialNetworks')
                linkdin = extract_social_links("LINKEDIN",social_media_data)
                skype = extract_social_links("SKYPE",social_media_data)
                youtube = extract_social_links("YOUTUBE",social_media_data)
                instagram = extract_social_links("INSTAGRAM",social_media_data)
                twitter = extract_social_links("TWITTER",social_media_data)
                facebook = extract_social_links("FACEBOOK",social_media_data)
                pinterest = extract_social_links("PINTEREST",social_media_data)
                event_data = data.get('withEvent').get('fields')
                vertical = field_extractions('Vertical',event_data)
                ticket_type = field_extractions('Ticket Type',event_data)
                attendee_type = field_extractions('Attendee Type',event_data)
                business_model = field_extractions('Business Model',event_data)
                company_size = field_extractions('Company Size',event_data)
                influencer_marketing_buyer = field_extractions('Influencer Marketing Buyer',event_data)
                objective_for_attending = field_extractions('Objective For Attending',event_data)
                solutions_needed = field_extractions('Solutions Needed',event_data)
                data_row = [
                firstName, lastName, photoUrl, jobTitle, organization, biography, email, websiteUrl,mobilePhone,landlinePhone,linkdin,
                skype,youtube,instagram,twitter,facebook,pinterest,ticket_type,attendee_type,business_model,company_size,influencer_marketing_buyer,objective_for_attending,solutions_needed,vertical]
        
                csv_file = "affiliatesummit_speakers.csv"
                
                headers =  [
                "firstName", "lastName", "photoUrl", "jobTitle", "organization", "biography", "email", "websiteUrl","mobilePhone","landlinePhone","linkdin",
                "skype","youtube","instagram","twitter","facebook","pinterest","ticket_type","attendee_type","business_model","company_size","influencer_marketing_buyer","objective_for_attending","solutions_needed","vertical"]
                
                try:
                    file_exists = False
                    try:
                        with open(csv_file, "r"):
                            file_exists = True
                    except FileNotFoundError:
                        pass
                    
                    with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        if not file_exists:
                            writer.writerow(headers)
                        writer.writerow(data_row)
                    print(f"Data written to {csv_file} successfully.")
                except Exception as e:
                    print(f"Error writing to CSV: {e}")

            else:
                print("Invalid data provided.")
                return None
            
        except Exception as e:
            print(f"Error fetching attendees data: {e}")

    def fetchAttendees_data(self, attendees_id:str) -> dict:
        payload ={
        "operationName": "EventPersonDetailsQuery",
        "variables": {
            "skipMeetings": True,
            "withEvent": True,
            "personId": attendees_id,
            "userId": "",
            "eventId": "RXZlbnRfMjIyNjc2OQ=="
        },
        "extensions": {
            "persistedQuery": {
            "version": 1,
            "sha256Hash": "03e6ab3182b93582753b79d92ee01125bd74c7164986e7870be9dcad9080f048"
            }
        }
        }
        response = Request().post(self.url,data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            response_data =  response.json().get("data").get('person') 
            self.save_data(response_data)
        else:
            print(f"Error fetching attendees data: {response.status_code}")
            return None
        
    def process_attendees_with_threads(self, max_threads=5):
        print(f"Processing {len(self.all_attendees_ids)} attendees...")
        with ThreadPoolExecutor(max_threads) as executor:
            executor.map(self.fetchAttendees_data, self.all_attendees_ids)


if __name__ == "__main__":
    attendees = Attendees()
    attendees.fetch_attendees()
    attendees.process_attendees_with_threads(max_threads=5) 