import json
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
import requests
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config.settings import hubspot, aws, pipeline
from loaders.s3_loader import S3Loader

logging.basicConfig(level= getattr(logging, pipeline.LOG_LEVEL, logging.INFO),
                    format= "%(asctime)s | %(levelname)s | %(name)s | %(message)s")

logger = logging.getLogger(__name__)

HUBSPOT_API_BASE = "https://api.hubapi.com"
END_POINTS = "/crm/v3/objects/contacts/search"
PAGE_SIZE = 100
MAX_RETRIES = 3

CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "phone",
    "office",
    "student_status",
    "counsellor",
    "country",
    "lead_source",
    "country_of_passport",
    "interested_destination"
]

class HubspotExtractor:

    def __init__(self):
        self.api_key = hubspot.HUBSPOT_API_KEY
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type":  "application/json"
            }
        
        self.loader = S3Loader()
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # def _init_s3_client(self):
    #     import boto3

    #     s3 = boto3.client(
    #         "s3",
    #         aws_access_key_id=aws.AWS_ACCESS_KEY_ID,
    #         aws_secret_access_key=aws.AWS_SECRET_ACCESS_KEY,
    #         region_name=aws.AWS_REGION
    #     )
    #     return s3

    def _read_state(self)->Optional[str]:

        state = self.loader.read_state(source="hubspot")
        return state.get("last_extracted_at") if state else None

        
    #-----------------write state--------------------------------------------------#

    def _write_state(self, records_extracted:int, status:str = "Success"):

        self.loader.write_state(
            source="hubspot",
            run_id=self.run_id, 
            records_extracted=records_extracted,
            status= status
            )     
    
    def _build_payload(self, last_extracted_at: Optional[str] = None, after: Optional[str]= None) -> Dict[str, Any]:
        
        payload = {
            "properties": CONTACT_PROPERTIES,
            "limit": PAGE_SIZE,
            "sorts": [
                {
                "propertyName": "lastmodifieddate",
                "direction": "ASCENDING"
                  }
            ]
        }

        if last_extracted_at:
            dt = datetime.fromisoformat(last_extracted_at.replace("Z", "+00:00"))
            timestamp_ms = int(dt.timestamp() * 1000)

            payload["filterGroups"] = [
                {
                    "filters":[
                        {
                            "propertyName": "lastmodifieddate",
                            "operator": "GTE",
                            "value": str(timestamp_ms)
                        }
                    ]
                }
            ]

        if after:
            payload["after"] = after

        return payload

    @retry(
            stop= stop_after_attempt(MAX_RETRIES),
            wait= wait_exponential(multiplier=1, min=2, max=30),
            retry = retry_if_exception_type(requests.exceptions.RequestException),
            reraise = True
             )
    def _api_post(self, endpoint:str, payload: dict) -> List[Dict[str, Any]]:
        
        url = f"{HUBSPOT_API_BASE}{endpoint}"
        response = requests.post(url=url, headers= self.headers, json=payload, timeout=30)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-after", 10))
            logger.warning(f"Rate limited — waiting {retry_after}s")
            time.sleep(retry_after)
            response = requests.post(url=url, headers= self.headers, json=payload, timeout=30)

        response.raise_for_status()
        return response.json()
    
    def _extract_all_pages(self, last_extracted_at: Optional[str]) -> list[dict]:
        
        all_contacts = []
        after = None
        page_number = 1

        while True:

            logger.info(f"Fetching page numer {page_number}, contacts collected so far: {len(all_contacts)}")

            payload = self._build_payload(last_extracted_at= last_extracted_at, after= after)
            response = self._api_post(endpoint=END_POINTS, payload= payload)
            #print(response)

            contacts_page = response.get("results",{})
            
            if not contacts_page:
                logger.info("Empty page received — extraction complete")
                break

            contacts = [c for c in contacts_page]
            all_contacts.extend(contacts)

            paging = response.get("paging",{})
            next_page = paging.get("next",{})
            after = next_page.get("after",{})

            if not after:
                logger.info("No more Page - Extraction Complete")
                break

            page_number += 1

        return all_contacts
    
    def _upload_to_s3(self, contacts: List[dict]):
    
        return self.loader.upload_raw(data=contacts, source= "hubspot", entity="leads", run_id=self.run_id)
    
    
    def run(self) -> Dict:
        
        logger.info(f"Hubspot Extractor started, run id:{self.run_id}")

        result = {
            "source": "Hubspot",
            "run_id": self.run_id,
            "status": "Failed",
            "records_extracted": 0,
            "s3_path": None,
            "Error": None
        }

        try:

            last_extracted_at = self._read_state()
            contacts = self._extract_all_pages(last_extracted_at= last_extracted_at)
            logger.info(f"Total contacts extracted: {len(contacts)}")

            if not contacts:
                logger.info("No new/updated contacts found since last run")

                result["status"] = "Success"
                return result
            
            s3_path = self._upload_to_s3(contacts=contacts)

            self._write_state(records_extracted= len(contacts), status= "Success")

            result.update({
                "status": "Success",
                "records_extracted": len(contacts),
                "s3_path": s3_path
            })
            logger.info(f"Extraction complete — {len(contacts)} records → {s3_path}")

        except Exception as e:
            logger.error(f"Extraction Failed: {e}", exc_info= True)
            result["Error"] = str(e)
            self._write_state(records_extracted= 0, status= "Failed")

        return result
    

if __name__ == "__main__":

    extractor = HubspotExtractor()
    result = extractor.run()

    for key, value in result.items():
        logger.info(f"{key}: {value}")




