import logging
import sys
import os
import requests
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator

# Detailed logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def check_account_balance(account_id: str, token: str, fb_api_version: str, spend_cap: int):
    """
    Check if the account has sufficient balance.
    If the 'balance' field is missing or less than spend_cap, raises an HTTPException with status 402.
    """
    url = f"https://graph.facebook.com/{fb_api_version}/act_{account_id}?fields=balance&access_token={token}"
    logging.debug(f"Checking account balance: {url}")
    response = requests.get(url)
    logging.debug(f"Balance check status code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Balance check response: {data}")
        if "balance" in data:
            try:
                balance = int(data["balance"])
            except Exception as e:
                logging.error("Error converting balance to integer.", exc_info=True)
                raise HTTPException(status_code=402, detail="Insufficient funds to publish the campaign")
            logging.debug(f"Current account balance: {balance}")
            if balance < spend_cap:
                raise HTTPException(status_code=402, detail="Insufficient funds to publish the campaign")
        else:
            logging.error("Field 'balance' not found in account response. Assuming insufficient funds.")
            raise HTTPException(status_code=402, detail="Insufficient funds to publish the campaign")
    else:
        logging.error("Error checking account balance.")
        raise HTTPException(status_code=400, detail="Error checking account balance")

class CampaignRequest(BaseModel):
    account_id: str             # Facebook Ad Account ID
    token: str                  # 60-day Token
    campaign_name: str = ""     # Campaign Name
    objective: str = "OUTCOME_TRAFFIC"  # Campaign Objective (default)
    content_type: str = ""      # Content type (carousel, single image, video)
    content: str = ""           # Content URL (if applicable)
    images: list[str] = []      # List of image URLs (for carousel or single image)
    video: str = ""             # Video URL (if applicable)
    description: str = ""       # Campaign description (will be used as ad message)
    keywords: str = ""          # Keywords (used as caption in ad creative)
    budget: float = 0.0         # Total Budget (in dollars, e.g., "$300.00")
    initial_date: str = ""      # Start Date (e.g., "04/03/2025")
    final_date: str = ""        # End Date (e.g., "04/04/2025")
    pricing_model: str = ""     # Pricing model (CPC, CPA, etc.)
    target_sex: str = ""        # Target gender (e.g., "Male", "Female", "All")
    target_age: int = 0         # Target age (if given as a single value)
    min_salary: float = 0.0     # Minimum salary
    max_salary: float = 0.0     # Maximum salary
    devices: list[str] = []     # Devices (e.g., ["Smartphone", "Desktop"])

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        mapping = {
            "Brand Awareness": "OUTCOME_AWARENESS",
            "Sales": "OUTCOME_SALES",
            "Leads": "OUTCOME_LEADS"
        }
        if isinstance(v, str) and v in mapping:
            converted = mapping[v]
            logging.debug(f"Objective converted from '{v}' to '{converted}'")
            return converted
        return v

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                logging.debug(f"Budget converted: {parsed}")
                return parsed
            except Exception as e:
                raise ValueError(f"Invalid budget: {v}") from e
        return v

    @field_validator("min_salary", mode="before")
    def parse_min_salary(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                logging.debug(f"min_salary converted: {parsed}")
                return parsed
            except Exception as e:
                raise ValueError(f"Invalid min_salary: {v}") from e
        return v

    @field_validator("max_salary", mode="before")
    def parse_max_salary(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                logging.debug(f"max_salary converted: {parsed}")
                return parsed
            except Exception as e:
                raise ValueError(f"Invalid max_salary: {v}") from e
        return v

    @field_validator("images", mode="before")
    def clean_images(cls, v):
        if isinstance(v, list):
            cleaned = [s.strip().rstrip(";") if isinstance(s, str) else s for s in v]
            logging.debug(f"Cleaned image URLs: {cleaned}")
            return cleaned
        return v

@app.post("/create_campaign")
async def create_campaign(request: Request):
    try:
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")
        logging.debug(f"Raw request body: {body_str}")
        data_dict = await request.json()
        logging.debug(f"Parsed request body as JSON: {data_dict}")
        data = CampaignRequest(**data_dict)
        logging.debug(f"CampaignRequest parsed: {data}")
    except Exception as e:
        logging.exception("Error reading or parsing request body")
        raise HTTPException(status_code=400, detail=f"Error in request body: {str(e)}")

    fb_api_version = "v16.0"
    ad_account_id = data.account_id
    campaign_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/campaigns"
    
    # Convert budget to smallest currency unit (e.g., cents)
    spend_cap = int(data.budget * 100)
    
    # Check account balance
    check_account_balance(ad_account_id, data.token, fb_api_version, spend_cap)
    
    # Create campaign payload
    campaign_payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",         # Campaign activated automatically
        "spend_cap": spend_cap,
        "start_time": data.initial_date,
        "end_time": data.final_date,
        "content_type": data.content_type,
        "content": data.content,
        "images": data.images,
        "video": data.video,
        "description": data.description,
        "keywords": data.keywords,
        "pricing_model": data.pricing_model,
        "target_sex": data.target_sex,
        "target_age": data.target_age,
        "min_salary": data.min_salary,
        "max_salary": data.max_salary,
        "devices": data.devices,
        "access_token": data.token,
        "special_ad_categories": []
    }
    
    logging.debug(f"Campaign payload: {campaign_payload}")
    
    try:
        campaign_response = requests.post(campaign_url, json=campaign_payload)
        logging.debug(f"Campaign response status: {campaign_response.status_code}")
        logging.debug(f"Campaign response content: {campaign_response.text}")
        campaign_response.raise_for_status()
        campaign_result = campaign_response.json()
        campaign_id = campaign_result.get("id")
        logging.info(f"Campaign created successfully: {campaign_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Error creating campaign via Facebook API", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error creating campaign: {str(e)}")
    
    # --- Create Ad Set ---
    # For targeting, map target_sex to gender codes: 1 for male, 2 for female.
    if data.target_sex.lower() == "male":
        genders = [1]
    elif data.target_sex.lower() == "female":
        genders = [2]
    else:
        genders = []
    
    # Build a basic targeting spec
    targeting_spec = {
        "genders": genders,
        "age_min": data.target_age,  # Using target_age as both min and max (adjust as needed)
        "age_max": data.target_age,
        "publisher_platforms": data.devices  # This may need adjustment based on API docs
    }
    
    ad_set_payload = {
        "name": f"Ad Set for {data.campaign_name}",
        "campaign_id": campaign_id,
        "daily_budget": spend_cap,  # In a real scenario, daily budget is different from campaign cap
        "billing_event": "IMPRESSIONS",
        "optimization_goal": "REACH",
        "bid_amount": 100,  # Placeholder bid amount (in cents)
        "targeting": targeting_spec,
        "start_time": data.initial_date,
        "end_time": data.final_date,
        "access_token": data.token
    }
    
    ad_set_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adsets"
    logging.debug(f"Ad Set payload: {ad_set_payload}")
    
    try:
        ad_set_response = requests.post(ad_set_url, json=ad_set_payload)
        logging.debug(f"Ad Set response status: {ad_set_response.status_code}")
        logging.debug(f"Ad Set response content: {ad_set_response.text}")
        ad_set_response.raise_for_status()
        ad_set_result = ad_set_response.json()
        ad_set_id = ad_set_result.get("id")
        logging.info(f"Ad Set created successfully: {ad_set_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Error creating Ad Set via Facebook API", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error creating Ad Set: {str(e)}")
    
    # --- Create Ad Creative ---
    # Note: Replace "PAGE_ID" with your actual Facebook Page ID.
    ad_creative_payload = {
        "name": f"Ad Creative for {data.campaign_name}",
        "object_story_spec": {
            "page_id": "PAGE_ID",  
            "link_data": {
                "message": data.description,
                "link": data.content if data.content else "https://www.example.com",
                "caption": data.keywords,
                "picture": data.images[0] if data.images else ""
            }
        },
        "access_token": data.token
    }
    
    ad_creative_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adcreatives"
    logging.debug(f"Ad Creative payload: {ad_creative_payload}")
    
    try:
        ad_creative_response = requests.post(ad_creative_url, json=ad_creative_payload)
        logging.debug(f"Ad Creative response status: {ad_creative_response.status_code}")
        logging.debug(f"Ad Creative response content: {ad_creative_response.text}")
        ad_creative_response.raise_for_status()
        ad_creative_result = ad_creative_response.json()
        creative_id = ad_creative_result.get("id")
        logging.info(f"Ad Creative created successfully: {ad_creative_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Error creating Ad Creative via Facebook API", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error creating Ad Creative: {str(e)}")
    
    # --- Create Ad ---
    ad_payload = {
        "name": f"Ad for {data.campaign_name}",
        "adset_id": ad_set_id,
        "creative": {"creative_id": creative_id},
        "status": "ACTIVE",
        "access_token": data.token
    }
    
    ad_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/ads"
    logging.debug(f"Ad payload: {ad_payload}")
    
    try:
        ad_response = requests.post(ad_url, json=ad_payload)
        logging.debug(f"Ad response status: {ad_response.status_code}")
        logging.debug(f"Ad response content: {ad_response.text}")
        ad_response.raise_for_status()
        ad_result = ad_response.json()
        ad_id = ad_result.get("id")
        logging.info(f"Ad created successfully: {ad_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Error creating Ad via Facebook API", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error creating Ad: {str(e)}")
    
    # Construct a link to the campaign in Ads Manager for convenience
    campaign_link = f"https://www.facebook.com/adsmanager/manage/campaigns?act={ad_account_id}&campaign_ids={campaign_id}"
    
    return {
        "status": "success",
        "campaign_id": campaign_id,
        "ad_set_id": ad_set_id,
        "creative_id": creative_id,
        "ad_id": ad_id,
        "campaign_link": campaign_link,
        "facebook_response": {
            "campaign": campaign_result,
            "ad_set": ad_set_result,
            "ad_creative": ad_creative_result,
            "ad": ad_result
        }
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Starting app with uvicorn on host 0.0.0.0 and port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
