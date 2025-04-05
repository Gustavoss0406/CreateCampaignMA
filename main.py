import logging
import sys
import os
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from typing import List

# Detailed logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust as necessary
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# List of valid country codes (ISO format) for global targeting
GLOBAL_COUNTRIES = [
    "US", "CA", "GB", "DE", "FR", "BR", "IN", "MX", "IT",
    "ES", "NL", "SE", "NO", "DK", "FI", "CH", "JP", "KR"
]

# Valid publisher platforms for ad placement
PUBLISHER_PLATFORMS = ["facebook", "instagram", "audience_network", "messenger"]

def extract_fb_error(response: requests.Response) -> str:
    """
    Extracts the error message (error_user_msg) from the Facebook API response.
    """
    try:
        error_json = response.json()
        return error_json.get("error", {}).get("error_user_msg", "Unknown error communicating with the Facebook API")
    except Exception:
        return "Error processing the Facebook API response"

def get_page_id(token: str) -> str:
    """
    Gets the first available page ID using the provided token.
    If no page is found, raises HTTPException with status 533.
    """
    url = f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}"
    logging.debug(f"Fetching available pages: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Pages response: {data}")
        if "data" in data and len(data["data"]) > 0:
            page_id = data["data"][0]["id"]
            logging.debug(f"Selected page: {page_id}")
            return page_id
        else:
            raise HTTPException(status_code=533, detail="No page available for use")
    else:
        raise HTTPException(status_code=response.status_code, detail="Error fetching available pages")

def check_account_balance(account_id: str, token: str, fb_api_version: str, spend_cap: int):
    """
    Checks if the account has sufficient funds.
    For prepaid accounts, available balance = spend_cap - amount_spent.
    If the available balance is less than the required spend_cap, raises HTTPException with status 402.
    """
    url = f"https://graph.facebook.com/{fb_api_version}/act_{account_id}?fields=spend_cap,amount_spent,currency&access_token={token}"
    logging.debug(f"Checking account balance: {url}")
    response = requests.get(url)
    logging.debug(f"Balance check status code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Balance check response: {data}")
        if "spend_cap" in data and "amount_spent" in data:
            try:
                spend_cap_api = int(data["spend_cap"])
                amount_spent = int(data["amount_spent"])
            except Exception as e:
                logging.error("Error converting spend_cap or amount_spent to integer.", exc_info=True)
                raise HTTPException(status_code=402, detail="Insufficient funds to create campaign")
            available_balance = spend_cap_api - amount_spent
            logging.debug(f"Calculated available balance: {available_balance}")
            if available_balance < spend_cap:
                raise HTTPException(status_code=402, detail="Insufficient funds to create campaign")
        else:
            logging.error("Required fields not found in account response. Assuming insufficient funds.")
            raise HTTPException(status_code=402, detail="Insufficient funds to create campaign")
    else:
        logging.error("Error checking account balance.")
        raise HTTPException(status_code=response.status_code, detail="Error checking account balance")

# Exception handler for validation errors, returning a simple error message.
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    try:
        first_message = exc.errors()[0]["msg"]
    except Exception:
        first_message = "Validation error"
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": first_message}
    )

class CampaignRequest(BaseModel):
    account_id: str             # Facebook account ID
    token: str                  # 60-day token
    campaign_name: str = ""     # Campaign name
    objective: str = "OUTCOME_TRAFFIC"  # Campaign objective (default if not provided)
    content_type: str = ""      # Content type (carousel, single image, video)
    content: str = ""           # Content URL (for single image) or other content
    images: List[str] = []      # List of image URLs (for carousel)
    video: str = ""             # Video URL (if campaign is video)
    description: str = ""       # Campaign description (used in the ad message)
    keywords: str = ""          # Keywords (used as ad caption)
    budget: float = 0.0         # Total budget (in dollars, e.g., "$300.00")
    initial_date: str = ""      # Start date (e.g., "04/03/2025")
    final_date: str = ""        # End date (e.g., "04/04/2025")
    pricing_model: str = ""     # Pricing model (CPC, CPA, etc.)
    target_sex: str = ""        # Target gender (e.g., "Male", "Female", "All")
    target_age: int = 0         # Target age (if given as a single value)
    min_salary: float = 0.0     # Minimum salary
    max_salary: float = 0.0     # Maximum salary
    devices: List[str] = []     # Devices (sent but not used in targeting)

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        mapping = {
            "Brand Awareness": "OUTCOME_AWARENESS",
            "Leads": "OUTCOME_LEADS",
            "Sales": "OUTCOME_SALES",
            "Vendas": "OUTCOME_SALES"
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
            except Exception:
                raise ValueError("Invalid budget")
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
        raise HTTPException(status_code=400, detail="Error reading or parsing request body")
    
    fb_api_version = "v16.0"
    ad_account_id = data.account_id
    campaign_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/campaigns"
    
    # Calculate total budget in cents
    total_budget_cents = int(data.budget * 100)
    
    # Check account balance before proceeding
    check_account_balance(ad_account_id, data.token, fb_api_version, total_budget_cents)
    
    # --- Create Campaign ---
    campaign_payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",
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
        error_msg = extract_fb_error(campaign_response)
        logging.error("Error creating campaign via Facebook API", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error creating campaign: {error_msg}")
    
    # --- Calculate daily budget for Ad Set ---
    try:
        start_dt = datetime.strptime(data.initial_date, "%m/%d/%Y")
        end_dt = datetime.strptime(data.final_date, "%m/%d/%Y")
        num_days = (end_dt - start_dt).days
        if num_days <= 0:
            num_days = 1
        daily_budget = total_budget_cents // num_days
        if (end_dt - start_dt) < timedelta(hours=24):
            raise HTTPException(status_code=400, detail="Campaign duration must be at least 24 hours")
        ad_set_start = int(start_dt.timestamp())
        ad_set_end = int(end_dt.timestamp())
    except Exception as e:
        logging.warning("Error processing dates; using fallback values")
        ad_set_start = data.initial_date
        ad_set_end = data.final_date
        daily_budget = total_budget_cents  # fallback

    # --- Determine optimization goal based on objective ---
    if data.objective == "OUTCOME_AWARENESS":
        optimization_goal = "REACH"
    elif data.objective in ["OUTCOME_LEADS", "OUTCOME_SALES"]:
        optimization_goal = "LINK_CLICKS"
    else:
        optimization_goal = "REACH"
    
    # --- Create Ad Set ---
    if data.target_sex.lower() == "male":
        genders = [1]
    elif data.target_sex.lower() == "female":
        genders = [2]
    else:
        genders = []
    
    targeting_spec = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": genders,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": PUBLISHER_PLATFORMS
    }
    
    # Get page ID for DSA fields
    page_id = get_page_id(data.token)
    
    ad_set_payload = {
        "name": f"Ad Set for {data.campaign_name}",
        "campaign_id": campaign_id,
        "daily_budget": daily_budget,
        "billing_event": "IMPRESSIONS",
        "optimization_goal": optimization_goal,
        "bid_amount": 100,
        "targeting": targeting_spec,
        "start_time": ad_set_start,
        "end_time": ad_set_end,
        "dsa_beneficiary": page_id,
        "dsa_payor": page_id,
        "access_token": data.token
    }
    
    ad_set_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adsets"
    logging.debug(f"Ad Set payload: {ad_set_payload}")
    
    try:
        ad_set_response = requests.post(ad_set_url, json=ad_set_payload)
        logging.debug(f"Ad Set response status: {ad_set_response.status_code}")
        logging.debug(f"Ad Set response: {ad_set_response.text}")
        ad_set_response.raise_for_status()
        ad_set_result = ad_set_response.json()
        ad_set_id = ad_set_result.get("id")
        logging.info(f"Ad Set created successfully: {ad_set_result}")
    except requests.exceptions.HTTPError as e:
        error_msg = extract_fb_error(ad_set_response)
        logging.error("Error creating Ad Set via Facebook API", exc_info=True)
        # Rollback: delete campaign
        requests.delete(f"https://graph.facebook.com/{fb_api_version}/{campaign_id}?access_token={data.token}")
        raise HTTPException(status_code=400, detail=f"Error creating Ad Set: {error_msg}")
    
    # --- Create Ad Creative ---
    link_data = {
        "message": data.description,
        "link": data.content if data.content else "https://www.example.com",
        "picture": data.images[0] if data.images else ""
    }
    if data.keywords.lower().startswith("http"):
        link_data["caption"] = data.keywords
    
    ad_creative_payload = {
        "name": f"Ad Creative for {data.campaign_name}",
        "object_story_spec": {
            "page_id": page_id,
            "link_data": link_data
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
        error_msg = extract_fb_error(ad_creative_response)
        logging.error("Error creating Ad Creative via Facebook API", exc_info=True)
        # Rollback: delete campaign
        requests.delete(f"https://graph.facebook.com/{fb_api_version}/{campaign_id}?access_token={data.token}")
        raise HTTPException(status_code=400, detail=f"Error creating Ad Creative: {error_msg}")
    
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
        error_msg = extract_fb_error(ad_response)
        logging.error("Error creating Ad via Facebook API", exc_info=True)
        # Rollback: delete campaign (which should cascade delete ad set, ad creative, etc.)
        requests.delete(f"https://graph.facebook.com/{fb_api_version}/{campaign_id}?access_token={data.token}")
        raise HTTPException(status_code=400, detail=f"Error creating Ad: {error_msg}")
    
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
