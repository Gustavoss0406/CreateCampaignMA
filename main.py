import logging
import sys
import os
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, ValidationInfo
from typing import List

# Configuração detalhada do logging
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()
logging.debug("Aplicação FastAPI iniciada.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Países válidos e plataformas
GLOBAL_COUNTRIES = [
    "US", "CA", "GB", "DE", "FR", "BR", "IN", "MX", "IT",
    "ES", "NL", "SE", "NO", "DK", "FI", "CH", "JP", "KR"
]
PUBLISHER_PLATFORMS = ["facebook", "instagram", "audience_network", "messenger"]

def extract_fb_error(response: requests.Response) -> str:
    try:
        err = response.json()
        return err.get("error", {}).get("error_user_msg", err.get("error", {}).get("message", "Erro desconhecido"))
    except:
        return "Erro processando resposta do Facebook"

def get_page_id(token: str) -> str:
    url = f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}"
    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail="Erro ao buscar páginas")
    data = r.json().get("data", [])
    if not data:
        raise HTTPException(status_code=533, detail="Nenhuma página disponível")
    return data[0]["id"]

def check_account_balance(account_id: str, token: str, fb_api_version: str, spend_cap: int):
    url = f"https://graph.facebook.com/{fb_api_version}/act_{account_id}?fields=spend_cap,amount_spent&access_token={token}"
    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail="Erro ao verificar saldo")
    d = r.json()
    cap = int(d.get("spend_cap", 0))
    spent = int(d.get("amount_spent", 0))
    if cap - spent < spend_cap:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg", "Erro de validação")
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": msg})

class CampaignRequest(BaseModel):
    account_id: str
    token: str
    campaign_name: str = ""
    objective: str = "OUTCOME_TRAFFIC"
    content: str = ""
    description: str = ""
    keywords: str = ""
    budget: float = 0.0
    initial_date: str = ""
    final_date: str = ""
    pricing_model: str = ""
    target_sex: str = ""
    target_age: int = 0
    min_salary: float = 0.0
    max_salary: float = 0.0
    devices: List[str] = []

    single_image: str = Field(default="", alias="Single Image")
    image: str = ""
    carrossel: List[str] = []
    video: str = Field(default="", alias="video")  # alias em minúsculo

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        mapping = {
            "Vendas": "OUTCOME_SALES",
            "Promover site/app": "OUTCOME_TRAFFIC",
            "Leads": "OUTCOME_LEADS",
            "Alcance de marca": "OUTCOME_AWARENESS"
        }
        return mapping.get(v, v)

    @field_validator("budget", "min_salary", "max_salary", mode="before")
    def parse_amounts(cls, v, info: ValidationInfo):
        defaults = {"budget": 0.0, "min_salary": 2000.0, "max_salary": 20000.0}
        if isinstance(v, str):
            if not v.strip():
                return defaults[info.field_name]
            clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                return float(clean)
            except ValueError:
                raise ValueError(f"{info.field_name} inválido: {v}")
        if v is None:
            return defaults[info.field_name]
        return v

@app.post("/create_campaign")
async def create_campaign(request: Request):
    data = CampaignRequest(**(await request.json()))
    fb_api = "v16.0"
    acct = data.account_id
    token = data.token

    # 1) Verifica saldo
    total_cents = int(data.budget * 100)
    check_account_balance(acct, token, fb_api, total_cents)

    # 2) Cria campanha
    try:
        camp_resp = requests.post(
            f"https://graph.facebook.com/{fb_api}/act_{acct}/campaigns",
            json={
                "name": data.campaign_name,
                "objective": data.objective,
                "status": "ACTIVE",
                "access_token": token
            }
        )
        camp_resp.raise_for_status()
        camp_id = camp_resp.json().get("id")
    except requests.exceptions.HTTPError:
        err = extract_fb_error(camp_resp)
        raise HTTPException(status_code=400, detail=f"Erro ao criar campanha: {err}")

    # 3) Datas e orçamento diário
    try:
        sd = datetime.strptime(data.initial_date, "%m/%d/%Y")
        ed = datetime.strptime(data.final_date, "%m/%d/%Y")
        days = max((ed - sd).days, 1)
        daily = total_cents // days
        if daily < 576:
            raise HTTPException(status_code=400, detail="O valor diário deve ser maior que $5.76")
        if (ed - sd) < timedelta(hours=24):
            raise HTTPException(status_code=400, detail="Duração mínima de 24h")
        start_ts, end_ts = int(sd.timestamp()), int(ed.timestamp())
    except HTTPException:
        raise
    except:
        daily, start_ts, end_ts = total_cents, data.initial_date, data.final_date

    # 4) Optimization goal
    optimization_goal = (
        "IMPRESSIONS"
        if data.objective == "OUTCOME_AWARENESS"
        else "LINK_CLICKS"
    )

    # 5) Targeting spec
    genders = (
        []
        if data.target_sex.lower() == "all"
        else [1]
        if data.target_sex.lower() == "male"
        else [2]
    )
    targeting = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": genders,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": PUBLISHER_PLATFORMS
    }

    # 6) Upload de vídeo na Página
    page_id = get_page_id(token)
    video_id = None
    if data.video.strip():
        logging.debug(f"Upload de vídeo para a Page {page_id}")
        upload_resp = requests.post(
            f"https://graph.facebook.com/{fb_api}/{page_id}/videos",
            data={
                "file_url": data.video.strip(),
                "published": False,
                "access_token": token
            }
        )
        if upload_resp.status_code != 200:
            err = extract_fb_error(upload_resp)
            raise HTTPException(status_code=400, detail=f"Erro no upload do vídeo: {err}")
        video_id = upload_resp.json().get("id")
        logging.info(f"Vídeo carregado, video_id={video_id}")

    # 7) Cria Ad Set
    adset_payload = {
        "name": f"Ad Set for {data.campaign_name}",
        "campaign_id": camp_id,
        "daily_budget": daily,
        "billing_event": "IMPRESSIONS",
        "optimization_goal": optimization_goal,
        "bid_amount": 100,
        "targeting": targeting,
        "start_time": start_ts,
        "end_time": end_ts,
        "dsa_beneficiary": page_id,
        "dsa_payor": page_id,
        "access_token": token
    }
    adset_resp = requests.post(
        f"https://graph.facebook.com/{fb_api}/act_{acct}/adsets",
        json=adset_payload
    )
    adset_resp.raise_for_status()
    adset_id = adset_resp.json().get("id")

    # 8) Prepara Ad Creative
    default_link = data.content.strip() or "https://www.adstock.ai"
    default_msg = data.description
    if video_id:
        creative_spec = {
            "video_data": {
                "video_id": video_id,
                "title": data.campaign_name,
                "message": default_msg
            }
        }
    elif data.image.strip():
        creative_spec = {
            "link_data": {
                "message": default_msg,
                "link": default_link,
                "picture": data.image.strip()
            }
        }
    elif any(u.strip() for u in data.carrossel):
        child = [
            {"link": default_link, "picture": u.strip(), "message": default_msg}
            for u in data.carrossel if u.strip()
        ]
        creative_spec = {
            "link_data": {
                "child_attachments": child,
                "message": default_msg,
                "link": default_link
            }
        }
    else:
        creative_spec = {
            "link_data": {
                "message": default_msg,
                "link": default_link,
                "picture": "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
            }
        }

    try:
        creative_resp = requests.post(
            f"https://graph.facebook.com/{fb_api}/act_{acct}/adcreatives",
            json={
                "name": f"Ad Creative for {data.campaign_name}",
                "object_story_spec": {"page_id": page_id, **creative_spec},
                "access_token": token
            }
        )
        creative_resp.raise_for_status()
        creative_id = creative_resp.json().get("id")
    except requests.exceptions.HTTPError:
        err = extract_fb_error(creative_resp)
        # rollback campanha se falhar no creative
        requests.delete(f"https://graph.facebook.com/{fb_api}/{camp_id}?access_token={token}")
        raise HTTPException(status_code=400, detail=f"Erro no Ad Creative: {err}")

    # 9) Cria o Ad final
    try:
        ad_resp = requests.post(
            f"https://graph.facebook.com/{fb_api}/act_{acct}/ads",
            json={
                "name": f"Ad for {data.campaign_name}",
                "adset_id": adset_id,
                "creative": {"creative_id": creative_id},
                "status": "ACTIVE",
                "access_token": token
            }
        )
        ad_resp.raise_for_status()
        ad_id = ad_resp.json().get("id")
    except requests.exceptions.HTTPError:
        err = extract_fb_error(ad_resp)
        requests.delete(f"https://graph.facebook.com/{fb_api}/{camp_id}?access_token={token}")
        raise HTTPException(status_code=400, detail=f"Erro no Ad: {err}")

    return {
        "status": "success",
        "campaign_id": camp_id,
        "ad_set_id": adset_id,
        "creative_id": creative_id,
        "ad_id": ad_id,
        "campaign_link": f"https://www.facebook.com/adsmanager/manage/campaigns?act={acct}&campaign_ids={camp_id}"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
