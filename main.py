import logging
import sys
import os
import requests
import uvicorn
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
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

GLOBAL_COUNTRIES = ["US","CA","GB","DE","FR","BR","IN","MX","IT","ES","NL","SE","NO","DK","FI","CH","JP","KR"]
PUBLISHER_PLATFORMS = ["facebook","instagram","audience_network","messenger"]

def extract_fb_error(response: requests.Response) -> str:
    try:
        err = response.json().get("error", {})
        return err.get("error_user_msg") or err.get("message") or "Erro desconhecido da API Facebook"
    except:
        return "Erro ao processar resposta de erro do Facebook"

def get_page_id(token: str) -> str:
    resp = requests.get(f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao buscar páginas")
    data = resp.json().get("data", [])
    if not data:
        raise HTTPException(status_code=533, detail="Nenhuma página disponível")
    return data[0]["id"]

def check_account_balance(account_id: str, token: str, v: str, cap: int):
    resp = requests.get(f"https://graph.facebook.com/{v}/act_{account_id}?fields=spend_cap,amount_spent&access_token={token}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao verificar saldo")
    d = resp.json()
    if int(d.get("spend_cap", 0)) - int(d.get("amount_spent", 0)) < cap:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg", "Erro de validação")
    return JSONResponse(status_code=422, content={"detail": msg})

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
    single_image: str = Field("", alias="Single Image")
    image: str = ""
    carrossel: List[str] = []
    video: str = ""  # aceita 'video' em lowercase

    @field_validator("objective", mode="before")
    def map_obj(cls, v):
        m = {
            "Vendas": "OUTCOME_SALES",
            "Promover site/app": "OUTCOME_TRAFFIC",
            "Leads": "OUTCOME_LEADS",
            "Alcance de marca": "OUTCOME_AWARENESS"
        }
        return m.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        return float(str(v).replace("$", "").replace(",", ".")) if isinstance(v, str) else v

    @field_validator("min_salary", mode="before")
    def parse_min(cls, v):
        if not v:
            return 2000.0
        return float(str(v).replace("$", "").replace(",", "."))

    @field_validator("max_salary", mode="before")
    def parse_max(cls, v):
        if not v:
            return 20000.0
        return float(str(v).replace("$", "").replace(",", "."))

@app.post("/create_campaign")
async def create_campaign(request: Request):
    data = CampaignRequest(**(await request.json()))
    fb_v = "v16.0"
    acct = data.account_id

    # 1) Verifica saldo
    cap = int(data.budget * 100)
    check_account_balance(acct, data.token, fb_v, cap)

    # 2) Cria campanha
    camp = requests.post(
        f"https://graph.facebook.com/{fb_v}/act_{acct}/campaigns",
        json={
            "name": data.campaign_name,
            "objective": data.objective,
            "status": "ACTIVE",
            "access_token": data.token,
            "special_ad_categories": []
        }
    )
    camp.raise_for_status()
    cid = camp.json()["id"]

    # 3) Calcula datas e orçamento diário
    sd = datetime.strptime(data.initial_date, "%m/%d/%Y")
    ed = datetime.strptime(data.final_date, "%m/%d/%Y")
    days = max((ed - sd).days, 1)
    daily = cap // days
    if daily < 576:
        raise HTTPException(status_code=400, detail="O valor diário deve ser maior que $5.76")
    ast = int(sd.timestamp())
    aet = int(ed.timestamp())

    # 4) Determina optimization goal
    opt = "IMPRESSIONS" if data.objective == "OUTCOME_AWARENESS" else "LINK_CLICKS"

    # 5) Segmentação por gênero
    g = []
    if data.target_sex.lower() == "male":
        g = [1]
    elif data.target_sex.lower() == "female":
        g = [2]

    # 6) Lógica de vídeo: orientação e placements
    vid = data.video.strip()
    plats = PUBLISHER_PLATFORMS.copy()
    pos = {}
    if vid:
        try:
            m = requests.get(
                f"https://graph.facebook.com/{fb_v}/{vid}?fields=width,height&access_token={data.token}"
            )
            m.raise_for_status()
            w, h = int(m.json().get("width", 0)), int(m.json().get("height", 0))
            if h > w:
                plats = ["instagram"]
                pos = {"instagram_positions": ["reels"]}
            else:
                plats = ["facebook"]
                pos = {"facebook_positions": ["feed"]}
        except:
            pass

    targeting = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": g,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": plats,
        **pos
    }

    # 7) Cria ad set
    aset = requests.post(
        f"https://graph.facebook.com/{fb_v}/act_{acct}/adsets",
        json={
            "name": f"AdSet {data.campaign_name}",
            "campaign_id": cid,
            "daily_budget": daily,
            "billing_event": "IMPRESSIONS",
            "optimization_goal": opt,
            "bid_amount": 100,
            "targeting": targeting,
            "start_time": ast,
            "end_time": aet,
            "dsa_beneficiary": get_page_id(data.token),
            "dsa_payor": get_page_id(data.token),
            "access_token": data.token
        }
    )
    aset.raise_for_status()
    aid = aset.json()["id"]

    # 8) Cria ad creative (com rollback em caso de erro)
    default_link = data.content.strip() or "https://www.adstock.ai"
    default_msg = data.description or ""
    if vid:
        creative_fields = {
            "video_data": {
                "video_id": vid,
                "title": data.campaign_name,
                "message": default_msg
            }
        }
    elif data.image.strip():
        creative_fields = {
            "link_data": {
                "message": default_msg,
                "link": default_link,
                "picture": data.image.strip()
            }
        }
    elif any(u.strip() for u in data.carrossel):
        ca = [
            {"link": default_link, "picture": u.strip(), "message": default_msg}
            for u in data.carrossel if u.strip()
        ]
        creative_fields = {
            "link_data": {
                "child_attachments": ca,
                "message": default_msg,
                "link": default_link
            }
        }
    else:
        creative_fields = {
            "link_data": {
                "message": default_msg,
                "link": default_link,
                "picture": "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
            }
        }

    try:
        cr = requests.post(
            f"https://graph.facebook.com/{fb_v}/act_{acct}/adcreatives",
            json={
                "name": f"Creative {data.campaign_name}",
                "object_story_spec": {"page_id": get_page_id(data.token), **creative_fields},
                "access_token": data.token
            }
        )
        cr.raise_for_status()
        crid = cr.json()["id"]
    except requests.exceptions.HTTPError:
        # rollback da campanha criada
        requests.delete(f"https://graph.facebook.com/{fb_v}/{cid}?access_token={data.token}")
        raise HTTPException(
            status_code=400,
            detail=f"Erro ao criar Ad Creative: {extract_fb_error(cr)}"
        )

    # 9) Cria ad final
    ad = requests.post(
        f"https://graph.facebook.com/{fb_v}/act_{acct}/ads",
        json={
            "name": f"Ad {data.campaign_name}",
            "adset_id": aid,
            "creative": {"creative_id": crid},
            "status": "ACTIVE",
            "access_token": data.token
        }
    )
    ad.raise_for_status()
    adid = ad.json()["id"]

    return {
        "status": "success",
        "campaign_id": cid,
        "ad_set_id": aid,
        "creative_id": crid,
        "ad_id": adid
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
