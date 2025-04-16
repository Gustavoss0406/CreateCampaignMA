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

# ─── Setup básico ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GLOBAL_COUNTRIES = ["US","CA","GB","DE","FR","BR","IN","MX","IT","ES","NL","SE","NO","DK","FI","CH","JP","KR"]
PUBLISHER_PLATFORMS = ["facebook","instagram","audience_network","messenger"]

# ─── Helpers ───────────────────────────────────────────────────────────────────

def extract_fb_error(response: requests.Response) -> str:
    try:
        err = response.json().get("error", {})
        return err.get("error_user_msg") or err.get("message") or "Erro desconhecido"
    except:
        return "Erro processando resposta do Facebook"

def get_page_id(token: str) -> str:
    resp = requests.get(f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Não foi possível buscar páginas")
    data = resp.json().get("data", [])
    if not data:
        raise HTTPException(status_code=533, detail="Nenhuma página disponível")
    return data[0]["id"]

def check_account_balance(account_id: str, token: str, version: str, required_cents: int):
    resp = requests.get(
        f"https://graph.facebook.com/{version}/act_{account_id}",
        params={"fields":"spend_cap,amount_spent","access_token":token}
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao verificar saldo")
    j = resp.json()
    cap = int(j.get("spend_cap",0))
    spent = int(j.get("amount_spent",0))
    if cap - spent < required_cents:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

# ─── Pydantic Model ────────────────────────────────────────────────────────────

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
    target_sex: str = ""
    target_age: int = 0
    min_salary: float = Field(default=2000.0)
    max_salary: float = Field(default=20000.0)
    image: str = ""
    carrossel: List[str] = []
    video: str = ""

    @field_validator("objective", mode="before")
    def map_obj(cls, v):
        m = {"Vendas":"OUTCOME_SALES","Promover site/app":"OUTCOME_TRAFFIC","Leads":"OUTCOME_LEADS","Alcance de marca":"OUTCOME_AWARENESS"}
        return m.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            v = v.replace("$","").replace(",","").strip()
        return float(v)

# ─── Exception Handler ─────────────────────────────────────────────────────────

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg","Erro de validação")
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": msg})

# ─── Endpoint Principal ────────────────────────────────────────────────────────

@app.post("/create_campaign")
async def create_campaign(req: Request):
    payload = await req.json()
    try:
        data = CampaignRequest(**payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Payload inválido")

    version = "v16.0"
    acct = data.account_id
    token = data.token
    budget_cents = int(data.budget * 100)
    check_account_balance(acct, token, version, budget_cents)

    # 1) Criar Campaign
    camp = requests.post(
        f"https://graph.facebook.com/{version}/act_{acct}/campaigns",
        json={
            "name": data.campaign_name,
            "objective": data.objective,
            "status": "ACTIVE",
            "access_token": token,
            "special_ad_categories": []
        }
    )
    if not camp.ok:
        raise HTTPException(status_code=400, detail=extract_fb_error(camp))
    campaign_id = camp.json()["id"]

    # 2) Calcular datas e daily budget
    sd = datetime.strptime(data.initial_date, "%m/%d/%Y")
    ed = datetime.strptime(data.final_date, "%m/%d/%Y")
    days = max((ed - sd).days, 1)
    daily = budget_cents // days
    if daily < 576:
        raise HTTPException(status_code=400, detail="O valor diário deve ser > $5.76")
    if (ed - sd) < timedelta(hours=24):
        raise HTTPException(status_code=400, detail="Duração mínima de 24h")
    start_ts, end_ts = int(sd.timestamp()), int(ed.timestamp())

    # 3) Criar Ad Set
    genders = [1] if data.target_sex.lower()=="male" else [2] if data.target_sex.lower()=="female" else []
    aset = requests.post(
        f"https://graph.facebook.com/{version}/act_{acct}/adsets",
        json={
            "name": f"Ad Set {data.campaign_name}",
            "campaign_id": campaign_id,
            "daily_budget": daily,
            "billing_event": "IMPRESSIONS",
            "optimization_goal": "LINK_CLICKS" if data.objective!="OUTCOME_AWARENESS" else "IMPRESSIONS",
            "bid_amount": 100,
            "targeting": {
                "geo_locations": {"countries": GLOBAL_COUNTRIES},
                "genders": genders,
                "age_min": data.target_age,
                "age_max": data.target_age,
                "publisher_platforms": PUBLISHER_PLATFORMS
            },
            "start_time": start_ts,
            "end_time": end_ts,
            "dsa_beneficiary": get_page_id(token),
            "dsa_payor": get_page_id(token),
            "access_token": token
        }
    )
    if not aset.ok:
        # rollback campaign
        requests.delete(f"https://graph.facebook.com/{version}/{campaign_id}", params={"access_token":token})
        raise HTTPException(status_code=400, detail=extract_fb_error(aset))
    ad_set_id = aset.json()["id"]

    # 4) Upload do vídeo (se houver) e criação do Creative
    default_link = data.content or "https://www.adstock.ai"
    default_msg  = data.description

    if data.video:
        up = requests.post(
            f"https://graph.facebook.com/{version}/act_{acct}/advideos",
            data={"file_url": data.video, "access_token": token}
        )
        if not up.ok:
            raise HTTPException(status_code=400, detail=extract_fb_error(up))
        video_id = up.json()["id"]
        creative_body = {
            "video_data": {
                "video_id": video_id,
                "message": default_msg
            }
        }
    elif data.image:
        creative_body = {"link_data": {"message": default_msg, "link": default_link, "picture": data.image}}
    elif data.carrossel:
        items = [{"link": default_link, "picture": url, "message": default_msg} for url in data.carrossel]
        creative_body = {"link_data": {"child_attachments": items, "message": default_msg, "link": default_link}}
    else:
        fallback = "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
        creative_body = {"link_data": {"message": default_msg, "link": default_link, "picture": fallback}}

    arc = requests.post(
        f"https://graph.facebook.com/{version}/act_{acct}/adcreatives",
        json={
            "name": f"Creative {data.campaign_name}",
            "object_story_spec": {
                "page_id": get_page_id(token),
                **creative_body
            },
            "access_token": token
        }
    )
    if not arc.ok:
        # rollback adset + campaign
        requests.delete(f"https://graph.facebook.com/{version}/{ad_set_id}", params={"access_token":token})
        requests.delete(f"https://graph.facebook.com/{version}/{campaign_id}", params={"access_token":token})
        raise HTTPException(status_code=400, detail=extract_fb_error(arc))
    creative_id = arc.json()["id"]

    # 5) Criar o Ad final
    ad = requests.post(
        f"https://graph.facebook.com/{version}/act_{acct}/ads",
        json={
            "name": f"Ad {data.campaign_name}",
            "adset_id": ad_set_id,
            "creative": {"creative_id": creative_id},
            "status": "ACTIVE",
            "access_token": token
        }
    )
    if not ad.ok:
        # rollback all
        requests.delete(f"https://graph.facebook.com/{version}/{campaign_id}", params={"access_token":token})
        raise HTTPException(status_code=400, detail=extract_fb_error(ad))
    ad_id = ad.json()["id"]

    return {
        "status": "success",
        "campaign_id": campaign_id,
        "ad_set_id": ad_set_id,
        "creative_id": creative_id,
        "ad_id": ad_id,
        "campaign_link": f"https://www.facebook.com/adsmanager/manage/campaigns?act={acct}&campaign_ids={campaign_id}"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
