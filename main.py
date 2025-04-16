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
from typing import List, Tuple

# Configuração de logging
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

GLOBAL_COUNTRIES = ["US","CA","GB","DE","FR","BR","IN","MX","IT",
                    "ES","NL","SE","NO","DK","FI","CH","JP","KR"]
PUBLISHER_PLATFORMS = ["facebook","instagram","audience_network","messenger"]

def extract_fb_error(resp: requests.Response) -> str:
    try:
        err = resp.json().get("error", {})
        return err.get("error_user_msg", err.get("message", "Erro desconhecido"))
    except:
        return "Erro processando resposta do Facebook"

def get_page_info(user_token: str) -> Tuple[str,str]:
    url = f"https://graph.facebook.com/v16.0/me/accounts?access_token={user_token}"
    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail="Erro ao buscar páginas")
    data = r.json().get("data", [])
    if not data:
        raise HTTPException(status_code=533, detail="Nenhuma página disponível")
    page = data[0]
    return page["id"], page["access_token"]

def check_account_balance(act_id: str, token: str, api_v: str, cap: int):
    url = f"https://graph.facebook.com/{api_v}/act_{act_id}?fields=spend_cap,amount_spent&access_token={token}"
    r = requests.get(url)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail="Erro ao verificar saldo")
    d = r.json()
    if int(d.get("spend_cap",0)) - int(d.get("amount_spent",0)) < cap:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(req: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg","Erro de validação")
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail":msg})

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
    video: str = Field(default="", alias="video")

    @field_validator("objective", mode="before")
    def map_objective(cls,v):
        m = {"Vendas":"OUTCOME_SALES","Promover site/app":"OUTCOME_TRAFFIC",
             "Leads":"OUTCOME_LEADS","Alcance de marca":"OUTCOME_AWARENESS"}
        return m.get(v,v)

    @field_validator("budget","min_salary","max_salary", mode="before")
    def parse_amounts(cls,v,info: ValidationInfo):
        defaults = {"budget":0.0,"min_salary":2000.0,"max_salary":20000.0}
        if isinstance(v,str):
            if not v.strip(): return defaults[info.field_name]
            c=v.replace("$","").replace(" ", "").replace(",",".")
            try: return float(c)
            except: raise ValueError(f"{info.field_name} inválido: {v}")
        if v is None: return defaults[info.field_name]
        return v

@app.post("/create_campaign")
async def create_campaign(req: Request):
    data = CampaignRequest(**(await req.json()))
    api_v = "v16.0"
    acct = data.account_id
    user_token = data.token

    # 1) Saldo
    cap = int(data.budget * 100)
    check_account_balance(acct, user_token, api_v, cap)

    # 2) Cria campanha
    resp = requests.post(
        f"https://graph.facebook.com/{api_v}/act_{acct}/campaigns",
        json={
            "name": data.campaign_name,
            "objective": data.objective,
            "status": "ACTIVE",
            "special_ad_categories": [],
            "access_token": user_token
        }
    )
    if resp.status_code != 200:
        logging.error("Erro criando campanha: %s", resp.text)
        raise HTTPException(status_code=400, detail=f"Erro ao criar campanha: {extract_fb_error(resp)}")
    camp_id = resp.json()["id"]

    # 3) Datas & orçamento diário
    try:
        sd = datetime.strptime(data.initial_date, "%m/%d/%Y")
        ed = datetime.strptime(data.final_date, "%m/%d/%Y")
        days = max((ed - sd).days, 1)
        daily = cap // days
        if daily < 576:
            raise HTTPException(status_code=400, detail="Daily < $5.76")
        if (ed - sd) < timedelta(hours=24):
            raise HTTPException(status_code=400, detail="Duração <24h")
        start_ts, end_ts = int(sd.timestamp()), int(ed.timestamp())
    except HTTPException:
        raise
    except:
        daily, start_ts, end_ts = cap, data.initial_date, data.final_date

    # 4) Optimization goal
    opt = "IMPRESSIONS" if data.objective == "OUTCOME_AWARENESS" else "LINK_CLICKS"

    # 5) Targeting spec
    genders = [] if data.target_sex.lower() == "all" else [1] if data.target_sex.lower() == "male" else [2]
    targeting = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": genders,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": PUBLISHER_PLATFORMS
    }

    # 6) Upload vídeo via multipart
    page_id, page_token = get_page_info(user_token)
    video_id = None
    if data.video.strip():
        # baixa do Firebase
        fb_vid = requests.get(data.video.strip(), stream=True)
        if fb_vid.status_code != 200:
            logging.error("Falha ao baixar vídeo: %s", fb_vid.text[:200])
            raise HTTPException(status_code=400, detail="Não foi possível baixar o vídeo da URL informada")
        # envia binário
        files = {"source": ("video.mp4", fb_vid.content, "video/mp4")}
        payload = {"published": False, "access_token": page_token}
        up = requests.post(f"https://graph.facebook.com/{api_v}/{page_id}/videos", files=files, data=payload)
        if up.status_code != 200:
            logging.error("Erro upload vídeo FB: %s", up.text)
            raise HTTPException(status_code=400, detail=f"Erro no upload do vídeo: {extract_fb_error(up)}")
        video_id = up.json().get("id")
        logging.info("Vídeo carregado com ID %s", video_id)

    # 7) Cria Ad Set
    adset = requests.post(
        f"https://graph.facebook.com/{api_v}/act_{acct}/adsets",
        json={
            "name": f"Ad Set {data.campaign_name}",
            "campaign_id": camp_id,
            "daily_budget": daily,
            "billing_event": "IMPRESSIONS",
            "optimization_goal": opt,
            "bid_amount": 100,
            "targeting": targeting,
            "start_time": start_ts,
            "end_time": end_ts,
            "dsa_beneficiary": page_id,
            "dsa_payor": page_id,
            "access_token": user_token
        }
    )
    adset.raise_for_status()
    adset_id = adset.json()["id"]

    # 8) Prepara AdCreative
    link = data.content.strip() or "https://www.adstock.ai"
    msg = data.description
    if video_id:
        spec = {"video_data": {"video_id": video_id, "title": data.campaign_name, "message": msg}}
    elif data.image.strip():
        spec = {"link_data": {"message": msg, "link": link, "picture": data.image.strip()}}
    elif any(u.strip() for u in data.carrossel):
        child = [{"link": link, "picture": u.strip(), "message": msg} for u in data.carrossel if u.strip()]
        spec = {"link_data": {"child_attachments": child, "message": msg, "link": link}}
    else:
        spec = {
            "link_data": {
                "message": msg,
                "link": link,
                "picture": "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
            }
        }

    cre = requests.post(
        f"https://graph.facebook.com/{api_v}/act_{acct}/adcreatives",
        json={
            "name": f"Creative {data.campaign_name}",
            "object_story_spec": {"page_id": page_id, **spec},
            "access_token": user_token
        }
    )
    cre.raise_for_status()
    creative_id = cre.json()["id"]

    # 9) Cria Ad final
    ad = requests.post(
        f"https://graph.facebook.com/{api_v}/act_{acct}/ads",
        json={
            "name": f"Ad {data.campaign_name}",
            "adset_id": adset_id,
            "creative": {"creative_id": creative_id},
            "status": "ACTIVE",
            "access_token": user_token
        }
    )
    ad.raise_for_status()
    ad_id = ad.json()["id"]

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
