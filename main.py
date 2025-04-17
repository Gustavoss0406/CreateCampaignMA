import logging
import sys
import os
import time
import json
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ─── FastAPI setup ─────────────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Constantes ─────────────────────────────────────────────────────────────────
FB_API_VERSION      = "v16.0"
GLOBAL_COUNTRIES    = ["US","CA","GB","DE","FR","BR","IN","MX","IT","ES","NL","SE","NO","DK","FI","CH","JP","KR"]
PUBLISHER_PLATFORMS = ["facebook","instagram","audience_network","messenger"]

OBJECTIVE_TO_OPT_GOAL = {
    "OUTCOME_AWARENESS": "IMPRESSIONS",
    "OUTCOME_TRAFFIC":   "LINK_CLICKS",
    "OUTCOME_LEADS":     "LEAD_GENERATION",
    "OUTCOME_SALES":     "OFFSITE_CONVERSIONS",
    "PAGE_LIKES":        "PAGE_LIKES",
}

OBJECTIVE_TO_BILLING_EVENT = {
    "OUTCOME_AWARENESS": "IMPRESSIONS",
    "OUTCOME_TRAFFIC":   "LINK_CLICKS",
    "OUTCOME_LEADS":     "IMPRESSIONS",
    "OUTCOME_SALES":     "IMPRESSIONS",
    "PAGE_LIKES":        "PAGE_LIKES",
}

CTA_MAP = {
    "OUTCOME_AWARENESS": {"type": "LEARN_MORE", "value": {"link": ""}},
    "OUTCOME_TRAFFIC":   {"type": "LEARN_MORE", "value": {"link": ""}},
    "OUTCOME_LEADS":     {"type": "SIGN_UP",    "value": {"link": ""}},
    "OUTCOME_SALES":     {"type": "SHOP_NOW",   "value": {"link": ""}},
    "PAGE_LIKES":        {"type": "LIKE_PAGE",  "value": {"page": ""}},
}

# ─── Helpers ────────────────────────────────────────────────────────────────────
def extract_fb_error(resp: requests.Response) -> str:
    try:
        err = resp.json().get("error", {})
        return err.get("error_user_msg") or err.get("message") or resp.text
    except:
        return resp.text or "Erro desconhecido"

def rollback_campaign(campaign_id: str, token: str):
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{campaign_id}"
    try:
        requests.delete(url, params={"access_token": token})
        logger.info(f"Rollback: campanha {campaign_id} deletada")
    except:
        logger.exception("Falha no rollback")

def upload_video_to_fb(account_id: str, token: str, video_url: str) -> str:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}/advideos"
    resp = requests.post(url, data={"file_url": video_url, "access_token": token})
    logger.debug(f"Upload vídeo status {resp.status_code}: {resp.text}")
    if resp.status_code != 200:
        raise Exception(f"Erro ao enviar vídeo: {extract_fb_error(resp)}")
    vid = resp.json().get("id")
    if not vid:
        raise Exception("Facebook não retornou video_id")
    return vid

def fetch_video_thumbnail(video_id: str, token: str) -> str:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{video_id}/thumbnails"
    for _ in range(5):
        resp = requests.get(url, params={"access_token": token})
        data = resp.json().get("data", [])
        if resp.status_code == 200 and data:
            return data[0]["uri"]
        logger.debug("Thumbnail ainda não disponível, aguardando...")
        time.sleep(2)
    raise Exception("Não foi possível obter thumbnail do vídeo")

def get_page_id(token: str) -> str:
    resp = requests.get(
        f"https://graph.facebook.com/{FB_API_VERSION}/me/accounts",
        params={"access_token": token}
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao buscar páginas")
    data = resp.json().get("data", [])
    if not data:
        raise HTTPException(status_code=533, detail="Nenhuma página disponível")
    return data[0]["id"]

def check_account_balance(account_id: str, token: str, required_cents: int):
    resp = requests.get(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}",
        params={"fields": "spend_cap,amount_spent", "access_token": token}
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao verificar saldo")
    js = resp.json()
    cap   = int(js.get("spend_cap", 0))
    spent = int(js.get("amount_spent", 0))
    if cap - spent < required_cents:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

# ─── Modelos Pydantic ───────────────────────────────────────────────────────────
class CampaignRequest(BaseModel):
    account_id: str
    token: str
    campaign_name: str = ""
    objective: str = "OUTCOME_TRAFFIC"
    content: str = ""
    description: str = ""
    keywords: str = ""
    budget: float = 0.0
    initial_date: str = ""   # "MM/DD/YYYY"
    final_date: str = ""     # "MM/DD/YYYY"
    target_sex: str = ""     # "male" / "female" / ""
    target_age: int = 0
    image: str = ""
    carrossel: List[str] = []
    video: str = Field(default="", alias="video")
    pixel_id: Optional[str] = None

    @field_validator("objective", mode="before")
    def map_objective(cls, v):
        m = {
            "Vendas":            "OUTCOME_SALES",
            "Promover site/app": "OUTCOME_TRAFFIC",
            "Leads":             "OUTCOME_LEADS",
            "Alcance de marca":  "PAGE_LIKES",       # Agora vira PAGE_LIKES
            "Seguidores":        "PAGE_LIKES",
        }
        return m.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            return float(v.replace("$", "").replace(",", "."))
        return v

@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg", "Erro de validação")
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": msg})

# ─── Endpoint ──────────────────────────────────────────────────────────────────
@app.post("/create_campaign")
async def create_campaign(req: Request):
    data = CampaignRequest(**await req.json())
    logger.info(f"Iniciando campanha: {data.campaign_name}")

    # 1) Verifica saldo
    total_cents = int(data.budget * 100)
    check_account_balance(data.account_id, data.token, total_cents)

    # 2) Cria campanha
    camp_resp = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/campaigns",
        json={
            "name":                 data.campaign_name,
            "objective":            data.objective,
            "status":               "ACTIVE",
            "access_token":         data.token,
            "special_ad_categories": []
        }
    )
    if camp_resp.status_code != 200:
        raise HTTPException(status_code=400, detail=extract_fb_error(camp_resp))
    campaign_id = camp_resp.json()["id"]

    # 3) Datas e orçamento diário
    start_dt  = datetime.strptime(data.initial_date, "%m/%d/%Y")
    end_dt    = datetime.strptime(data.final_date,   "%m/%d/%Y")
    days_diff = (end_dt - start_dt).days
    days      = max(days_diff, 1)
    daily     = total_cents // days
    logger.debug(f"Dias planejados: {days_diff} → budget diário: {daily} cents")
    if daily < 576:
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail="Orçamento diário deve ser ≥ $5.76")

    now_ts     = int(time.time())
    start_ts   = max(int(start_dt.timestamp()), now_ts + 60)
    end_ts     = start_ts + days * 86400

    opt_goal      = OBJECTIVE_TO_OPT_GOAL.get(data.objective)
    billing_event = OBJECTIVE_TO_BILLING_EVENT.get(data.objective)
    genders       = {"male":[1], "female":[2]}.get(data.target_sex.lower(), [])
    page_id       = get_page_id(data.token)

    # 4) Monta Ad Set
    adset_payload = {
        "name":               f"AdSet {data.campaign_name}",
        "campaign_id":        campaign_id,
        "daily_budget":       daily,
        "billing_event":      billing_event,
        "optimization_goal":  opt_goal,
        "bid_amount":         100,
        "targeting": {
            "geo_locations": {"countries": GLOBAL_COUNTRIES},
            "genders":       genders,
            "age_min":       data.target_age,
            "age_max":       data.target_age,
            "publisher_platforms": PUBLISHER_PLATFORMS
        },
        "start_time":         start_ts,
        "end_time":           end_ts,
        "access_token":       data.token
    }

    # promoted_object apenas para Leads e Sales
    if data.objective in ["OUTCOME_LEADS", "OUTCOME_SALES"]:
        prom = {"page_id": page_id} if data.objective=="OUTCOME_LEADS" else {"pixel_id": data.pixel_id, "custom_event_type":"PURCHASE"}
        if data.objective=="OUTCOME_SALES" and not data.pixel_id:
            rollback_campaign(campaign_id, data.token)
            raise HTTPException(status_code=400, detail="pixel_id obrigatório")
        adset_payload["promoted_object"] = prom

    resp_adset = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/adsets",
        json=adset_payload
    )
    if resp_adset.status_code != 200:
        logger.error("Falha AdSet payload:\n%s", json.dumps(adset_payload, indent=2))
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(resp_adset))
    adset_id = resp_adset.json()["id"]

    # 5) Vídeo + thumbnail
    video_id  = None
    thumbnail = None
    if data.video.strip():
        try:
            video_id  = upload_video_to_fb(data.account_id, data.token, data.video.strip())
            thumbnail = fetch_video_thumbnail(video_id, data.token)
        except Exception as e:
            rollback_campaign(campaign_id, data.token)
            raise HTTPException(status_code=400, detail=str(e))

    # 6) Monta creative_spec com suporte a PAGE_LIKES
    default_link    = data.content or "https://www.adstock.ai"
    default_message = data.description

    if data.objective == "PAGE_LIKES":
        cta = CTA_MAP["PAGE_LIKES"].copy()
        cta["value"]["page"] = page_id
        if video_id:
            creative_spec = {
                "video_data": {
                    "video_id":       video_id,
                    "message":        default_message,
                    "image_url":      thumbnail,
                    "call_to_action": cta
                }
            }
        else:
            creative_spec = {
                "link_data": {
                    "message":        default_message,
                    "link":           default_link,
                    "picture":        data.image.strip() or "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder",
                    "call_to_action": cta
                }
            }
    elif video_id:
        cta = CTA_MAP.get(data.objective, {}).copy()
        cta["value"]["link"] = default_link
        creative_spec = {
            "video_data": {
                "video_id":       video_id,
                "message":        default_message,
                "image_url":      thumbnail,
                "call_to_action": cta
            }
        }
    elif data.image.strip():
        creative_spec = {
            "link_data": {
                "message": default_message,
                "link":    default_link,
                "picture": data.image.strip()
            }
        }
    elif any(u.strip() for u in data.carrossel):
        attachments = [
            {"link": default_link, "picture": u, "message": default_message}
            for u in data.carrossel if u.strip()
        ]
        creative_spec = {
            "link_data": {
                "child_attachments": attachments,
                "message":           default_message,
                "link":              default_link
            }
        }
    else:
        creative_spec = {
            "link_data": {
                "message": default_message,
                "link":    default_link,
                "picture": "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
            }
        }

    # 7) Cria Creative
    creative_resp = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/adcreatives",
        json={
            "name":              f"Creative {data.campaign_name}",
            "object_story_spec": {"page_id": page_id, **creative_spec},
            "access_token":      data.token
        }
    )
    if creative_resp.status_code != 200:
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(creative_resp))
    creative_id = creative_resp.json()["id"]

    # 8) Cria o Ad
    ad_resp = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/ads",
        json={
            "name":         f"Ad {data.campaign_name}",
            "adset_id":     adset_id,
            "creative":     {"creative_id": creative_id},
            "status":       "ACTIVE",
            "access_token": data.token
        }
    )
    if ad_resp.status_code != 200:
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(ad_resp))
    ad_id = ad_resp.json()["id"]

    return {
        "status":        "success",
        "campaign_id":   campaign_id,
        "ad_set_id":     adset_id,
        "creative_id":   creative_id,
        "ad_id":         ad_id,
        "campaign_link": f"https://www.facebook.com/adsmanager/manage/campaigns?act={data.account_id}&campaign_ids={campaign_id}"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
