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
from typing import List

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
FB_API_VERSION       = "v16.0"
GLOBAL_COUNTRIES     = ["US","CA","GB","DE","FR","BR","IN","MX","IT","ES","NL","SE","NO","DK","FI","CH","JP","KR"]
PUBLISHER_PLATFORMS  = ["facebook","instagram","audience_network","messenger"]

# Objetivos → optimization_goal
OBJECTIVE_TO_OPT_GOAL = {
    "OUTCOME_AWARENESS": "IMPRESSIONS",
    "OUTCOME_TRAFFIC":   "LINK_CLICKS",
}

# Todos os billing_event como IMPRESSIONS para máxima compatibilidade
OBJECTIVE_TO_BILLING_EVENT = {
    "OUTCOME_AWARENESS": "IMPRESSIONS",
    "OUTCOME_TRAFFIC":   "IMPRESSIONS",
}

CTA_MAP = {
    "OUTCOME_AWARENESS": {"type": "LEARN_MORE", "value": {"link": ""}},
    "OUTCOME_TRAFFIC":   {"type": "LEARN_MORE", "value": {"link": ""}},
}

MIN_DAILY_BUDGET_CENTS = 500  # mínimo fixo de 5.00 na moeda da conta

# ─── Helpers ────────────────────────────────────────────────────────────────────
def extract_fb_error(resp: requests.Response) -> str:
    try:
        err = resp.json().get("error", {})
        return err.get("error_user_msg") or err.get("message") or resp.text
    except:
        return resp.text or "Erro desconhecido"

def rollback_campaign(campaign_id: str, token: str):
    try:
        url = f"https://graph.facebook.com/{FB_API_VERSION}/{campaign_id}"
        requests.delete(url, params={"access_token": token})
        logger.info(f"Rollback: campanha {campaign_id} deletada")
    except:
        logger.exception("Falha no rollback da campanha")

def upload_video_to_fb(account_id: str, token: str, video_url: str) -> str:
    logger.debug(f"Iniciando upload de vídeo via URL: {video_url}")
    url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}/advideos"
    resp = requests.post(url, data={"file_url": video_url, "access_token": token})
    logger.debug(f"Resposta upload vídeo: {resp.status_code} {resp.text}")
    if resp.status_code != 200:
        raise Exception(f"Erro ao enviar vídeo: {extract_fb_error(resp)}")
    vid = resp.json().get("id")
    if not vid:
        raise Exception("Facebook não retornou video_id")
    return vid

def fetch_video_thumbnail(video_id: str, token: str) -> str:
    logger.debug(f"Buscando thumbnail para video_id={video_id}")
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{video_id}/thumbnails"
    for _ in range(5):
        resp = requests.get(url, params={"access_token": token})
        items = resp.json().get("data", [])
        if resp.status_code == 200 and items:
            return items[0]["uri"]
        time.sleep(2)
    raise Exception("Não foi possível obter thumbnail do vídeo")

def get_page_id(token: str) -> str:
    logger.debug("Recuperando page_id via /me/accounts")
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
    target_sex: str = ""     # "male"/"female"/""
    target_age: int = 0
    image: str = ""
    carrossel: List[str] = []
    video: str = Field(default="", alias="video")

    @field_validator("objective", mode="before")
    def map_objective(cls, v):
        m = {
            "Vendas":            "OUTCOME_TRAFFIC",
            "Promover site/app": "OUTCOME_TRAFFIC",
            "Leads":             "OUTCOME_TRAFFIC",
            "Alcance de marca":  "OUTCOME_AWARENESS",
        }
        return m.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            cleaned = v.replace("$", "").replace(",", ".")
            logger.debug(f"Parsing budget '{v}' → {cleaned}")
            return float(cleaned)
        return v

@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg", "Erro de validação")
    logger.error(f"Validação de entrada falhou: {msg}")
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": msg})

# ─── Endpoint ──────────────────────────────────────────────────────────────────
@app.post("/create_campaign")
async def create_campaign(req: Request):
    body = await req.json()
    logger.debug(f"Request body: {json.dumps(body)}")
    data = CampaignRequest(**body)

    # Checagens iniciais
    if data.budget <= 0:
        logger.error("budget inválido ou zero")
    if not data.campaign_name:
        logger.error("campaign_name vazio")
    if not data.initial_date or not data.final_date:
        logger.error("initial_date ou final_date vazio")
    if not (data.video or data.image or any(data.carrossel)):
        logger.warning("Sem mídia: será usado placeholder")

    # 1) Verifica saldo da conta
    info = requests.get(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}",
        params={"fields": "spend_cap,amount_spent,currency", "access_token": data.token}
    ).json()
    cap   = int(info.get("spend_cap", 0))
    spent = int(info.get("amount_spent", 0))
    logger.debug(f"Conta {data.account_id}: cap={cap}, spent={spent}")
    total_cents = int(data.budget * 100)
    if cap - spent < total_cents:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

    # 2) Cria campanha
    camp_payload = {
        "name":                 data.campaign_name,
        "objective":            data.objective,
        "status":               "ACTIVE",
        "access_token":         data.token,
        "special_ad_categories": []
    }
    logger.debug(f"Payload Campaign: {json.dumps(camp_payload)}")
    camp_resp = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/campaigns",
        json=camp_payload
    )
    logger.debug(f"Campaign response: {camp_resp.status_code} {camp_resp.text}")
    if camp_resp.status_code != 200:
        raise HTTPException(status_code=400, detail=extract_fb_error(camp_resp))
    campaign_id = camp_resp.json()["id"]

    # 3) Datas e orçamento diário
    start_dt   = datetime.strptime(data.initial_date, "%m/%d/%Y")
    end_dt     = datetime.strptime(data.final_date,   "%m/%d/%Y")
    days_diff  = (end_dt - start_dt).days
    days       = max(days_diff, 1)
    daily      = total_cents // days
    logger.debug(f"Dias planejados: {days_diff} → usando {days} → budget diário: {daily} cents")

    if daily < MIN_DAILY_BUDGET_CENTS:
        logger.error(f"Orçamento diário abaixo do mínimo de {MIN_DAILY_BUDGET_CENTS/100:.2f}")
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(
            status_code=400,
            detail=f"Orçamento diário deve ser ≥ {MIN_DAILY_BUDGET_CENTS/100:.2f}"
        )

    # Garante duração ≥24h
    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())
    if end_ts - start_ts < 86400:
        logger.warning("Duração <24h, ajustando para +24h")
        end_ts = start_ts + 86400

    # 4) Cria Ad Set
    opt_goal      = OBJECTIVE_TO_OPT_GOAL[data.objective]
    billing_event = OBJECTIVE_TO_BILLING_EVENT[data.objective]
    genders       = {"male":[1], "female":[2]}.get(data.target_sex.lower(), [])
    page_id       = get_page_id(data.token)

    adset_payload = {
        "name":               f"AdSet {data.campaign_name}",
        "campaign_id":        campaign_id,
        "daily_budget":       daily,
        "billing_event":      billing_event,
        "optimization_goal":  opt_goal,
        "bid_amount":         100,
        "targeting": {
            "geo_locations":    {"countries": GLOBAL_COUNTRIES},
            "genders":          genders,
            "age_min":          data.target_age,
            "age_max":          data.target_age,
            "publisher_platforms": PUBLISHER_PLATFORMS
        },
        "start_time":         start_ts,
        "end_time":           end_ts,
        "access_token":       data.token
    }
    logger.debug(f"Payload AdSet: {json.dumps(adset_payload)}")
    resp_adset = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/adsets",
        json=adset_payload
    )
    logger.debug(f"AdSet response: {resp_adset.status_code} {resp_adset.text}")
    if resp_adset.status_code != 200:
        logger.error("Erro ao criar Ad Set")
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(resp_adset))
    adset_id = resp_adset.json()["id"]

    # 5) Upload vídeo + thumbnail
    video_id  = None
    thumbnail = None
    if data.video.strip():
        try:
            video_id  = upload_video_to_fb(data.account_id, data.token, data.video.strip().rstrip(";,"))
            thumbnail = fetch_video_thumbnail(video_id, data.token)
        except Exception as e:
            logger.exception("Erro no upload de vídeo")
            rollback_campaign(campaign_id, data.token)
            raise HTTPException(status_code=400, detail=str(e))

    # 6) Monta creative_spec
    default_link    = data.content or "https://www.adstock.ai"
    default_message = data.description
    if video_id:
        cta = CTA_MAP[data.objective].copy()
        cta["value"]["link"] = default_link
        creative_spec = {"video_data": {
            "video_id":       video_id,
            "message":        default_message,
            "image_url":      thumbnail,
            "call_to_action": cta
        }}
    elif data.image.strip():
        creative_spec = {"link_data": {
            "message": default_message,
            "link":    default_link,
            "picture": data.image.strip()
        }}
    elif any(u.strip() for u in data.carrossel):
        child = [{"link": default_link, "picture": u, "message": default_message}
                 for u in data.carrossel if u.strip()]
        creative_spec = {"link_data": {
            "child_attachments": child,
            "message":           default_message,
            "link":              default_link
        }}
    else:
        creative_spec = {"link_data": {
            "message": default_message,
            "link":    default_link,
            "picture": "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
        }}

    creative_payload = {
        "name":              f"Creative {data.campaign_name}",
        "object_story_spec": {"page_id": page_id, **creative_spec},
        "access_token":      data.token
    }
    logger.debug(f"Payload Creative: {json.dumps(creative_payload)}")
    creative_resp = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/adcreatives",
        json=creative_payload
    )
    logger.debug(f"Creative response: {creative_resp.status_code} {creative_resp.text}")
    if creative_resp.status_code != 200:
        logger.error("Erro ao criar Ad Creative")
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(creative_resp))
    creative_id = creative_resp.json()["id"]

    # 7) Cria Ad final
    ad_payload = {
        "name":         f"Ad {data.campaign_name}",
        "adset_id":     adset_id,
        "creative":     {"creative_id": creative_id},
        "status":       "ACTIVE",
        "access_token": data.token
    }
    logger.debug(f"Payload Ad: {json.dumps(ad_payload)}")
    ad_resp = requests.post(
        f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/ads",
        json=ad_payload
    )
    logger.debug(f"Ad response: {ad_resp.status_code} {ad_resp.text}")
    if ad_resp.status_code != 200:
        logger.error("Erro ao criar Ad")
        rollback_campaign(campaign_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(ad_resp))
    ad_id = ad_resp.json()["id"]

    # 8) Retorno
    return {
        "status":        "success",
        "campaign_id":   campaign_id,
        "ad_set_id":     adset_id,
        "creative_id":   creative_id,
        "ad_id":         ad_id,
        "campaign_link": (
            "https://www.facebook.com/adsmanager/manage/campaigns"
            f"?act={data.account_id}&campaign_ids={campaign_id}"
        )
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
