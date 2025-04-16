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

# ─── Configuração de logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ─── FastAPI setup ────────────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajuste conforme ambiente
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Constantes ────────────────────────────────────────────────────────────────
FB_API_VERSION = "v16.0"
GLOBAL_COUNTRIES = ["US","CA","GB","DE","FR","BR","IN","MX","IT","ES","NL","SE","NO","DK","FI","CH","JP","KR"]
PUBLISHER_PLATFORMS = ["facebook","instagram","audience_network","messenger"]

# ─── Helpers ──────────────────────────────────────────────────────────────────
def extract_fb_error(response: requests.Response) -> str:
    try:
        err = response.json().get("error", {})
        return err.get("error_user_msg") or err.get("message") or response.text
    except Exception:
        return response.text or "Unknown Facebook API error"

def rollback_campaign(campaign_id: str, token: str):
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{campaign_id}"
    try:
        requests.delete(url, params={"access_token": token})
        logger.info(f"Rollback: campanha {campaign_id} deletada")
    except Exception:
        logger.exception("Falha ao fazer rollback da campanha")

def upload_video_to_fb(account_id: str, token: str, video_url: str) -> str:
    """
    Upload de vídeo para a conta de anúncios via file_url.
    Retorna o video_id no Facebook ou lança Exception com mensagem de erro.
    """
    url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}/advideos"
    payload = {
        "file_url": video_url,
        "access_token": token
    }
    resp = requests.post(url, data=payload)
    logger.debug(f"Upload vídeo status {resp.status_code}: {resp.text}")
    if resp.status_code != 200:
        msg = extract_fb_error(resp)
        raise Exception(f"Facebook video upload error: {msg}")
    vid = resp.json().get("id")
    if not vid:
        raise Exception("Facebook não retornou video_id após upload")
    return vid

def get_page_id(token: str) -> str:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/me/accounts"
    resp = requests.get(url, params={"access_token": token})
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao buscar páginas")
    data = resp.json().get("data", [])
    if not data:
        raise HTTPException(status_code=533, detail="Nenhuma página disponível")
    return data[0]["id"]

def check_account_balance(account_id: str, token: str, spend_cap: int):
    url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{account_id}"
    resp = requests.get(url, params={"fields":"spend_cap,amount_spent","access_token":token})
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Erro ao verificar saldo")
    js = resp.json()
    cap = int(js.get("spend_cap",0))
    spent = int(js.get("amount_spent",0))
    if cap - spent < spend_cap:
        raise HTTPException(status_code=402, detail="Fundos insuficientes")

# ─── Modelos Pydantic ─────────────────────────────────────────────────────────
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
    image: str = ""
    carrossel: List[str] = []
    video: str = Field(default="", alias="video")

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        mapping = {
            "Vendas":"OUTCOME_SALES",
            "Promover site/app":"OUTCOME_TRAFFIC",
            "Leads":"OUTCOME_LEADS",
            "Alcance de marca":"OUTCOME_AWARENESS"
        }
        return mapping.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            return float(v.replace("$","").replace(",","."))
        return v

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    msg = exc.errors()[0].get("msg", "Erro de validação")
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": msg})

# ─── Endpoint principal ───────────────────────────────────────────────────────
@app.post("/create_campaign")
async def create_campaign(req: Request):
    data = CampaignRequest(**await req.json())
    logger.info(f"Criando campanha: {data.campaign_name}")

    # 1) Verifica saldo
    total_budget_cents = int(data.budget * 100)
    check_account_balance(data.account_id, data.token, total_budget_cents)

    # 2) Cria campanha
    camp_url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/campaigns"
    camp_payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",
        "access_token": data.token,
        "special_ad_categories": []
    }
    camp_resp = requests.post(camp_url, json=camp_payload)
    if camp_resp.status_code != 200:
        raise HTTPException(status_code=400, detail=extract_fb_error(camp_resp))
    camp_id = camp_resp.json()["id"]

    # 3) Calcula orçamento diário e cria Ad Set
    start_dt = datetime.strptime(data.initial_date, "%m/%d/%Y")
    end_dt   = datetime.strptime(data.final_date, "%m/%d/%Y")
    days     = max((end_dt - start_dt).days, 1)
    daily_budget = total_budget_cents // days
    if daily_budget < 576:
        rollback_campaign(camp_id, data.token)
        raise HTTPException(status_code=400, detail="O valor diário deve ser maior que $5.76")
    if (end_dt - start_dt) < timedelta(hours=24):
        rollback_campaign(camp_id, data.token)
        raise HTTPException(status_code=400, detail="Duração mínima de 24h")

    opt_goal = ("IMPRESSIONS" if data.objective=="OUTCOME_AWARENESS"
                else "LINK_CLICKS")
    genders = {"male":[1],"female":[2]}.get(data.target_sex.lower(),[])
    tgt_spec = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": genders,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": PUBLISHER_PLATFORMS
    }
    page_id = get_page_id(data.token)
    adset_url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/adsets"
    adset_payload = {
        "name": f"AdSet {data.campaign_name}",
        "campaign_id": camp_id,
        "daily_budget": daily_budget,
        "billing_event": "IMPRESSIONS",
        "optimization_goal": opt_goal,
        "bid_amount": 100,
        "targeting": tgt_spec,
        "start_time": int(start_dt.timestamp()),
        "end_time":   int(end_dt.timestamp()),
        "dsa_beneficiary": page_id,
        "dsa_payor":       page_id,
        "access_token":    data.token
    }
    adset_resp = requests.post(adset_url, json=adset_payload)
    if adset_resp.status_code != 200:
        rollback_campaign(camp_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(adset_resp))
    adset_id = adset_resp.json()["id"]

    # 4) Se for vídeo, faz upload via URL e obtém video_id
    video_id = None
    if data.video.strip():
        # remove eventuais ; ou ,
        video_url = data.video.strip().rstrip(";,")
        try:
            video_id = upload_video_to_fb(data.account_id, data.token, video_url)
        except Exception as e:
            rollback_campaign(camp_id, data.token)
            raise HTTPException(status_code=400, detail=str(e))

    # 5) Monta spec do Creative
    default_link    = data.content or "https://www.adstock.ai"
    default_message = data.description
    if video_id:
        creative_spec = {
            "video_data": {
                "video_id": video_id,
                "title":   data.campaign_name,
                "message": default_message
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
        child = [{"link":default_link,"picture":u,"message":default_message}
                 for u in data.carrossel if u.strip()]
        creative_spec = {"link_data": {"child_attachments":child,
                                       "message":default_message,
                                       "link":default_link}}
    else:
        creative_spec = {
            "link_data": {
                "message": default_message,
                "link": default_link,
                "picture": "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
            }
        }

    # 6) Cria Ad Creative
    creative_url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/adcreatives"
    creative_payload = {
        "name": f"Creative {data.campaign_name}",
        "object_story_spec": {"page_id":page_id, **creative_spec},
        "access_token": data.token
    }
    creative_resp = requests.post(creative_url, json=creative_payload)
    if creative_resp.status_code != 200:
        rollback_campaign(camp_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(creative_resp))
    creative_id = creative_resp.json()["id"]

    # 7) Cria o Ad final
    ad_url = f"https://graph.facebook.com/{FB_API_VERSION}/act_{data.account_id}/ads"
    ad_payload = {
        "name": f"Ad {data.campaign_name}",
        "adset_id": adset_id,
        "creative": {"creative_id": creative_id},
        "status": "ACTIVE",
        "access_token": data.token
    }
    ad_resp = requests.post(ad_url, json=ad_payload)
    if ad_resp.status_code != 200:
        rollback_campaign(camp_id, data.token)
        raise HTTPException(status_code=400, detail=extract_fb_error(ad_resp))
    ad_id = ad_resp.json()["id"]

    # 8) Retorna resultados
    return {
        "status": "success",
        "campaign_id": camp_id,
        "ad_set_id": adset_id,
        "creative_id": creative_id,
        "ad_id": ad_id,
        "campaign_link": f"https://www.facebook.com/adsmanager/manage/campaigns?act={data.account_id}&campaign_ids={camp_id}"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
