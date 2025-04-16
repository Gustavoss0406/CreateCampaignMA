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
    allow_origins=["*"],  # Ajuste conforme necessário
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lista de códigos de país válidos (formato ISO) para direcionamento global
GLOBAL_COUNTRIES = [
    "US", "CA", "GB", "DE", "FR", "BR", "IN", "MX", "IT",
    "ES", "NL", "SE", "NO", "DK", "FI", "CH", "JP", "KR"
]
logging.debug(f"Global Countries definidos: {GLOBAL_COUNTRIES}")

# Plataformas publicadoras válidas para posicionamento do anúncio
PUBLISHER_PLATFORMS = ["facebook", "instagram", "audience_network", "messenger"]
logging.debug(f"Plataformas publicadoras definidas: {PUBLISHER_PLATFORMS}")

def extract_fb_error(response: requests.Response) -> str:
    logging.debug("Extraindo erro da resposta do Facebook.")
    try:
        error_json = response.json()
        logging.debug(f"JSON de erro recebido: {error_json}")
        return error_json.get("error", {}).get("error_user_msg", "Erro desconhecido ao comunicar com a API do Facebook")
    except Exception:
        logging.exception("Exceção ao processar JSON de erro da API do Facebook.")
        return "Erro processando a resposta da API do Facebook"

def get_page_id(token: str) -> str:
    url = f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}"
    logging.debug(f"Buscando páginas disponíveis com URL: {url}")
    response = requests.get(url)
    logging.debug(f"Código de status da requisição de páginas: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Resposta JSON de páginas: {data}")
        if data.get("data"):
            page_id = data["data"][0]["id"]
            logging.info(f"Página selecionada: {page_id}")
            return page_id
        logging.error("Nenhuma página encontrada na resposta do Facebook.")
        raise HTTPException(status_code=533, detail="Nenhuma página disponível para uso")
    else:
        logging.error(f"Erro ao buscar páginas, status: {response.status_code}")
        raise HTTPException(status_code=response.status_code, detail="Erro ao buscar páginas disponíveis")

def check_account_balance(account_id: str, token: str, fb_api_version: str, spend_cap: int):
    url = f"https://graph.facebook.com/{fb_api_version}/act_{account_id}?fields=spend_cap,amount_spent,currency&access_token={token}"
    logging.debug(f"Iniciando verificação do saldo da conta, URL: {url}")
    response = requests.get(url)
    logging.debug(f"Código de status da requisição de saldo: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Resposta JSON da verificação de saldo: {data}")
        if "spend_cap" in data and "amount_spent" in data:
            spend_cap_api = int(data["spend_cap"])
            amount_spent = int(data["amount_spent"])
            available_balance = spend_cap_api - amount_spent
            logging.info(f"Saldo disponível calculado: {available_balance}")
            if available_balance < spend_cap:
                logging.error("Saldo insuficiente para a campanha.")
                raise HTTPException(status_code=402, detail="Fundos insuficientes para criar a campanha")
        else:
            logging.error("Campos spend_cap ou amount_spent não encontrados na resposta.")
            raise HTTPException(status_code=402, detail="Fundos insuficientes para criar a campanha")
    else:
        logging.error("Erro ao verificar o saldo da conta na API do Facebook.")
        raise HTTPException(status_code=response.status_code, detail="Erro ao verificar o saldo da conta")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.error(f"Erro de validação na requisição: {exc.errors()}")
    first_message = exc.errors()[0].get("msg", "Erro de validação")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": first_message}
    )

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
    video: str = ""  # agora aceita 'video' em lowercase

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        mapping = {
            "Vendas": "OUTCOME_SALES",
            "Promover site/app": "OUTCOME_TRAFFIC",
            "Leads": "OUTCOME_LEADS",
            "Alcance de marca": "OUTCOME_AWARENESS"
        }
        return mapping.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            return float(v_clean)
        return v

    @field_validator("min_salary", mode="before")
    def parse_min_salary(cls, v):
        if v in [None, ""]:
            return 2000.0
        return float(str(v).replace("$", "").replace(" ", "").replace(",", "."))

    @field_validator("max_salary", mode="before")
    def parse_max_salary(cls, v):
        if v in [None, ""]:
            return 20000.0
        return float(str(v).replace("$", "").replace(" ", "").replace(",", "."))

@app.post("/create_campaign")
async def create_campaign(request: Request):
    logging.info("Início do endpoint /create_campaign")
    try:
        data_dict = await request.json()
        data = CampaignRequest(**data_dict)
        logging.info(f"Objeto CampaignRequest parseado com sucesso: {data}")
    except Exception:
        logging.exception("Erro ao ler ou parsear o corpo da requisição")
        raise HTTPException(status_code=400, detail="Erro ao ler ou parsear o corpo da requisição")

    fb_api_version = "v16.0"
    ad_account_id = data.account_id

    # Converte o orçamento total para centavos e verifica saldo
    total_budget_cents = int(data.budget * 100)
    check_account_balance(ad_account_id, data.token, fb_api_version, total_budget_cents)

    # --- Criação da Campanha ---
    campaign_payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",
        "access_token": data.token,
        "special_ad_categories": []
    }
    campaign_response = requests.post(
        f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/campaigns",
        json=campaign_payload
    )
    campaign_response.raise_for_status()
    campaign_id = campaign_response.json().get("id")

    # --- Cálculo de datas e orçamento diário ---
    try:
        start_dt = datetime.strptime(data.initial_date, "%m/%d/%Y")
        end_dt = datetime.strptime(data.final_date, "%m/%d/%Y")
        num_days = max((end_dt - start_dt).days, 1)
        daily_budget = total_budget_cents // num_days
        if daily_budget < 576:
            raise HTTPException(status_code=400, detail="O valor diário deve ser maior que $5.76")
        if (end_dt - start_dt) < timedelta(hours=24):
            raise HTTPException(status_code=400, detail="A duração da campanha deve ser de pelo menos 24 horas")
        ad_set_start = int(start_dt.timestamp())
        ad_set_end = int(end_dt.timestamp())
    except HTTPException:
        raise
    except Exception:
        ad_set_start = data.initial_date
        ad_set_end = data.final_date
        daily_budget = total_budget_cents

    # --- Determinação de optimization goal ---
    if data.objective == "OUTCOME_AWARENESS":
        optimization_goal = "IMPRESSIONS"
    elif data.objective in ["OUTCOME_TRAFFIC", "OUTCOME_LEADS", "OUTCOME_SALES"]:
        optimization_goal = "LINK_CLICKS"
    else:
        optimization_goal = "REACH"

    # --- Segmentação por gênero ---
    if data.target_sex.lower() == "male":
        genders = [1]
    elif data.target_sex.lower() == "female":
        genders = [2]
    else:
        genders = []

    # --- Lógica de vídeo: detecção de orientação e placements ---
    video_id = data.video.strip()
    platforms = PUBLISHER_PLATFORMS.copy()
    specific_positions = {}
    if video_id:
        try:
            meta_resp = requests.get(
                f"https://graph.facebook.com/{fb_api_version}/{video_id}"
                f"?fields=width,height&access_token={data.token}"
            )
            meta_resp.raise_for_status()
            info = meta_resp.json()
            w = int(info.get("width", 0))
            h = int(info.get("height", 0))
            logging.info(f"Detected video resolution: {w}x{h}")
            if h > w:
                platforms = ["instagram"]
                specific_positions = {"instagram_positions": ["reels"]}
                logging.info("Vertical video detectado: usando Instagram Reels")
            else:
                platforms = ["facebook"]
                specific_positions = {"facebook_positions": ["feed"]}
                logging.info("Horizontal video detectado: usando Facebook Feed")
        except Exception as e:
            logging.warning(f"Não foi possível obter metadados do vídeo: {e}")

    targeting_spec = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": genders,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": platforms,
    }
    targeting_spec.update(specific_positions)
    logging.debug(f"Targeting spec: {targeting_spec}")

    page_id = get_page_id(data.token)

    # --- Criação do Ad Set ---
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
    ad_set_response = requests.post(
        f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adsets",
        json=ad_set_payload
    )
    ad_set_response.raise_for_status()
    ad_set_id = ad_set_response.json().get("id")

    # --- Criação do Ad Creative ---
    default_link = data.content.strip() or "https://www.adstock.ai"
    default_message = data.description or ""

    if video_id:
        creative_spec = {
            "video_data": {
                "video_id": video_id,
                "title": data.campaign_name,
                "message": default_message
            }
        }
    elif data.image.strip():
        creative_spec = {
            "link_data": {
                "message": default_message,
                "link": default_link,
                "picture": data.image.strip()
            }
        }
    elif data.carrossel and any(u.strip() for u in data.carrossel):
        child_attachments = [
            {"link": default_link, "picture": u.strip(), "message": default_message}
            for u in data.carrossel if u.strip()
        ]
        creative_spec = {
            "link_data": {
                "child_attachments": child_attachments,
                "message": default_message,
                "link": default_link,
            }
        }
    else:
        fallback_image = "https://via.placeholder.com/1200x628.png?text=Ad+Placeholder"
        creative_spec = {
            "link_data": {
                "message": default_message,
                "link": default_link,
                "picture": fallback_image
            }
        }

    ad_creative_payload = {
        "name": f"Ad Creative for {data.campaign_name}",
        "object_story_spec": {
            "page_id": page_id,
            **creative_spec
        },
        "access_token": data.token
    }
    ad_creative_response = requests.post(
        f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adcreatives",
        json=ad_creative_payload
    )
    ad_creative_response.raise_for_status()
    creative_id = ad_creative_response.json().get("id")

    # --- Criação do Ad final ---
    ad_payload = {
        "name": f"Ad for {data.campaign_name}",
        "adset_id": ad_set_id,
        "creative": {"creative_id": creative_id},
        "status": "ACTIVE",
        "access_token": data.token
    }
    ad_response = requests.post(
        f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/ads",
        json=ad_payload
    )
    ad_response.raise_for_status()
    ad_id = ad_response.json().get("id")

    campaign_link = (
        f"https://www.facebook.com/adsmanager/manage/campaigns?"
        f"act={ad_account_id}&campaign_ids={campaign_id}"
    )
    logging.info("Processo de criação de campanha finalizado com sucesso.")

    return {
        "status": "success",
        "campaign_id": campaign_id,
        "ad_set_id": ad_set_id,
        "creative_id": creative_id,
        "ad_id": ad_id,
        "campaign_link": campaign_link
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Iniciando app com uvicorn no host 0.0.0.0 e porta {port}")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
