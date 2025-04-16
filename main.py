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
    """
    Extrai a mensagem de erro (error_user_msg) da resposta da API do Facebook.
    """
    logging.debug("Extraindo erro da resposta do Facebook.")
    try:
        error_json = response.json()
        logging.debug(f"JSON de erro recebido: {error_json}")
        return error_json.get("error", {}).get("error_user_msg", "Erro desconhecido ao comunicar com a API do Facebook")
    except Exception:
        logging.exception("Exceção ao processar JSON de erro da API do Facebook.")
        return "Erro processando a resposta da API do Facebook"

def get_page_id(token: str) -> str:
    """
    Obtém o primeiro ID de página disponível utilizando o token fornecido.
    Caso nenhuma página seja encontrada, lança HTTPException com status 533.
    """
    url = f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}"
    logging.debug(f"Buscando páginas disponíveis com URL: {url}")
    response = requests.get(url)
    logging.debug(f"Código de status da requisição de páginas: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Resposta JSON de páginas: {data}")
        if "data" in data and len(data["data"]) > 0:
            page_id = data["data"][0]["id"]
            logging.info(f"Página selecionada: {page_id}")
            return page_id
        else:
            logging.error("Nenhuma página encontrada na resposta do Facebook.")
            raise HTTPException(status_code=533, detail="Nenhuma página disponível para uso")
    else:
        logging.error(f"Erro ao buscar páginas, status: {response.status_code}")
        raise HTTPException(status_code=response.status_code, detail="Erro ao buscar páginas disponíveis")

def check_account_balance(account_id: str, token: str, fb_api_version: str, spend_cap: int):
    """
    Verifica se a conta possui fundos suficientes.
    Para contas pré-pagas, o saldo disponível = spend_cap - amount_spent.
    Se o saldo for inferior ao valor requerido, lança HTTPException com status 402.
    """
    url = f"https://graph.facebook.com/{fb_api_version}/act_{account_id}?fields=spend_cap,amount_spent,currency&access_token={token}"
    logging.debug(f"Iniciando verificação do saldo da conta, URL: {url}")
    response = requests.get(url)
    logging.debug(f"Código de status da requisição de saldo: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Resposta JSON da verificação de saldo: {data}")
        if "spend_cap" in data and "amount_spent" in data:
            try:
                spend_cap_api = int(data["spend_cap"])
                amount_spent = int(data["amount_spent"])
                logging.debug(f"spend_cap da conta: {spend_cap_api}, amount_spent: {amount_spent}")
            except Exception:
                logging.exception("Erro ao converter spend_cap ou amount_spent para inteiro.")
                raise HTTPException(status_code=402, detail="Fundos insuficientes para criar a campanha")
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
    try:
        first_message = exc.errors()[0]["msg"]
    except Exception:
        first_message = "Erro de validação"
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": first_message}
    )

class CampaignRequest(BaseModel):
    account_id: str               # ID da conta do Facebook
    token: str                    # Token válido por 60 dias
    campaign_name: str = ""       # Nome da campanha
    objective: str = "OUTCOME_TRAFFIC"  # Objetivo da campanha (valor default se não fornecido)
    content: str = ""             # URL de destino (ex.: link da landing page)
    description: str = ""          # Descrição da campanha (usada na mensagem do anúncio)
    keywords: str = ""             # Palavras-chave (usadas como legenda do anúncio)
    budget: float = 0.0            # Orçamento total (em dólares, ex.: "$300.00")
    initial_date: str = ""         # Data de início (ex.: "04/03/2025")
    final_date: str = ""           # Data final (ex.: "04/04/2025")
    pricing_model: str = ""        # Modelo de precificação (CPC, CPA, etc.)
    target_sex: str = ""           # Gênero alvo (ex.: "Male", "Female", "All")
    target_age: int = 0            # Idade alvo
    min_salary: float = 0.0        # Salário mínimo
    max_salary: float = 0.0        # Salário máximo
    devices: List[str] = []        # Dispositivos (informados mas não utilizados no direcionamento)

    # Novos campos para mídia:
    single_image: str = Field(default="", alias="Single Image")  # Campo antigo para imagem única (opcional)
    image: str = ""  # Novo campo para imagem estática; se preenchido, prevalece sobre carrossel
    carrossel: List[str] = []  # Lista de URLs para carrossel
    video: str = Field(default="", alias="video")    # Vídeo (alias agora em minúsculo)

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        logging.debug(f"Validando objetivo recebido: {v}")
        mapping = {
            "Vendas": "OUTCOME_SALES",
            "Promover site/app": "OUTCOME_TRAFFIC",
            "Leads": "OUTCOME_LEADS",
            "Alcance de marca": "OUTCOME_AWARENESS"
        }
        return mapping.get(v, v)

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        logging.debug(f"Validando orçamento: {v}")
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                return float(v_clean)
            except Exception:
                logging.exception("Erro ao converter orçamento para float.")
                raise ValueError("Orçamento inválido")
        return v

    @field_validator("min_salary", mode="before")
    def parse_min_salary(cls, v):
        logging.debug(f"Validando min_salary: {v}")
        if v in [None, ""]:
            return 2000.0
        v_clean = str(v).replace("$", "").replace(" ", "").replace(",", ".")
        try:
            return float(v_clean)
        except Exception:
            logging.exception(f"Erro ao converter min_salary: {v}")
            raise ValueError(f"min_salary inválido: {v}")

    @field_validator("max_salary", mode="before")
    def parse_max_salary(cls, v):
        logging.debug(f"Validando max_salary: {v}")
        if v in [None, ""]:
            return 20000.0
        v_clean = str(v).replace("$", "").replace(" ", "").replace(",", ".")
        try:
            return float(v_clean)
        except Exception:
            logging.exception(f"Erro ao converter max_salary: {v}")
            raise ValueError(f"max_salary inválido: {v}")

@app.post("/create_campaign")
async def create_campaign(request: Request):
    logging.info("Início do endpoint /create_campaign")
    try:
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")
        logging.debug(f"Corpo da requisição (raw): {body_str}")
        data_dict = await request.json()
        logging.debug(f"Corpo da requisição em JSON: {data_dict}")
        data = CampaignRequest(**data_dict)
        logging.info(f"Objeto CampaignRequest parseado com sucesso: {data}")
    except Exception:
        logging.exception("Erro ao ler ou parsear o corpo da requisição")
        raise HTTPException(status_code=400, detail="Erro ao ler ou parsear o corpo da requisição")

    fb_api_version = "v16.0"
    ad_account_id = data.account_id
    campaign_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/campaigns"
    logging.debug(f"URL para criação da campanha: {campaign_url}")

    total_budget_cents = int(data.budget * 100)
    logging.debug(f"Orçamento total em centavos: {total_budget_cents}")

    check_account_balance(ad_account_id, data.token, fb_api_version, total_budget_cents)

    # Criação da campanha
    campaign_payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",
        "access_token": data.token,
        "special_ad_categories": []
    }
    try:
        campaign_response = requests.post(campaign_url, json=campaign_payload)
        campaign_response.raise_for_status()
        campaign_result = campaign_response.json()
        campaign_id = campaign_result.get("id")
        logging.info(f"Campanha criada com sucesso: {campaign_result}")
    except requests.exceptions.HTTPError:
        error_msg = extract_fb_error(campaign_response)
        logging.error("Erro ao criar campanha via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar campanha: {error_msg}")

    # Cálculo de orçamento diário e datas
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

    # Definição de optimization_goal
    if data.objective == "OUTCOME_AWARENESS":
        optimization_goal = "IMPRESSIONS"
    elif data.objective in ["OUTCOME_TRAFFIC", "OUTCOME_LEADS", "OUTCOME_SALES"]:
        optimization_goal = "LINK_CLICKS"
    else:
        optimization_goal = "REACH"

    # Gender targeting
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

    page_id = get_page_id(data.token)

    # Criação do Ad Set
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
    try:
        ad_set_response = requests.post(ad_set_url, json=ad_set_payload)
        ad_set_response.raise_for_status()
        ad_set_result = ad_set_response.json()
        ad_set_id = ad_set_result.get("id")
    except requests.exceptions.HTTPError:
        error_msg = extract_fb_error(ad_set_response)
        requests.delete(f"https://graph.facebook.com/{fb_api_version}/{campaign_id}?access_token={data.token}")
        raise HTTPException(status_code=400, detail=f"Erro ao criar Ad Set: {error_msg}")

    # Criação do Ad Creative
    default_link = data.content.strip() or "https://www.adstock.ai"
    default_message = data.description

    def is_valid_image_url(url: str) -> bool:
        u = url.lower()
        return u.endswith(".jpg") or u.endswith(".jpeg") or u.endswith(".png")

    if data.video.strip():
        creative_spec = {
            "video_data": {
                "video_id": data.video.strip(),
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
    ad_creative_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adcreatives"
    try:
        ad_creative_response = requests.post(ad_creative_url, json=ad_creative_payload)
        ad_creative_response.raise_for_status()
        ad_creative_result = ad_creative_response.json()
        creative_id = ad_creative_result.get("id")
    except requests.exceptions.HTTPError:
        error_msg = extract_fb_error(ad_creative_response)
        requests.delete(f"https://graph.facebook.com/{fb_api_version}/{campaign_id}?access_token={data.token}")
        raise HTTPException(status_code=400, detail=f"Erro ao criar Ad Creative: {error_msg}")

    # Criação do Ad
    ad_payload = {
        "name": f"Ad for {data.campaign_name}",
        "adset_id": ad_set_id,
        "creative": {"creative_id": creative_id},
        "status": "ACTIVE",
        "access_token": data.token
    }
    ad_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/ads"
    try:
        ad_response = requests.post(ad_url, json=ad_payload)
        ad_response.raise_for_status()
        ad_result = ad_response.json()
        ad_id = ad_result.get("id")
    except requests.exceptions.HTTPError:
        error_msg = extract_fb_error(ad_response)
        requests.delete(f"https://graph.facebook.com/{fb_api_version}/{campaign_id}?access_token={data.token}")
        raise HTTPException(status_code=400, detail=f"Erro ao criar Ad: {error_msg}")

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
        },
        "$.metaerror": {
            "daily_value_error": "O valor diário deve ser maior que $5.76.",
            "campaign_duration_error": "A duração da campanha deve ser de pelo menos 24 horas."
        }
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Iniciando app com uvicorn no host 0.0.0.0 e porta {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
