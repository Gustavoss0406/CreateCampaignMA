import logging
import sys
import os
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator

# Configuração detalhada de logs para debug
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ajuste conforme necessário
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lista representativa de códigos de país válidos (formato ISO) para direcionamento global
GLOBAL_COUNTRIES = [
    "US", "CA", "GB", "AU", "DE", "FR", "BR", "IN", "MX", "IT",
    "ES", "NL", "SE", "NO", "DK", "FI", "CH", "JP", "KR", "SG"
]

# Plataformas válidas para posicionamento (publisher_platforms)
PUBLISHER_PLATFORMS = ["facebook", "instagram", "audience_network", "messenger"]

def get_page_id(token: str) -> str:
    """
    Obtém o ID da primeira página disponível a partir do token.
    Se não encontrar nenhuma página, retorna o código 533.
    """
    url = f"https://graph.facebook.com/v16.0/me/accounts?access_token={token}"
    logging.debug(f"Buscando páginas disponíveis: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Resposta de páginas: {data}")
        if "data" in data and len(data["data"]) > 0:
            page_id = data["data"][0]["id"]
            logging.debug(f"Página selecionada: {page_id}")
            return page_id
        else:
            raise HTTPException(status_code=533, detail="Nenhuma página disponível para uso")
    else:
        raise HTTPException(status_code=response.status_code, detail="Erro ao buscar páginas disponíveis")

class CampaignRequest(BaseModel):
    account_id: str             # ID da conta do Facebook
    token: str                  # Token de 60 dias
    campaign_name: str = ""     # Nome da campanha
    objective: str = "OUTCOME_TRAFFIC"  # Objetivo da campanha (valor padrão se não for enviado)
    content_type: str = ""      # Tipo de conteúdo (carrossel, single image, video)
    content: str = ""           # URL da imagem (para single image) ou outro conteúdo
    images: list[str] = []      # Lista de URLs (para carrossel)
    video: str = ""             # URL do vídeo (caso seja campanha de vídeo)
    description: str = ""       # Descrição da campanha (usada na mensagem do anúncio)
    keywords: str = ""          # Palavras-chave (usadas como legenda no anúncio)
    budget: float = 0.0         # Orçamento total (em dólares, ex.: "$300.00")
    initial_date: str = ""      # Data inicial (ex.: "04/03/2025")
    final_date: str = ""        # Data final (ex.: "04/04/2025")
    pricing_model: str = ""     # Modelo de precificação (CPC, CPA, etc.)
    target_sex: str = ""        # Público-alvo (ex.: "Male", "Female", "All")
    target_age: int = 0         # Faixa etária (se informado como valor único)
    min_salary: float = 0.0     # Salário mínimo
    max_salary: float = 0.0     # Salário máximo
    devices: list[str] = []     # Dispositivos (campo enviado, mas não usado no targeting)

    @field_validator("objective", mode="before")
    def validate_objective(cls, v):
        mapping = {
            "Brand Awareness": "OUTCOME_AWARENESS",
            "Sales": "OUTCOME_SALES",
            "Leads": "OUTCOME_LEADS"
        }
        if isinstance(v, str) and v in mapping:
            converted = mapping[v]
            logging.debug(f"Objective convertido de '{v}' para '{converted}'")
            return converted
        return v

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                logging.debug(f"Budget convertido: {parsed}")
                return parsed
            except Exception as e:
                raise ValueError(f"Budget inválido: {v}") from e
        return v

    @field_validator("min_salary", mode="before")
    def parse_min_salary(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                logging.debug(f"min_salary convertido: {parsed}")
                return parsed
            except Exception as e:
                raise ValueError(f"min_salary inválido: {v}") from e
        return v

    @field_validator("max_salary", mode="before")
    def parse_max_salary(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                logging.debug(f"max_salary convertido: {parsed}")
                return parsed
            except Exception as e:
                raise ValueError(f"max_salary inválido: {v}") from e
        return v

    @field_validator("images", mode="before")
    def clean_images(cls, v):
        if isinstance(v, list):
            cleaned = [s.strip().rstrip(";") if isinstance(s, str) else s for s in v]
            logging.debug(f"URLs de imagens após limpeza: {cleaned}")
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
        logging.exception("Erro ao ler ou parsear o corpo da requisição")
        raise HTTPException(status_code=400, detail=f"Erro no corpo da requisição: {str(e)}")
    
    fb_api_version = "v16.0"
    ad_account_id = data.account_id
    campaign_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/campaigns"
    
    # Converte o orçamento para centavos
    spend_cap = int(data.budget * 100)
    
    # --- Criação da Campanha ---
    campaign_payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",         # Campanha ativada automaticamente
        "spend_cap": spend_cap,
        "start_time": data.initial_date,
        "end_time": data.final_date,
        "content_type": data.content_type,
        "content": data.content,
        "images": data.images,
        "video": data.video,
        "description": data.description,
        "keywords": data.keywords,
        "pricing_model": data.pricing_model,
        "target_sex": data.target_sex,
        "target_age": data.target_age,
        "min_salary": data.min_salary,
        "max_salary": data.max_salary,
        "devices": data.devices,
        "access_token": data.token,
        "special_ad_categories": []
    }
    
    logging.debug(f"Payload da Campanha: {campaign_payload}")
    
    try:
        campaign_response = requests.post(campaign_url, json=campaign_payload)
        logging.debug(f"Status da resposta da Campanha: {campaign_response.status_code}")
        logging.debug(f"Conteúdo da resposta da Campanha: {campaign_response.text}")
        campaign_response.raise_for_status()
        campaign_result = campaign_response.json()
        campaign_id = campaign_result.get("id")
        logging.info(f"Campanha criada com sucesso: {campaign_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Erro ao criar a campanha via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar a campanha: {str(e)}")
    
    # --- Criação do Ad Set ---
    if data.target_sex.lower() == "male":
        genders = [1]
    elif data.target_sex.lower() == "female":
        genders = [2]
    else:
        genders = []
    
    try:
        start_dt = datetime.strptime(data.initial_date, "%m/%d/%Y")
        end_dt = datetime.strptime(data.final_date, "%m/%d/%Y")
        if (end_dt - start_dt) < timedelta(hours=25):
            end_dt = start_dt + timedelta(hours=25)
        ad_set_start = int(start_dt.timestamp())
        ad_set_end = int(end_dt.timestamp())
    except Exception as e:
        logging.warning("Não foi possível processar as datas; usando valores originais como timestamps.")
        ad_set_start = data.initial_date
        ad_set_end = data.final_date
    
    targeting_spec = {
        "geo_locations": {"countries": GLOBAL_COUNTRIES},
        "genders": genders,
        "age_min": data.target_age,
        "age_max": data.target_age,
        "publisher_platforms": PUBLISHER_PLATFORMS
    }
    
    # Obtemos o ID da página disponível para uso nos campos DSA
    page_id = get_page_id(data.token)
    
    ad_set_payload = {
        "name": f"Ad Set for {data.campaign_name}",
        "campaign_id": campaign_id,
        "daily_budget": spend_cap,
        "billing_event": "IMPRESSIONS",
        "optimization_goal": "REACH",
        "bid_amount": 100,
        "targeting": targeting_spec,
        "start_time": ad_set_start,
        "end_time": ad_set_end,
        "dsa_beneficiary": page_id,
        "dsa_payor": page_id,
        "access_token": data.token
    }
    
    ad_set_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/adsets"
    logging.debug(f"Payload do Ad Set: {ad_set_payload}")
    
    try:
        ad_set_response = requests.post(ad_set_url, json=ad_set_payload)
        logging.debug(f"Status da resposta do Ad Set: {ad_set_response.status_code}")
        logging.debug(f"Conteúdo da resposta do Ad Set: {ad_set_response.text}")
        ad_set_response.raise_for_status()
        ad_set_result = ad_set_response.json()
        ad_set_id = ad_set_result.get("id")
        logging.info(f"Ad Set criado com sucesso: {ad_set_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Erro ao criar o Ad Set via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar o Ad Set: {str(e)}")
    
    # --- Criação do Ad Creative ---
    # Construímos o payload do Ad Creative, removendo o campo caption se não for uma URL.
    link_data = {
        "message": data.description,
        "link": data.content if data.content else "https://www.example.com",
        "picture": data.images[0] if data.images else ""
    }
    # Se data.keywords for uma URL, incluímos; caso contrário, omitimos
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
    logging.debug(f"Payload do Ad Creative: {ad_creative_payload}")
    
    try:
        ad_creative_response = requests.post(ad_creative_url, json=ad_creative_payload)
        logging.debug(f"Status da resposta do Ad Creative: {ad_creative_response.status_code}")
        logging.debug(f"Conteúdo da resposta do Ad Creative: {ad_creative_response.text}")
        ad_creative_response.raise_for_status()
        ad_creative_result = ad_creative_response.json()
        creative_id = ad_creative_result.get("id")
        logging.info(f"Ad Creative criado com sucesso: {ad_creative_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Erro ao criar o Ad Creative via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar o Ad Creative: {str(e)}")
    
    # --- Criação do Anúncio ---
    ad_payload = {
        "name": f"Ad for {data.campaign_name}",
        "adset_id": ad_set_id,
        "creative": {"creative_id": creative_id},
        "status": "ACTIVE",
        "access_token": data.token
    }
    
    ad_url = f"https://graph.facebook.com/{fb_api_version}/act_{ad_account_id}/ads"
    logging.debug(f"Payload do Anúncio: {ad_payload}")
    
    try:
        ad_response = requests.post(ad_url, json=ad_payload)
        logging.debug(f"Status da resposta do Anúncio: {ad_response.status_code}")
        logging.debug(f"Conteúdo da resposta do Anúncio: {ad_response.text}")
        ad_response.raise_for_status()
        ad_result = ad_response.json()
        ad_id = ad_result.get("id")
        logging.info(f"Anúncio criado com sucesso: {ad_result}")
    except requests.exceptions.HTTPError as e:
        logging.error("Erro ao criar o Anúncio via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar o Anúncio: {str(e)}")
    
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
    logging.info(f"Iniciando a aplicação com uvicorn no host 0.0.0.0 e porta {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
