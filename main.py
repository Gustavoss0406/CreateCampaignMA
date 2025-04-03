import logging
import sys
import os
import requests
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

def check_account_balance(account_id: str, token: str, fb_api_version: str, spend_cap: int):
    """
    Verifica se a conta possui saldo suficiente.
    Se o campo 'balance' não for encontrado ou se for menor que spend_cap,
    lança uma exceção com status 402 (Payment Required).
    """
    url = f"https://graph.facebook.com/{fb_api_version}/act_{account_id}?fields=balance&access_token={token}"
    logging.debug(f"Verificando saldo da conta: {url}")
    response = requests.get(url)
    logging.debug(f"Status Code da verificação de saldo: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.debug(f"Resposta da verificação de saldo: {data}")
        if "balance" in data:
            try:
                balance = int(data["balance"])
            except Exception as e:
                logging.error("Erro ao converter o saldo para inteiro.", exc_info=True)
                raise HTTPException(status_code=402, detail="Saldo insuficiente para a campanha")
            logging.debug(f"Saldo atual da conta: {balance}")
            if balance < spend_cap:
                raise HTTPException(status_code=402, detail="Saldo insuficiente para a campanha")
        else:
            logging.error("Campo 'balance' não encontrado na resposta da conta. Considerando saldo insuficiente.")
            raise HTTPException(status_code=402, detail="Saldo insuficiente para a campanha")
    else:
        logging.error("Erro ao verificar saldo da conta.")
        raise HTTPException(status_code=400, detail="Erro ao verificar saldo da conta")

class CampaignRequest(BaseModel):
    account_id: str            # ID da conta do Facebook
    token: str                 # Token de 60 dias
    campaign_name: str = ""      # Nome da campanha
    objective: str = "OUTCOME_TRAFFIC"  # Objetivo da campanha (valor padrão se não for enviado)
    content_type: str = ""       # Tipo de conteúdo (carrossel, single image, video)
    content: str = ""            # URL da imagem (para single image) ou outro conteúdo
    images: list[str] = []       # Lista de URLs (para carrossel)
    video: str = ""              # URL do vídeo (caso seja campanha de vídeo)
    description: str = ""        # Descrição da campanha
    keywords: str = ""           # Palavras-chave (pode ser uma string separada por vírgulas)
    budget: float = 0.0          # Orçamento total
    initial_date: str = ""       # Data inicial (formato ISO ou conforme definido)
    final_date: str = ""         # Data final
    pricing_model: str = ""      # Modelo de precificação (CPC, CPA, etc.)
    target_sex: str = ""         # Sexo do público-alvo (ex.: Male, Female, All)
    target_age: int = 0          # Idade do público-alvo (valor numérico)
    min_salary: float = 0.0      # Salário mínimo do público-alvo
    max_salary: float = 0.0      # Salário máximo do público-alvo
    devices: list[str] = []      # Dispositivos (ex.: ["Smartphone", "Desktop"])

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
    url = f"https://graph.facebook.com/{fb_api_version}/act_{data.account_id}/campaigns"
    
    spend_cap = int(data.budget * 100)
    
    # Verifica o saldo da conta
    check_account_balance(data.account_id, data.token, fb_api_version, spend_cap)
    
    payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",         # Campanha ativada automaticamente
        "spend_cap": spend_cap,     # Valor em centavos
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
    
    logging.debug(f"Payload enviado para a API do Facebook: {payload}")
    
    try:
        response = requests.post(url, json=payload)
        logging.debug(f"Status Code da resposta do Facebook: {response.status_code}")
        logging.debug(f"Conteúdo da resposta do Facebook: {response.text}")
        response.raise_for_status()
        result = response.json()
        logging.info(f"Campanha criada com sucesso: {result}")
        return {
            "status": "success",
            "received_body": data_dict,
            "facebook_response": result
        }
    except requests.exceptions.HTTPError as e:
        logging.error("Erro ao criar a campanha via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar a campanha: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Iniciando a aplicação com uvicorn no host 0.0.0.0 e porta {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
