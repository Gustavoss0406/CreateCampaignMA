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
            # Remove espaços e pontos-e-vírgulas ao final de cada URL, se existirem
            cleaned = [s.strip().rstrip(";") if isinstance(s, str) else s for s in v]
            logging.debug(f"URLs de imagens após limpeza: {cleaned}")
            return cleaned
        return v

@app.post("/create_campaign")
async def create_campaign(request: Request):
    try:
        # Captura o corpo bruto da requisição e exibe no log
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")
        logging.debug(f"Raw request body: {body_str}")

        # Parse do JSON e log dos dados
        data_dict = await request.json()
        logging.debug(f"Parsed request body as JSON: {data_dict}")

        # Validação com o modelo Pydantic
        data = CampaignRequest(**data_dict)
        logging.debug(f"CampaignRequest parsed: {data}")
    except Exception as e:
        logging.exception("Erro ao ler ou parsear o corpo da requisição")
        raise HTTPException(status_code=400, detail=f"Erro no corpo da requisição: {str(e)}")

    # Configura a URL da API do Facebook (ajuste conforme necessário)
    fb_api_version = "v16.0"
    url = f"https://graph.facebook.com/{fb_api_version}/act_{data.account_id}/campaigns"
    
    # Converte o orçamento para a menor unidade da moeda (centavos)
    spend_cap = int(data.budget * 100)
    
    # Monta o payload com os dados para a API do Facebook, incluindo "special_ad_categories"
    # Status definido como "ACTIVE" para ativar a campanha automaticamente
    payload = {
        "name": data.campaign_name,
        "objective": data.objective,
        "status": "ACTIVE",           # Campanha ativada automaticamente
        "spend_cap": spend_cap,       # Valor em centavos
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
        "special_ad_categories": []  # Parâmetro obrigatório; lista vazia se não houver categoria especial
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
    except requests.exceptions.RequestException as e:
        logging.error("Erro ao criar a campanha via API do Facebook", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Erro ao criar a campanha: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Iniciando a aplicação com uvicorn no host 0.0.0.0 e porta {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
