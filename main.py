import logging
import sys
import os
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Configuração de logs detalhados
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
    account_id: str            # ID da conta do usuário no Facebook
    token: str                 # Token de 60 dias
    campaign_name: str = ""      # Nome da campanha
    content_type: str = ""       # Tipo de conteúdo (carrossel, single image, video)
    content: str = ""            # Conteúdo: URL da imagem (single image) ou vídeo
    images: list[str] = []       # Lista de URLs para imagens se for carrossel
    video: str = ""              # URL do vídeo se for campanha de vídeo
    description: str = ""        # Descrição da campanha
    keywords: str = ""           # Palavras-chave (pode ser uma string separada por vírgulas)
    budget: float = 0.0          # Orçamento total (valor numérico)
    initial_date: str = ""       # Data inicial (formato ISO ou conforme definido pela API)
    final_date: str = ""         # Data final
    pricing_model: str = ""      # Modelo de precificação (CPC, CPA, etc.)
    target_sex: str = ""         # Sexo do público-alvo (ex.: MALE, FEMALE)
    target_age: int = 0          # Idade do público-alvo (valor único ou você pode ajustar para range)
    min_salary: float = 0.0      # Salário mínimo do público-alvo
    max_salary: float = 0.0      # Salário máximo do público-alvo
    devices: list[str] = []      # Dispositivos (Desktop, Tablet, Smartphone)

@app.post("/create_campaign")
async def create_campaign(data: CampaignRequest):
    # Configuração da URL do endpoint do Facebook (ajuste a versão e os parâmetros conforme necessário)
    fb_api_version = "v16.0"
    url = f"https://graph.facebook.com/{fb_api_version}/act_{data.account_id}/campaigns"

    # Monta o payload com os dados da campanha
    payload = {
        "name": data.campaign_name,
        "objective": "LINK_CLICKS",  # Exemplo de objetivo; ajuste conforme sua necessidade
        "status": "PAUSED",          # Inicialmente pausada; ajuste se preferir outra opção
        "spend_cap": int(data.budget),  # Converte o orçamento se necessário (considere a unidade de medida exigida)
        "start_time": data.initial_date,
        "end_time": data.final_date,
        # Campos customizados – dependendo de como sua API do Facebook está configurada,
        # você pode enviar esses dados para serem utilizados na criação da campanha
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
        # Parâmetro obrigatório para autenticação na API do Facebook
        "access_token": data.token
    }

    logging.debug(f"Payload para Facebook API: {payload}")

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        result = response.json()
        logging.info(f"Campanha criada com sucesso: {result}")
        return {
            "status": "success",
            "result": result
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao criar a campanha: {e}")
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"Iniciando a aplicação com uvicorn no host 0.0.0.0 e porta {port}.")
    uvicorn.run(app, host="0.0.0.0", port=port)
