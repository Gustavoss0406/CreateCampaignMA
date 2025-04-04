import logging
import sys
import os
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

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

# Exception handler que intercepta erros de validação
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Se o erro de validação for no campo 'budget', retorna apenas "Budget Inválido".
    Caso contrário, retorna uma mensagem de erro de validação genérico.
    """
    for error in exc.errors():
        # Verifica se o campo que falhou é 'budget'
        if "budget" in error.get("loc", []):
            return JSONResponse(
                status_code=400,
                content={"detail": "Budget Inválido"}
            )
    # Se não for no campo 'budget', mostra uma mensagem de erro de validação genérico
    return JSONResponse(
        status_code=400,
        content={"detail": "Erro de validação genérico"}
    )

class CampaignRequest(BaseModel):
    budget: float = 0.0

    @field_validator("budget", mode="before")
    def parse_budget(cls, v):
        if isinstance(v, str):
            v_clean = v.replace("$", "").replace(" ", "").replace(",", ".")
            try:
                parsed = float(v_clean)
                return parsed
            except Exception:
                # Lança ValueError para acionar o RequestValidationError do Pydantic
                raise ValueError("Budget Inválido")
        return v

@app.post("/test")
async def test_endpoint(data: CampaignRequest):
    return {"status": "success", "budget": data.budget}
