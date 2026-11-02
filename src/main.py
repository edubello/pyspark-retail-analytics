"""
Módulo de Ingestão e Sanitização de Dados Brutos (Raw Layer)
Projeto: Retail Analytics Lakehouse (PySpark & Delta Lake)
Autor: Eduardo de Castro Bello

Responsável por verificar fontes brutas, aplicar correções estruturais iniciais 
(como sanitização de JSON malformado) e preparar os dados para a camada Silver.
"""

import yaml
from pathlib import Path


def load_config() -> dict:
    """Carrega o arquivo de configuração settings.yaml."""
    config_path = Path("config/settings.yaml") if Path("config/settings.yaml").exists() else Path("../config/settings.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def sanitize_products_json(raw_dir: str) -> None:
    """
    Realiza a inspeção e sanitização do arquivo products.json bruto,
    corrigindo eventuais problemas de formatação de array (ex: colchetes ausentes).
    """
    products_file = Path(raw_dir) / "products.json"
    
    if not products_file.exists():
        print(f"[WARNING] Arquivo não encontrado: {products_file}")
        return

    raw_text = products_file.read_text(encoding="utf-8").strip()
    
    # Corrige JSON caso não comece com colchete (array malformado)
    if not raw_text.startswith("["):
        sanitized_text = "[" + raw_text.rstrip(",") + "]"
        products_file.write_text(sanitized_text, encoding="utf-8")
        print("[INFO] Arquivo products.json sanitizado com sucesso (colchetes ajustados).")
    else:
        print("[INFO] Arquivo products.json já está estruturado corretamente.")


def run_ingestion_pipeline() -> None:
    """Executa o processo de ingestão e preparação da camada raw."""
    settings = load_config()
    raw_dir = settings["paths"]["raw_data"]
    
    print(f"[INFO] Iniciando pipeline de ingestão. Diretório Raw: {raw_dir}")
    
    # Garante que o diretório raw existe
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    
    # Sanitiza as fontes que necessitam de tratamento prévio
    sanitize_products_json(raw_dir)
    print("[SUCCESS] Processo de ingestão da Camada Raw concluído!")


if __name__ == "__main__":
    run_ingestion_pipeline()