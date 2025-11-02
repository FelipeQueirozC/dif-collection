#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B3 DI1 – Coletor de Database de Histórico (API v1 - Fallback)

Lógica de Coleta (Sensível ao Horário):
- A API sempre retorna o último ajuste calculado em 'prvsDayAdjstmntPric'.
- O script determina a data (D ou D-1) a que esse ajuste se refere.

- Cenário 1: "Pós-Fechamento" (ex: 20:01 ou Fim de Semana)
  - O ajuste na API é do dia útil mais recente (D).
  - Script salva o ajuste para a data D.

- Cenário 2: "Pregão Aberto" (ex: 10:00 em dia útil)
  - O ajuste na API é do dia útil anterior (D-1).
  - Script salva o ajuste para a data D-1 (fazendo o backfill).
"""

from __future__ import annotations
import pandas as pd
import re
import unicodedata
from typing import List, Tuple, Dict, Any
from datetime import date

# Importações
import json
import os
import requests 
import logging

# Dependências de cálculo
import bizdays

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constantes Globais ---
B3_API_URL = "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DI1"
DATABASE_FILE = "di1_database_fallback.json" 
DIAS_HISTORICO = 30
HORA_AJUSTE = 18    # 18:00
MINUTO_AJUSTE = 1   # 01 -> 18:01

# --- Funções de Gerenciamento de Database (Sem Alteração) ---

def carregar_database() -> dict:
    if os.path.exists(DATABASE_FILE):
        try:
            with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if "metadata" in data and "data" in data:
                    return data
                if data and all(re.match(r"^\d{4}-\d{2}-\d{2}$", k) for k in data.keys()):
                    logging.info("Migrando database antigo para novo formato (metadata/data)...")
                    return {"metadata": {}, "data": data}
                logging.warning(f"{DATABASE_FILE} em formato desconhecido. Começando do zero.")
                return {"metadata": {}, "data": {}}
        except json.JSONDecodeError:
            logging.warning(f"{DATABASE_FILE} corrompido. Começando do zero.")
    return {"metadata": {}, "data": {}}

def salvar_database(data: dict):
    try:
        with open(DATABASE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        logging.error(f"ERRO CRÍTICO AO SALVAR DATABASE: {e}")

def formatar_dados_para_json(df: pd.DataFrame) -> List[dict]:
    df_copy = df.copy()
    df_copy['VENCTO'] = 'DI1' + df_copy['VENCTO'].astype(str)
    df_copy['MATURITY_DATE'] = df_copy['MATURITY_DATE'].astype(str)
    df_copy['TRADE_DATE'] = df_copy['TRADE_DATE'].astype(str)
    
    df_copy = df_copy.rename(columns={
        'VENCTO': 'codigo',
        'MATURITY_DATE': 'vencimento',
        'TAXA_ANUAL': 'taxa',
        'AJUSTE_NUM': 'preco_ajuste'
    })
    
    cols_desejadas = ['codigo', 'vencimento', 'taxa', 'preco_ajuste']
    if not all(c in df_copy.columns for c in cols_desejadas):
        logging.error("O dataframe processado não tem todas as colunas esperadas.")
        return []
        
    return df_copy[cols_desejadas].to_dict('records')

# --- Novas Funções de Coleta e Processamento (API) ---

def get_run_context() -> Tuple[pd.Timestamp, bizdays.Calendar]:
    """Retorna o timestamp atual (BRL) e o calendário ANBIMA."""
    try:
        cal = bizdays.Calendar.load('ANBIMA')
    except Exception as e:
        logging.critical(f"Falha ao carregar calendário ANBIMA: {e}")
        raise
        
    try:
        agora_brl = pd.Timestamp.now(tz='America/Sao_Paulo')
    except Exception:
        agora_brl = pd.Timestamp.now() # Fallback
        
    logging.info(f"Data/hora atual: {agora_brl}")
    return agora_brl, cal


def fetch_and_process_b3_api(
    trade_date: date, 
    calendar: bizdays.Calendar,
    price_field: str # 'prvsDayAdjstmntPric'
) -> pd.DataFrame:
    """
    Busca dados da API B3 e processa a taxa de ajuste (usando o price_field)
    para o dia de pregão (trade_date).
    """
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(B3_API_URL, headers=headers, timeout=10)
        response.raise_for_status() 
        json_data = response.json()
    except requests.RequestException as e:
        logging.error(f"Falha ao buscar dados da API B3: {e}")
        return pd.DataFrame() 

    contracts = []
    
    if "Scty" not in json_data:
        logging.warning("API não retornou o array 'Scty'. Resposta vazia.")
        return pd.DataFrame()

    for item in json_data['Scty']:
        try:
            symb = item.get('symb')
            if symb == 'DI1D': continue

            venc_code_full = symb[3:]
            mtrty_date_str = item.get('asset', {}).get('AsstSummry', {}).get('mtrtyCode')
            if not mtrty_date_str: continue
            mtrty_date = pd.to_datetime(mtrty_date_str).date()

            # Usa o price_field que foi decidido (sempre será 'prvsDayAdjstmntPric')
            taxa_pct_ajuste = item.get('SctyQtn', {}).get(price_field)
            
            if taxa_pct_ajuste is not None and isinstance(taxa_pct_ajuste, (int, float)):
                taxa_anual = taxa_pct_ajuste / 100.0
                N = calendar.bizdays(trade_date, mtrty_date)
                if N > 0:
                    preco_ajuste = 100000.0 / ((1 + taxa_anual) ** (N / 252.0))
                    contracts.append({
                        'VENCTO': venc_code_full, 'MATURITY_DATE': mtrty_date,
                        'TAXA_ANUAL': taxa_anual, 'AJUSTE_NUM': preco_ajuste,
                        'TRADE_DATE': trade_date 
                    })
            
        except Exception as e:
            logging.warning(f"Falha ao processar contrato {symb}: {e}")

    if not contracts:
        logging.warning("Nenhum contrato DI1 foi processado com sucesso.")
        return pd.DataFrame()

    logging.info(f"Processados {len(contracts)} registros de ajuste para {trade_date}.")
    return pd.DataFrame(contracts)

# --- Bloco de Execução Principal (ETL Refatorado) ---

def executar_atualizacao_principal():
    """Função mestre que roda o ETL de atualização do database."""
    
    logging.info(f"Iniciando atualização do banco de dados DI1 ({DATABASE_FILE})...")
    
    try:
        agora_brl, cal = get_run_context()
        hoje = agora_brl.date()
    except Exception as e:
        logging.critical(f"Falha ao determinar a data do pregão: {e}")
        return

    # --- Etapa 1: Lógica de Decisão de Data (A LÓGICA CRÍTICA) ---
    
    is_bizday_today = cal.isbizday(hoje)
    is_after_close = (agora_brl.hour > HORA_AJUSTE) or \
                     (agora_brl.hour == HORA_AJUSTE and agora_brl.minute >= MINUTO_AJUSTE)

    # O script SEMPRE busca o 'prvsDayAdjstmntPric'.
    # A questão é: a qual dia esse preço pertence?
    price_field_to_use = 'prvsDayAdjstmntPric'
    
    if is_bizday_today and not is_after_close:
        # Cenário 2: "Pregão Aberto" (ex: 10:00 - 18:00 em dia útil)
        # O ajuste na API é do dia útil anterior (D-1).
        # Vamos salvar o ajuste para D-1 (backfill).
        date_to_save = cal.offset(hoje, -1)
        logging.info(f"MODO BACKFILL (Pregão Aberto): Tentando salvar ajuste de D-1 ({date_to_save})...")
    else:
        # Cenário 1: "Pós-Fechamento" (ex: 20:01 ou Fim de Semana)
        # O ajuste na API é do dia útil mais recente (D).
        # Vamos salvar o ajuste para D.
        date_to_save = cal.adjust_previous(hoje)
        logging.info(f"MODO PADRÃO (Pós-Fechamento): Tentando salvar ajuste de D ({date_to_save})...")

    data_iso = date_to_save.isoformat()
    
    db = carregar_database()
    data_historico = db.get('data', {})
    alteracao_detectada = False

    # --- Etapa 2: Coleta de Dados da API ---
    # Só roda a coleta se a data AINDA NÃO EXISTIR
    if data_iso not in data_historico:
        logging.info(f"[{data_iso}] É um novo dia de pregão. Coletando dados da API B3...")
        try:
            curva_di_df = fetch_and_process_b3_api(
                date_to_save, 
                cal,
                price_field_to_use
            )
            
            if curva_di_df.empty:
                logging.warning(f"[{data_iso}] Coleta falhou ou não retornou contratos.")
                data_historico[data_iso] = {"status": "erro_coleta", "contratos": [], "erro_msg": "Nenhum contrato processado."}
                alteracao_detectada = True
            else:
                # Ordena o grupo por data de vencimento antes de salvar
                group_df_sorted = curva_di_df.sort_values(by='MATURITY_DATE', ascending=True)
                json_data_novo = formatar_dados_para_json(group_df_sorted)
                
                logging.info(f"[{data_iso}] Adicionando {len(json_data_novo)} novos contratos...")
                data_historico[data_iso] = {"status": "dia_util", "contratos": json_data_novo}
                alteracao_detectada = True

        except Exception as e:
            logging.error(f"FALHA CRÍTICA NA COLETA. Erro: {e}")
            data_historico[data_iso] = {"status": "erro_coleta", "contratos": [], "erro_msg": str(e)}
            alteracao_detectada = True
            pass 
    else:
        logging.info(f"[{data_iso}] Dados já existem no database. Coleta não necessária.")

    # --- Etapa 3: Definição do Range de 30 dias ---
    data_fim_range = agora_brl.date()
    data_inicio_range = data_fim_range - pd.DateOffset(days=DIAS_HISTORICO - 1)
    datas_desejadas_range = pd.date_range(data_inicio_range, data_fim_range, freq='D')
    
    # --- Etapa 4: Preenchimento de Feriados/Fins de Semana ---
    logging.info("Verificando e preenchendo dias não-úteis ausentes...")
    for data_pd in datas_desejadas_range:
        data_iso_loop = data_pd.strftime('%Y-%m-%d')
        data_date_loop = data_pd.date()

        if data_iso_loop not in data_historico:
            if not cal.isbizday(data_date_loop):
                logging.info(f"[{data_iso_loop}] Preenchendo dia não-útil ausente (feriado/fim de semana).")
                data_historico[data_iso_loop] = {"status": "feriado", "contratos": []}
                alteracao_detectada = True

    # --- Etapa 5: Pruning (Apagar dados antigos) ---
    logging.info("Limpando dados antigos...")
    datas_iso_desejadas_set = {d.strftime('%Y-%m-%d') for d in datas_desejadas_range}
    chaves_para_apagar = [chave for chave in data_historico if chave not in datas_iso_desejadas_set]
    
    if chaves_para_apagar:
        logging.info(f"Apagando {len(chaves_para_apagar)} dias antigos: {chaves_para_apagar}")
        for chave in chaves_para_apagar:
            del data_historico[chave]
        alteracao_detectada = True

    # --- Etapa 6: Salvar ---
    db['metadata']['last_updated'] = agora_brl.isoformat()
        
    if alteracao_detectada:
        logging.info("Salvando database atualizado...")
        db['data'] = data_historico
        salvar_database(db)
    else:
        logging.info("Nenhuma alteração de dados. Apenas atualizando 'last_updated' timestamp.")
        salvar_database(db) 
    
    logging.info("Atualização concluída.")

if __name__ == "__main__":
    executar_atualizacao_principal()