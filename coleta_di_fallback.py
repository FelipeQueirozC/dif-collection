#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B3 Coletor de Database de Histórico (API v1 - Fallback) - DI1 e DAP

Lógica de Coleta (Sensível ao Horário):
- Itera sobre lista de ativos (DI1, DAP).
- A API sempre retorna o último ajuste calculado em 'prvsDayAdjstmntPric'.
- O script determina a data (D ou D-1) a que esse ajuste se refere.
- Salva em arquivos JSON separados (ex: di1_fallback.json, dap_fallback.json).
- Mantém histórico de 90 dias (configurável).
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

# --- Constantes e Configurações ---
DIAS_HISTORICO = 90
HORA_AJUSTE = 18    # 18:00
MINUTO_AJUSTE = 1   # 01 -> 18:01

# Configuração dos Ativos
ASSETS_CONFIG = [
    {
        'name': 'DI1',
        'filename': 'di1_database_fallback.json',
        'url': "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DI1",
        'ignore_symb': ['DI1D']
    },
    {
        'name': 'DAP',
        'filename': 'dap_database_fallback.json', # Arquivo separado para DAP
        'url': "https://cotacao.b3.com.br/mds/api/v1/DerivativeQuotation/DAP",
        'ignore_symb': []
    }
]

# --- Funções de Gerenciamento de Database ---

def carregar_database(filename: str) -> dict:
    """Carrega o JSON específico do ativo."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if "metadata" in data and "data" in data:
                    return data
                # Migração simples se formato antigo
                if data and all(re.match(r"^\d{4}-\d{2}-\d{2}$", k) for k in data.keys()):
                    logging.info(f"[{filename}] Migrando formato antigo...")
                    return {"metadata": {}, "data": data}
                logging.warning(f"[{filename}] Formato desconhecido. Resetando.")
                return {"metadata": {}, "data": {}}
        except json.JSONDecodeError:
            logging.warning(f"[{filename}] Arquivo corrompido. Resetando.")
    return {"metadata": {}, "data": {}}

def salvar_database(data: dict, filename: str):
    """Salva o JSON específico do ativo."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        logging.error(f"[{filename}] ERRO CRÍTICO AO SALVAR: {e}")

def formatar_dados_para_json(df: pd.DataFrame, asset_prefix: str) -> List[dict]:
    """
    Formata o DataFrame para lista de dicionários.
    Adiciona o prefixo (DI1 ou DAP) ao código do contrato.
    """
    df_copy = df.copy()
    # Cria o código completo, ex: DI1F25 ou DAPK24
    df_copy['VENCTO'] = asset_prefix + df_copy['VENCTO'].astype(str)
    
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
        agora_brl = pd.Timestamp.now()
        
    logging.info(f"Data/hora atual: {agora_brl}")
    return agora_brl, cal


def fetch_and_process_b3_api(
    trade_date: date, 
    calendar: bizdays.Calendar,
    price_field: str,
    asset_url: str,
    ignore_list: List[str]
) -> pd.DataFrame:
    """
    Busca dados da API B3 para uma URL específica e processa.
    """
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(asset_url, headers=headers, timeout=10)
        response.raise_for_status() 
        json_data = response.json()
    except requests.RequestException as e:
        logging.error(f"Falha ao buscar dados da API ({asset_url}): {e}")
        return pd.DataFrame() 

    contracts = []
    
    if "Scty" not in json_data:
        logging.warning("API não retornou o array 'Scty'. Resposta vazia.")
        return pd.DataFrame()

    for item in json_data['Scty']:
        try:
            symb = item.get('symb')
            if symb in ignore_list: continue

            # Lógica Genérica: Ticker tem 3 letras (DI1 ou DAP) + Código Vencimento
            venc_code_full = symb[3:]
            
            mtrty_date_str = item.get('asset', {}).get('AsstSummry', {}).get('mtrtyCode')
            if not mtrty_date_str: continue
            mtrty_date = pd.to_datetime(mtrty_date_str).date()

            # Usa o price_field (prvsDayAdjstmntPric)
            taxa_pct_ajuste = item.get('SctyQtn', {}).get(price_field)
            
            if taxa_pct_ajuste is not None and isinstance(taxa_pct_ajuste, (int, float)):
                taxa_anual = taxa_pct_ajuste / 100.0
                N = calendar.bizdays(trade_date, mtrty_date)
                
                # Cálculo PU
                if N > 0:
                    preco_ajuste = 100000.0 / ((1 + taxa_anual) ** (N / 252.0))
                    contracts.append({
                        'VENCTO': venc_code_full, 'MATURITY_DATE': mtrty_date,
                        'TAXA_ANUAL': taxa_anual, 'AJUSTE_NUM': preco_ajuste,
                        'TRADE_DATE': trade_date 
                    })
            
        except Exception as e:
            # logging.debug(f"Pulo no contrato {symb}: {e}")
            pass

    if not contracts:
        return pd.DataFrame()

    return pd.DataFrame(contracts)

# --- Bloco de Execução Principal (ETL Refatorado) ---

def executar_atualizacao_principal():
    
    logging.info("Iniciando atualização MULTI-ATIVOS (JSON Fallback)...")
    
    try:
        agora_brl, cal = get_run_context()
        hoje = agora_brl.date()
    except Exception as e:
        logging.critical(f"Falha ao determinar a data do pregão: {e}")
        return

    # --- Etapa 1: Lógica de Decisão de Data (Única para todos) ---
    is_bizday_today = cal.isbizday(hoje)
    is_after_close = (agora_brl.hour > HORA_AJUSTE) or \
                     (agora_brl.hour == HORA_AJUSTE and agora_brl.minute >= MINUTO_AJUSTE)

    price_field_to_use = 'prvsDayAdjstmntPric'
    
    if is_bizday_today and not is_after_close:
        date_to_save = cal.offset(hoje, -1)
        mode_msg = "BACKFILL (Pregão Aberto -> D-1)"
    else:
        date_to_save = cal.adjust_previous(hoje)
        mode_msg = "PADRÃO (Pós-Fechamento -> D)"

    data_iso = date_to_save.isoformat()
    logging.info(f"Modo: {mode_msg}. Data alvo: {data_iso}")

    # --- Loop pelos Ativos ---
    for asset in ASSETS_CONFIG:
        nome = asset['name']
        arquivo = asset['filename']
        url = asset['url']
        ignore = asset['ignore_symb']

        logging.info(f"--- Processando: {nome} (Arquivo: {arquivo}) ---")
        
        db = carregar_database(arquivo)
        data_historico = db.get('data', {})
        alteracao_detectada = False

        # --- Etapa 2: Coleta (Se necessário) ---
        if data_iso not in data_historico:
            logging.info(f"[{nome}] Coletando dados da API...")
            try:
                df_ativo = fetch_and_process_b3_api(
                    date_to_save, 
                    cal,
                    price_field_to_use,
                    url,
                    ignore
                )
                
                if df_ativo.empty:
                    logging.warning(f"[{nome}] Coleta falhou ou vazia.")
                    data_historico[data_iso] = {"status": "erro_coleta", "contratos": [], "erro_msg": "Vazio"}
                    alteracao_detectada = True
                else:
                    df_sorted = df_ativo.sort_values(by='MATURITY_DATE', ascending=True)
                    # Formata passando o prefixo do ativo
                    json_data_novo = formatar_dados_para_json(df_sorted, nome)
                    
                    logging.info(f"[{nome}] Adicionando {len(json_data_novo)} contratos...")
                    data_historico[data_iso] = {"status": "dia_util", "contratos": json_data_novo}
                    alteracao_detectada = True

            except Exception as e:
                logging.error(f"[{nome}] Erro na coleta: {e}")
                data_historico[data_iso] = {"status": "erro_coleta", "contratos": [], "erro_msg": str(e)}
                alteracao_detectada = True
        else:
            logging.info(f"[{nome}] Dados já atualizados para {data_iso}.")

        # --- Etapa 3 e 4: Range de Dias e Preenchimento ---
        # Recalcula o range a cada loop (rápido) ou usa o calculado fora
        data_fim_range = agora_brl.date()
        data_inicio_range = data_fim_range - pd.DateOffset(days=DIAS_HISTORICO - 1)
        datas_desejadas_range = pd.date_range(data_inicio_range, data_fim_range, freq='D')

        for data_pd in datas_desejadas_range:
            data_iso_loop = data_pd.strftime('%Y-%m-%d')
            data_date_loop = data_pd.date()

            if data_iso_loop not in data_historico:
                if not cal.isbizday(data_date_loop):
                    # logging.info(f"[{nome}] Preenchendo feriado/fim de semana: {data_iso_loop}")
                    data_historico[data_iso_loop] = {"status": "feriado", "contratos": []}
                    alteracao_detectada = True

        # --- Etapa 5: Pruning (Limpeza) ---
        datas_iso_desejadas_set = {d.strftime('%Y-%m-%d') for d in datas_desejadas_range}
        chaves_para_apagar = [k for k in data_historico if k not in datas_iso_desejadas_set]
        
        if chaves_para_apagar:
            logging.info(f"[{nome}] Limpando {len(chaves_para_apagar)} dias antigos.")
            for k in chaves_para_apagar:
                del data_historico[k]
            alteracao_detectada = True

        # --- Etapa 6: Salvar ---
        db['metadata']['last_updated'] = agora_brl.isoformat()
        if alteracao_detectada:
            logging.info(f"[{nome}] Salvando alterações no disco.")
            db['data'] = data_historico
            salvar_database(db, arquivo)
        else:
            logging.info(f"[{nome}] Sem alterações de dados. Atualizando metadata.")
            salvar_database(db, arquivo)

    logging.info("Atualização concluída.")

if __name__ == "__main__":
    executar_atualizacao_principal()

