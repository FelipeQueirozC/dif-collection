#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B3 Coletor e Gerenciador de Database de Histórico (Scraping/Playwright)
Suporte: DI1 e DAP

Este script:
1. Itera sobre a lista de ativos configurados (DI1, DAP).
2. Carrega o respectivo JSON de histórico.
3. Verifica os últimos 90 dias.
4. Se faltar dados:
    a. Scraping via Playwright na página do Boletim da B3.
    b. Cálculo de taxas (DI1: vence dia 1º; DAP: vence dia 15).
5. Salva dados e apaga registros > 90 dias.
"""

from __future__ import annotations
import pandas as pd
import re
import unicodedata
from typing import List, Tuple
from io import StringIO
from datetime import date
import json
import os

# Dependências de cálculo e web
import bizdays
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except Exception as _e:
    raise ImportError(
        "Playwright é necessário. Instale com:\n"
        "  pip install playwright pandas lxml bizdays\n"
        "  python -m playwright install chromium"
    ) from _e

# --- Configuração Global ---
B3_BASE = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao1.asp"
DIAS_HISTORICO = 90 

# Configuração dos Ativos
ASSETS_CONFIG = [
    {
        'name': 'DI1',
        'code_url': 'DI1',          # Parâmetro 'Mercadoria' na URL
        'filename': 'di1_database.json',
        'maturity_day': 1           # Regra: 1º dia útil do mês
    },
    {
        'name': 'DAP',
        'code_url': 'DAP',
        'filename': 'dap_database.json',
        'maturity_day': 15          # Regra: Dia 15 (ou próximo útil)
    }
]

# --- Funções de Gerenciamento de Database ---

def carregar_database(filename: str) -> dict:
    """Carrega o arquivo JSON específico do ativo."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if "metadata" in data and "data" in data:
                    return data
                if data and all(re.match(r"^\d{4}-\d{2}-\d{2}$", k) for k in data.keys()):
                    print(f"[{filename}] Migrando database antigo...")
                    return {"metadata": {}, "data": data}
                print(f"[{filename}] Formato desconhecido. Resetando.")
                return {"metadata": {}, "data": {}}
        except json.JSONDecodeError:
            print(f"[{filename}] Corrompido. Resetando.")
    return {"metadata": {}, "data": {}}

def salvar_database(data: dict, filename: str):
    """Salva o dicionário no arquivo JSON especificado."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        print(f"[{filename}] ERRO CRÍTICO AO SALVAR: {e}")

def formatar_dados_para_json(df: pd.DataFrame, asset_prefix: str) -> List[dict]:
    """Converte o DataFrame para lista de dicts, prefixando o código (ex: DAPF25)."""
    df_copy = df.copy()

    # Adiciona o prefixo dinâmico (DI1 ou DAP)
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
        print("ERRO: Colunas ausentes no DataFrame final.")
        return []
        
    return df_copy[cols_desejadas].to_dict('records')

# --- Funções de Coleta (Scraping) ---

def _parse_input_date(s: str) -> Tuple[str, str]:
    s = (s or "").strip()
    if not s: raise ValueError("Data vazia.")
    try:
        is_dmy = '/' in s
        ts = pd.to_datetime(s, dayfirst=is_dmy)
        d = ts.date()
        return d.strftime("%d/%m/%Y"), d.isoformat()
    except Exception as e:
        raise ValueError(f"Data inválida '{s}': {e}")

def _build_url(date_dmy: str, commodity_code: str) -> str:
    """Constrói a URL com a data e a mercadoria (DI1 ou DAP)."""
    return f"{B3_BASE}?Data={date_dmy}&Mercadoria={commodity_code}"

def get_b3_tables(
    date_str: str,
    commodity_code: str,
    *,
    wait_until: str = "networkidle",
    timeout_ms: int = 20000,
    headless: bool = True,
) -> Tuple[str, List[pd.DataFrame]]:
    """
    Busca a página histórica da B3 para a mercadoria e data especificadas.
    """
    date_dmy, date_iso = _parse_input_date(date_str)
    url = _build_url(date_dmy, commodity_code)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
            page = context.new_page()

            try:
                page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            except PWTimeout as e:
                browser.close()
                raise RuntimeError(f"Timeout navegando para {url}") from e

            try:
                page.wait_for_selector("table", timeout=timeout_ms)
            except PWTimeout as e:
                # raise RuntimeError("Nenhuma tabela encontrada (Timeout).") from e
                # Se não achou tabela, pode ser feriado ou sem dados.
                # Retornamos lista vazia para tratar acima.
                browser.close()
                return "", []

            html = page.content()
            browser.close()
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Erro no Playwright: {e}") from e

    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        # Pandas não achou tabelas no HTML retornado
        return html, []
    except Exception as e:
        raise RuntimeError(f"Erro parseando tabelas: {e}") from e

    if not tables:
        return html, []

    return html, tables

# --- Funções de Processamento de Dados ---

def _norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def _clean_numeric(s: str | int | float) -> float | None:
    if isinstance(s, (int, float)): return s
    if s is None: return pd.NA
    s = str(s).strip()
    if s == "-" or s == "": return pd.NA
    if '.' in s and ',' in s: s = s.replace(".", "")
    s = s.replace(",", ".")
    try:
        return pd.to_numeric(s)
    except Exception:
        return pd.NA

def get_maturity_date(venc_code: str, calendar: bizdays.Calendar, start_day: int) -> date | None:
    """
    Calcula a data de vencimento.
    - DI1: start_day=1. Data base é dia 1. Se não for útil, próximo.
    - DAP: start_day=15. Data base é dia 15. Se não for útil, próximo.
    """
    MONTH_CODES = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    
    try:
        letter = venc_code[0].upper()
        year_short = venc_code[1:]
        
        month = MONTH_CODES[letter]
        year = int(f"20{year_short}")
        
        # Cria a data base (dia 1 ou dia 15)
        base_date = pd.Timestamp(year, month, start_day).date()
        
        # Ajusta para o próximo dia útil se necessário
        # Se base_date JÁ É útil, following retorna ele mesmo (comportamento padrão do bizdays.following no pandas, 
        # mas no bizdays puro library é bom garantir).
        # A bizdays.calendar.following retorna a data se for util, ou a proxima.
        
        if calendar.isbizday(base_date):
            return base_date
        else:
            return calendar.following(base_date)
            
    except Exception as e:
        # print(f"Erro parsing vencimento '{venc_code}': {e}")
        return None

def calculate_rates(
    combined_df: pd.DataFrame, 
    trade_date_str: str, 
    maturity_day_rule: int
) -> pd.DataFrame:
    """Calcula taxas e PUs com a regra de vencimento correta."""
    
    _, trade_date_iso = _parse_input_date(trade_date_str)
    trade_date = pd.to_datetime(trade_date_iso).date()
    cal = bizdays.Calendar.load('ANBIMA')
    
    df = combined_df.copy()

    # Normalização de colunas
    col_map = {df.columns[0]: 'VENCTO'}
    ajuste_col_name = None
    for col in df.columns:
        if str(col).strip().upper() == 'AJUSTE':
            ajuste_col_name = col
            break
            
    if not ajuste_col_name:
        # Tenta pegar pela posição se o nome falhar
        if len(df.columns) > 1:
            ajuste_col_name = df.columns[1] # Assume coluna 1 como Ajuste
        else:
            return pd.DataFrame()

    col_map[ajuste_col_name] = 'AJUSTE'
    df = df.rename(columns=col_map)
    
    df_proc = df[['VENCTO', 'AJUSTE']].copy()
    df_proc['AJUSTE_NUM'] = df_proc['AJUSTE'].apply(_clean_numeric)
    
    df_proc['TRADE_DATE'] = trade_date
    
    # Aplica regra de vencimento (DI1=1, DAP=15)
    df_proc['MATURITY_DATE'] = df_proc['VENCTO'].apply(
        lambda x: get_maturity_date(x, cal, start_day=maturity_day_rule)
    )
    
    df_proc = df_proc.dropna(subset=['MATURITY_DATE', 'AJUSTE_NUM'])
    
    df_proc['DIAS_UTEIS_N'] = df_proc.apply(
        lambda row: cal.bizdays(row['TRADE_DATE'], row['MATURITY_DATE']),
        axis=1
    )
    
    # Taxa (Base 252)
    # Obs: DAP também usa base 252 para conversão PU/Taxa no padrão de mercado de futuros.
    df_proc['TAXA_ANUAL'] = (
        (100000 / df_proc['AJUSTE_NUM']) ** (252 / df_proc['DIAS_UTEIS_N'])
    ) - 1

    df_final = df_proc[[
        'VENCTO', 'AJUSTE_NUM', 'TRADE_DATE', 'MATURITY_DATE', 'DIAS_UTEIS_N', 'TAXA_ANUAL'
    ]].copy()
    
    return df_final[df_final['DIAS_UTEIS_N'] > 0]

# --- Identificação de Tabelas (Heurística visual) ---

def _looks_like_vencto(df: pd.DataFrame) -> bool:
    if df.shape[1] != 1 or len(df) == 0: return False
    first_cell = _norm(df.iloc[0, 0])
    return "VENCTO" in first_cell

def _looks_like_ajuste_block(df: pd.DataFrame) -> bool:
    if df.shape[1] < 5 or len(df) == 0: return False # DAP as vezes tem menos colunas que DI, baixei para 5
    first_cell = _norm(df.iloc[0, 0])
    return first_cell.startswith("AJUSTE ANTER") or "AJUSTE" in first_cell

def combine_vencto_and_ajuste(tables: List[pd.DataFrame]) -> pd.DataFrame:
    if not tables: raise ValueError("No tables.")

    candidates = []
    
    # Tentativa posicional (Geralmente 6 e 7 na lista do pandas)
    if len(tables) >= 8:
        t7, t8 = tables[6], tables[7]
        if _looks_like_vencto(t7) and len(t7) == len(t8):
            candidates.append((t7.copy(), t8.copy()))

    # Busca exaustiva se posicional falhar
    if not candidates:
        vencto_idxs = [i for i, df in enumerate(tables) if _looks_like_vencto(df)]
        # Para DAP, a tabela de ajuste pode variar um pouco, pegamos a mais provavel
        ajuste_idxs = [i for i, df in enumerate(tables) if _looks_like_ajuste_block(df)]

        for i in vencto_idxs:
            for j in ajuste_idxs:
                # Geralmente ajuste vem logo depois do vencto
                if j == i + 1: 
                    df_v = tables[i].copy()
                    df_a = tables[j].copy()
                    if len(df_v) == len(df_a) and len(df_v) > 0:
                        candidates.append((df_v, df_a))
                        break
            if candidates: break

    if not candidates:
        raise ValueError("Não foi possível alinhar tabelas VENCTO e AJUSTE.")

    vencto_df, ajuste_df = candidates[0]

    # Promover cabeçalhos
    v_header = vencto_df.iloc[0]
    vencto_df = vencto_df[1:].copy()
    vencto_df.columns = v_header
    vencto_df = vencto_df.rename(columns={vencto_df.columns[0]: "VENCTO"})

    a_header = ajuste_df.iloc[0]
    ajuste_df = ajuste_df[1:].copy()
    ajuste_df.columns = a_header

    vencto_df = vencto_df.reset_index(drop=True)
    ajuste_df = ajuste_df.reset_index(drop=True)

    return pd.concat([vencto_df, ajuste_df], axis=1)

# --- Execução Principal ---

def executar_atualizacao_principal():
    print("Iniciando atualização MULTI-ATIVOS (Playwright/Scraping)...")
    
    try:
        cal = bizdays.Calendar.load('ANBIMA')
    except Exception as e:
        print(f"ERRO: Calendário ANBIMA não carregado. {e}")
        return
        
    try:
        hoje_ts = pd.Timestamp.now(tz='America/Sao_Paulo')
        hoje = hoje_ts.date()
    except Exception:
        hoje_ts = pd.Timestamp.now()
        hoje = hoje_ts.date()

    # Loop pelos Ativos
    for asset in ASSETS_CONFIG:
        nome = asset['name']
        filename = asset['filename']
        code_url = asset['code_url']
        rule_day = asset['maturity_day']

        print(f"\n--- Processando Ativo: {nome} ---")
        
        db = carregar_database(filename)
        data_historico = db.get('data', {})
        db['metadata']['last_updated'] = hoje_ts.isoformat()

        # Define datas
        data_inicio = hoje - pd.DateOffset(days=DIAS_HISTORICO - 1)
        datas_desejadas = pd.date_range(data_inicio, hoje, freq='D')
        
        alteracao = False

        for data_pd in datas_desejadas:
            data_iso = data_pd.strftime('%Y-%m-%d')
            data_date = data_pd.date()
            
            if data_iso in data_historico: continue
            
            alteracao = True
            
            if not cal.isbizday(data_date):
                # print(f"[{nome}] [{data_iso}] Fim de semana/Feriado.")
                data_historico[data_iso] = {"status": "feriado", "contratos": []}
                continue
            
            print(f"[{nome}] [{data_iso}] Coletando...")
            try:
                # Passa o código da mercadoria (DI1 ou DAP)
                _, tables = get_b3_tables(data_iso, commodity_code=code_url)
                
                if not tables:
                    # Se retornou lista vazia, Playwright não achou tabela (timeout ou vazia)
                    print(f"[{nome}] [{data_iso}] Sem tabelas encontradas.")
                    data_historico[data_iso] = {"status": "sem_dados", "contratos": []}
                    continue

                combined_df = combine_vencto_and_ajuste(tables)
                
                # Passa a regra do dia de vencimento (1 ou 15)
                df_calculado = calculate_rates(combined_df, data_iso, maturity_day_rule=rule_day)
                
                if df_calculado.empty:
                    print(f"[{nome}] [{data_iso}] Coleta OK, mas contratos vazios.")
                    data_historico[data_iso] = {"status": "dia_util", "contratos": []}
                else:
                    print(f"[{nome}] [{data_iso}] Sucesso: {len(df_calculado)} contratos.")
                    # Passa o nome para prefixar o JSON (ex: DAPF25)
                    json_data = formatar_dados_para_json(df_calculado, nome)
                    data_historico[data_iso] = {"status": "dia_util", "contratos": json_data}
            
            except Exception as e:
                print(f"[{nome}] [{data_iso}] ERRO: {e}")
                data_historico[data_iso] = {"status": "erro_coleta", "contratos": [], "erro_msg": str(e)}

        # Limpeza
        datas_iso_range = {d.strftime('%Y-%m-%d') for d in datas_desejadas}
        apagar = [k for k in data_historico if k not in datas_iso_range]
        if apagar:
            print(f"[{nome}] Limpando {len(apagar)} registros antigos.")
            for k in apagar: del data_historico[k]
            alteracao = True

        if alteracao:
            print(f"[{nome}] Salvando dados...")
            db['data'] = data_historico
            salvar_database(db, filename)
        else:
            print(f"[{nome}] Sem novos dados. Metadata atualizado.")
            salvar_database(db, filename)

    print("\nAtualização Geral Concluída.")

if __name__ == "__main__":
    executar_atualizacao_principal()
