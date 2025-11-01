#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B3 DI1 – Coletor e Gerenciador de Database de Histórico (30 dias)

Este script:
1. Carrega um 'di1_database.json' existente.
2. Verifica os últimos 30 dias.
3. Para cada dia que falta no JSON:
    a. Se for feriado/fim de semana, marca como "feriado".
    b. Se for dia útil, tenta baixar os dados da B3.
4. Salva os dados coletados e o status no JSON.
5. Apaga quaisquer dados com mais de 30 dias.
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

# --- Constantes Globais ---
B3_BASE = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao1.asp"
DATABASE_FILE = "di1_database.json"
DIAS_HISTORICO = 30 # Manter os últimos 30 dias corridos

# --- Funções de Gerenciamento de Database ---

def carregar_database() -> dict:
    """Carrega o arquivo JSON do disco. Se não existir, retorna a estrutura padrão."""
    if os.path.exists(DATABASE_FILE):
        try:
            with open(DATABASE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Se o arquivo já tem o formato novo, retorne-o
                if "metadata" in data and "data" in data:
                    return data
                
                # Se o arquivo é antigo (só tem datas na raiz), migra
                if data and all(re.match(r"^\d{4}-\d{2}-\d{2}$", k) for k in data.keys()):
                    print("Migrando database antigo para novo formato (metadata/data)...")
                    return {"metadata": {}, "data": data}
                
                # Se o arquivo está vazio ou em formato desconhecido, reseta
                print(f"Aviso: {DATABASE_FILE} em formato desconhecido. Começando do zero.")
                return {"metadata": {}, "data": {}}
                
        except json.JSONDecodeError:
            print(f"Aviso: {DATABASE_FILE} corrompido. Começando do zero.")
            
    # Retorna a estrutura padrão se o arquivo não existir
    return {"metadata": {}, "data": {}}

def salvar_database(data: dict):
    """Salva o dicionário no arquivo JSON com formatação."""
    try:
        with open(DATABASE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        print(f"ERRO CRÍTICO AO SALVAR DATABASE: {e}")

def formatar_dados_para_json(df: pd.DataFrame) -> List[dict]:
    """Converte o DataFrame final para o formato JSON desejado."""
    
    # O JSON não lida bem com tipos de data nativos
    df_copy = df.copy()

    # --- INÍCIO DA MODIFICAÇÃO ---
    # Adiciona o prefixo "DI1" ao código (ex: F26 -> DI1F26)
    # A coluna original chama-se 'VENCTO' no DataFrame que recebemos.
    df_copy['VENCTO'] = 'DI1' + df_copy['VENCTO'].astype(str)
    # --- FIM DA MODIFICAÇÃO ---
    
    df_copy['MATURITY_DATE'] = df_copy['MATURITY_DATE'].astype(str)
    df_copy['TRADE_DATE'] = df_copy['TRADE_DATE'].astype(str)
    
    # Renomeia colunas para o formato pedido
    df_copy = df_copy.rename(columns={
        'VENCTO': 'codigo',
        'MATURITY_DATE': 'vencimento',
        'TAXA_ANUAL': 'taxa',
        'AJUSTE_NUM': 'preco_ajuste'
    })
    
    # Seleciona e converte
    cols_desejadas = ['codigo', 'vencimento', 'taxa', 'preco_ajuste']
    
    # Garante que temos todas as colunas
    if not all(c in df_copy.columns for c in cols_desejadas):
        print("ERRO: O dataframe processado não tem todas as colunas esperadas.")
        return []
        
    return df_copy[cols_desejadas].to_dict('records')

# --- Funções de Coleta (Scraping) ---

def _parse_input_date(s: str) -> Tuple[str, str]:
    """
    Accepts 'YYYY-MM-DD' or 'DD/MM/YYYY' using pandas.
    Returns (date_dmy 'DD/MM/YYYY', date_iso 'YYYY-MM-DD').
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("data_str não pode estar vazia.")
    
    try:
        # pd.to_datetime é flexível.
        # Se houver '/', assume que o dia vem primeiro (DD/MM/YYYY).
        is_dmy = '/' in s
        ts = pd.to_datetime(s, dayfirst=is_dmy)
        
        d = ts.date() # Extrai o objeto datetime.date
        return d.strftime("%d/%m/%Y"), d.isoformat()
    except Exception as e:
        raise ValueError(f"Formato de data inválido '{s}'. Erro: {e}")


def _build_url(date_dmy: str) -> str:
    return f"{B3_BASE}?Data={date_dmy}&Mercadoria=DI1"


def get_b3_di1_tables(
    date_str: str,
    *,
    wait_until: str = "networkidle",
    timeout_ms: int = 20000,
    headless: bool = True,
) -> Tuple[str, List[pd.DataFrame]]:
    """
    Fetch the B3 DI1 historical page for a given date and return raw tables.

    Parameters
    ----------
    date_str : str
        Target date, "YYYY-MM-DD" or "DD/MM/YYYY".
    wait_until : {"load","domcontentloaded","networkidle","commit"}
        Playwright navigation wait strategy (default "networkidle").
    timeout_ms : int
        Navigation and selector timeout in milliseconds (default 20000).
    headless : bool
        Whether to run Chromium headless (default True).

    Returns
    -------
    (html_snapshot_str, tables) : (str, List[pandas.DataFrame])
        html_snapshot_str : the full rendered HTML of the page
        tables            : list of DataFrames extracted by pandas.read_html(html)

    Raises
    ------
    ValueError
        If the input date format is invalid or no tables are found.
    RuntimeError
        For navigation/timeout errors or unexpected failures.
    """
    # 1) Normalize/validate date formats
    date_dmy, date_iso = _parse_input_date(date_str)
    url = _build_url(date_dmy)

    # 2) Launch browser and navigate
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ))
            page = context.new_page()

            try:
                page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            except PWTimeout as e:
                browser.close()
                raise RuntimeError(
                    f"Timeout navigating to {url} "
                    f"(wait_until={wait_until}, timeout_ms={timeout_ms})."
                ) from e

            # Wait for at least one table to be present
            try:
                page.wait_for_selector("table", timeout=timeout_ms)
            except PWTimeout as e:
                html = page.content()
                browser.close()
                # Return the HTML to help debugging, but also raise (no tables ready)
                raise RuntimeError(
                    "No <table> found on the page before timeout. "
                    "Inspect the returned HTML to adjust parsing/waits."
                ) from e

            html = page.content()
            browser.close()
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Playwright session failed: {e}") from e

    # 3) Parse ALL tables as-is using pandas
    try:
        # tables = pd.read_html(html)  # no transformations; raw as-is
        tables = pd.read_html(StringIO(html)) # <--- LINHA CORRIGIDA
    except ValueError as e:
        # pandas raises ValueError("No tables found")
        raise ValueError(f"pandas could not find tables in the HTML ({date_iso}).") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error while parsing tables: {e}") from e

    if not tables:
        raise ValueError(f"No tables found in the HTML for {date_iso}.")

    return html, tables

# --- Funções de Processamento de Dados (Pandas) ---

def _norm(s: str) -> str:
    """Uppercase, strip, collapse spaces, remove accents/punctuation for robust matching."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s

def _clean_numeric(s: str | int | float) -> float | None:
    """Converte '99.889,83' para 99889.83, e '-' para NA."""
    if isinstance(s, (int, float)):
        return s
    if s is None:
        return pd.NA
    
    s = str(s).strip()
    if s == "-" or s == "":
        return pd.NA
    
    # Converte '99.889,83' para '99889.83'
    if '.' in s and ',' in s:
        s = s.replace(".", "") # Tira separador de milhar
    s = s.replace(",", ".") # Troca vírgula decimal por ponto
    
    try:
        return pd.to_numeric(s)
    except Exception:
        return pd.NA

def get_maturity_date(venc_code: str, calendar: bizdays.Calendar) -> date | None:
    """
    Converte um código de vencimento B3 (ex: 'F26') para a data
    de vencimento real (1º dia útil do mês).
    """
    # Mapeamento padrão de meses B3
    MONTH_CODES = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    
    try:
        letter = venc_code[0].upper()
        year_short = venc_code[1:]
        
        month = MONTH_CODES[letter]
        year = int(f"20{year_short}")
        
        # 1º dia do mês de vencimento
        first_day_of_month = pd.Timestamp(year, month, 1).date()
        
        # --- CORREÇÃO AQUI ---
        
        # O método correto é 'following(data)'.
        # Ele ajusta a data para o próximo dia útil se ela não for útil.
        
        # return calendar.adjust(first_day_of_month, 'following') # <--- LINHA ANTIGA E ERRADA
        return calendar.following(first_day_of_month) # <--- LINHA CORRIGIDA

        
    except Exception as e:
        print(f"Erro ao parsear vencimento '{venc_code}': {e}")
        return None

def calculate_di1_rates(combined_df: pd.DataFrame, trade_date_str: str) -> pd.DataFrame:
    """
    Recebe a tabela combinada (VENCTO + Ajustes) e a data do pregão,
    calcula os dias úteis (N) e a taxa anualizada.
    """
    
    # 1. Validar data do pregão (deve ser YYYY-MM-DD)
    try:
        # Tenta parsear a data do argumento. O script já valida
        # YYYY-MM-DD ou DD/MM/YYYY na função _parse_input_date.
        # Vamos usar a ISO-date (YYYY-MM-DD) retornada por ela.
        _, trade_date_iso = _parse_input_date(trade_date_str)
        # trade_date = datetime.strptime(trade_date_iso, '%Y-%m-%d').date() # <--- LINHA ANTIGA
        trade_date = pd.to_datetime(trade_date_iso).date() # <--- LINHA NOVA usando pd.to_datetime
    except Exception as e:
        raise ValueError(f"Data do pregão inválida '{trade_date_str}': {e}")

    # 2. Inicializar calendário ANBIMA
    cal = bizdays.Calendar.load('ANBIMA')
    
    df = combined_df.copy()

    # 3. Limpar colunas
    # A função combine_vencto_and_ajuste já promoveu a linha de header.
    # O cabeçalho 'AJUSTE' (PU) estava na posição 7 (índice 7) da linha de header.
    # Vamos renomear as colunas de forma robusta.
    
    col_map = {df.columns[0]: 'VENCTO'}
    
    # Encontra a coluna AJUSTE
    ajuste_col_name = None
    for col in df.columns:
        if str(col).strip().upper() == 'AJUSTE':
            ajuste_col_name = col
            break
            
    if not ajuste_col_name:
        raise ValueError(f"Não foi possível encontrar a coluna 'AJUSTE'. Colunas: {list(df.columns)}")

    col_map[ajuste_col_name] = 'AJUSTE'
    df = df.rename(columns=col_map)
    
    # 4. Limpar dados e calcular
    df_proc = df[['VENCTO', 'AJUSTE']].copy()
    df_proc['AJUSTE_NUM'] = df_proc['AJUSTE'].apply(_clean_numeric)
    
    # 5. Calcular Vencimento e Dias Úteis (N)
    df_proc['TRADE_DATE'] = trade_date
    df_proc['MATURITY_DATE'] = df_proc['VENCTO'].apply(lambda x: get_maturity_date(x, cal))
    
    # Remove vencimentos que não conseguiu parsear
    df_proc = df_proc.dropna(subset=['MATURITY_DATE'])
    
    # Calcula 'N'
    df_proc['DIAS_UTEIS_N'] = df_proc.apply(
        lambda row: cal.bizdays(row['TRADE_DATE'], row['MATURITY_DATE']),
        axis=1
    )
    
    # 6. Aplicar a fórmula
    # Taxa = ((100000 / PU) ** (252 / N)) - 1
    df_proc['TAXA_ANUAL'] = (
        (100000 / df_proc['AJUSTE_NUM']) ** (252 / df_proc['DIAS_UTEIS_N'])
    ) - 1

    # 7. Limpar e retornar
    df_final = df_proc[[
        'VENCTO', 
        'AJUSTE_NUM', 
        'TRADE_DATE', 
        'MATURITY_DATE', 
        'DIAS_UTEIS_N', 
        'TAXA_ANUAL'
    ]].copy()
    
    df_final = df_final.dropna(subset=['TAXA_ANUAL'])
    df_final = df_final[df_final['DIAS_UTEIS_N'] > 0] # Remove contratos vencidos
    
    return df_final

# --- Funções de Identificação de Tabela ---

def _looks_like_vencto(df: pd.DataFrame) -> bool:
    # Exatamente 1 coluna e pelo menos 1 linha (o "cabeçalho" falso)
    if df.shape[1] != 1 or len(df) == 0:
        return False
    
    # Verifica a CÉLULA [0, 0] (linha 0, coluna 0)
    first_cell = _norm(df.iloc[0, 0])
    return "VENCTO" in first_cell

def _looks_like_ajuste_block(df: pd.DataFrame) -> bool:
    # Pelo menos 11 colunas e pelo menos 1 linha
    if df.shape[1] < 11 or len(df) == 0:
        return False
        
    # Verifica a CÉLULA [0, 0] (linha 0, coluna 0)
    first_cell = _norm(df.iloc[0, 0])
    return first_cell.startswith("AJUSTE ANTER")

def combine_vencto_and_ajuste(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Find the VENCTO (1-col) table and the AJUSTE ANTER.* (~11-col) table,
    promote their first row to header, and return them horizontally stacked.
    """
    if not tables:
        raise ValueError("No tables provided.")

    candidates = []

    # 1) Try the common position: table 7 and 8 (1-based) -> indices 6 and 7
    try:
        t7_raw, t8_raw = tables[6], tables[7]
        if _looks_like_vencto(t7_raw) and _looks_like_ajuste_block(t8_raw) and len(t7_raw) == len(t8_raw):
            candidates.append(("positional", t7_raw.copy(), t8_raw.copy()))
    except Exception:
        pass  # indices may not exist

    # 2) If positional attempt not valid, search by shape/header
    if not candidates:
        vencto_idxs = [i for i, df in enumerate(tables) if _looks_like_vencto(df)]
        ajuste_idxs = [i for i, df in enumerate(tables) if _looks_like_ajuste_block(df)]

        for i in vencto_idxs:
            for j in ajuste_idxs:
                df_v = tables[i].copy()
                df_a = tables[j].copy()
                if len(df_v) == len(df_a) and len(df_v) > 0:
                    candidates.append((f"matched {i}+{j}", df_v, df_a))
                    break
            if candidates:
                break

    if not candidates:
        shapes = [df.shape for df in tables]
        raise ValueError(
            "Could not locate matching VENCTO and AJUSTE ANTER.* tables with equal rows.\n"
            f"Table shapes: {shapes}\n"
            "Heuristics expect: a 1-column table containing 'VENCTO' in cell [0,0], "
            "and a table with >=11 columns whose cell [0,0] starts with 'AJUSTE ANTER'."
        )

    _, vencto_df, ajuste_df = candidates[0]

    # --- INÍCIO DA NOVA LÓGICA DE PROMOÇÃO DE CABEÇALHO ---
    
    # 1. Promover cabeçalho da Tabela Vencto
    v_header = vencto_df.iloc[0]  # Pega a primeira linha (ex: "VENCTO")
    vencto_df = vencto_df[1:].copy()      # Pega os dados (da linha 1 em diante)
    vencto_df.columns = v_header         # Define a primeira linha como cabeçalho
    # Renomeia a coluna para 'VENCTO' para garantir
    vencto_df = vencto_df.rename(columns={vencto_df.columns[0]: "VENCTO"})

    # 2. Promover cabeçalho da Tabela Ajuste
    a_header = ajuste_df.iloc[0]  # Pega a primeira linha
    ajuste_df = ajuste_df[1:].copy()      # Pega os dados (da linha 1 em diante)
    ajuste_df.columns = a_header         # Define a primeira linha como cabeçalho

    # --- FIM DA NOVA LÓGICA ---

    # Reset indices to align rows 1:1
    vencto_df = vencto_df.reset_index(drop=True)
    ajuste_df = ajuste_df.reset_index(drop=True)

    # Final horizontal stack
    combined = pd.concat([vencto_df, ajuste_df], axis=1)

    return combined

# --- Bloco de Execução Principal (ETL) ---

def executar_atualizacao_principal():
    """Função mestre que roda o ETL de atualização do database."""
    
    print("Iniciando atualização do banco de dados DI1...")
    
    try:
        cal = bizdays.Calendar.load('ANBIMA')
    except Exception as e:
        print(f"ERRO: Não foi possível carregar o calendário ANBIMA. {e}")
        return
        
    # --- MODIFICAÇÃO 1: Carregar DB e 'data_historico' ---
    db = carregar_database()
    data_historico = db.get('data', {}) # Pega o sub-dicionário 'data'
    
    # 2. Definir range de datas e ATUALIZAR METADADOS
    try:
        hoje_ts = pd.Timestamp.now(tz='America/Sao_Paulo')
        hoje = hoje_ts.date()
    except Exception:
        hoje_ts = pd.Timestamp.now() # Fallback
        hoje = hoje_ts.date()

    # --- NOVA LINHA: Adiciona o timestamp da atualização ---
    db['metadata']['last_updated'] = hoje_ts.isoformat()
    # --- FIM DA NOVA LINHA ---

    data_inicio = hoje - pd.DateOffset(days=DIAS_HISTORICO - 1)
    datas_desejadas = pd.date_range(data_inicio, hoje, freq='D')
    
    alteracao_detectada = False
    
    # 3. Loop de atualização (operando em 'data_historico')
    for data_pd in datas_desejadas:
        data_iso = data_pd.strftime('%Y-%m-%d')
        data_date = data_pd.date()
        
        # Pula o que já está no DB
        if data_iso in data_historico: # <--- Opera em data_historico
            continue
        
        alteracao_detectada = True
        
        if not cal.isbizday(data_date):
            print(f"[{data_iso}] É feriado/fim de semana. Registrando.")
            data_historico[data_iso] = {"status": "feriado", "contratos": []} # <--- Opera em data_historico
            continue
        
        # ... (O resto do seu loop 'try/except' de coleta continua igual) ...
        # ... (Apenas garanta que ele está salvando em 'data_historico[data_iso]') ...
        print(f"[{data_iso}] É dia útil. Coletando dados da B3...")
        try:
            html, tables = get_b3_di1_tables(data_iso)
            combined_df = combine_vencto_and_ajuste(tables)
            curva_di_df = calculate_di1_rates(combined_df, data_iso)
            
            if curva_di_df.empty:
                print(f"[{data_iso}] Coleta OK, mas sem contratos (pregão vazio?).")
                data_historico[data_iso] = {"status": "dia_util", "contratos": []} # <--- Opera em data_historico
            else:
                print(f"[{data_iso}] Sucesso! {len(curva_di_df)} contratos encontrados.")
                json_data = formatar_dados_para_json(curva_di_df)
                data_historico[data_iso] = {"status": "dia_util", "contratos": json_data} # <--- Opera em data_historico
        
        except Exception as e:
            print(f"[{data_iso}] FALHA NA COLETA. Erro: {e}")
            data_historico[data_iso] = {"status": "erro_coleta", "contratos": [], "erro_msg": str(e)} # <--- Opera em data_historico
        

    # 4. Pruning (Apagar dados antigos)
    print("Limpando dados antigos...")
    datas_iso_desejadas = {d.strftime('%Y-%m-%d') for d in datas_desejadas}
    chaves_para_apagar = [chave for chave in data_historico if chave not in datas_iso_desejadas] # <--- Opera em data_historico
    
    if chaves_para_apagar:
        print(f"Apagando {len(chaves_para_apagar)} dias antigos: {chaves_para_apagar}")
        for chave in chaves_para_apagar:
            del data_historico[chave] # <--- Opera em data_historico
        alteracao_detectada = True 

    # 5. Salvar
    if alteracao_detectada or db['metadata']['last_updated'] == hoje_ts.isoformat():
        # (Sempre salva se o timestamp foi atualizado, mesmo sem novos dados)
        print("Salvando database atualizado...")
        
        # --- MODIFICAÇÃO 3: Reatribuir 'data_historico' ao DB principal ---
        db['data'] = data_historico
        salvar_database(db) # <--- Salva o 'db' completo
        
    else:
        print("Nenhuma alteração. Database já está atualizado.")
    
    print("Atualização concluída.")

if __name__ == "__main__":
    # Removemos o argparse. Agora o script sempre roda a atualização completa.
    executar_atualizacao_principal()