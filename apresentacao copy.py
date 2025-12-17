import streamlit as st
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import altair as alt
import datetime
import numpy as np

# ========================================================
#          CONFIGURAÇÕES DO BANCO DE DADOS
# ========================================================
DB_CONFIG = {
    "host": st.secrets["db_host"],
    "user": st.secrets["db_user"],
    "password": st.secrets["db_password"],
    "database": st.secrets["db_name"],
    "port": 3306
}

DB_URL = f"mysql+mysqlconnector://{st.secrets['db_user']}:{st.secrets['db_password']}@{st.secrets['db_host']}:3306/{st.secrets['db_name']}"

# ========================================================
#     FUNÇÃO PARA LER DADOS (POR SEMANA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        WITH AllData AS (
            SELECT 
                nomeObra AS Obra, 
                CAST(DATE_SUB(data_Projeto, INTERVAL WEEKDAY(data_Projeto) DAY) AS DATE) AS Semana_Inicio, 
                volumeProjetado AS Volume_Projetado, 0 AS Volume_Fabricado, 0 AS Volume_Montado
            FROM `plannix-db`.`plannix` WHERE data_Projeto IS NOT NULL AND volumeProjetado > 0
            UNION ALL
            SELECT 
                nomeObra AS Obra, 
                CAST(DATE_SUB(data_Acabamento, INTERVAL WEEKDAY(data_Acabamento) DAY) AS DATE) AS Semana_Inicio, 
                0 AS Volume_Projetado, volumeFabricado AS Volume_Fabricado, 0 AS Volume_Montado
            FROM `plannix-db`.`plannix` WHERE data_Acabamento IS NOT NULL AND volumeFabricado > 0
            UNION ALL
            SELECT 
                nomeObra AS Obra, 
                CAST(DATE_SUB(dataMontada, INTERVAL WEEKDAY(dataMontada) DAY) AS DATE) AS Semana_Inicio, 
                0 AS Volume_Projetado, 0 AS Volume_Fabricado, volumeMontado AS Volume_Montado
            FROM `plannix-db`.`plannix` WHERE dataMontada IS NOT NULL AND volumeMontado > 0
        )
        SELECT
            Obra, Semana_Inicio AS Semana, 
            SUM(Volume_Projetado) AS Volume_Projetado,
            SUM(Volume_Fabricado) AS Volume_Fabricado,
            SUM(Volume_Montado) AS Volume_Montado
        FROM AllData
        GROUP BY Obra, Semana_Inicio ORDER BY Obra, Semana_Inicio;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # --- LÓGICA DE UNIFICAÇÃO (MALL SILVIO SILVEIRA) ---
    df.loc[df['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df = df.groupby(['Obra', 'Semana'], as_index=False)[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']].sum()
    # ---------------------------------------------------

    return df

# ========================================================
# FUNÇÃO PARA LER DADOS (TOTAIS POR OBRA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados_gerais():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            nomeObra AS Obra,
            SUM(volumeProjetado) AS Projetado,
            SUM(volumeFabricado) AS Fabricado,
            SUM(volumeAcabado) AS Acabado,
            SUM(volumeExpedido) AS Expedido,
            SUM(volumeMontado) AS Montado,
            AVG(peso_frouxo_por_volume) AS "Taxa de Aço" 
        FROM `plannix-db`.`plannix`
        GROUP BY nomeObra
        ORDER BY nomeObra;
    """
    df_geral = pd.read_sql(query, conn)
    conn.close()

    # --- LÓGICA DE UNIFICAÇÃO (MALL SILVIO SILVEIRA) ---
    df_geral.loc[df_geral['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    
    df_geral = df_geral.groupby('Obra', as_index=False).agg({
        'Projetado': 'sum',
        'Fabricado': 'sum',
        'Acabado': 'sum',
        'Expedido': 'sum',
        'Montado': 'sum',
        'Taxa de Aço': 'mean' 
    })
    # ---------------------------------------------------

    return df_geral

# ========================================================
# FUNÇÃO PARA LER DADOS (POR FAMÍLIA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados_familias():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            nomeObra AS Obra,
            familia AS Familia,
            COUNT(nomePeca) AS unidade,
            SUM(volumeReal) AS Volume
        FROM `plannix-db`.`plannix`
        WHERE 
            familia IS NOT NULL 
            AND nomePeca IS NOT NULL 
            AND volumeReal IS NOT NULL
        GROUP BY 
            nomeObra, 
            familia
        ORDER BY 
            Obra, 
            Familia;
    """
    df_familias = pd.read_sql(query, conn)
    conn.close()

    # --- LÓGICA DE UNIFICAÇÃO (MALL SILVIO SILVEIRA) ---
    df_familias.loc[df_familias['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df_familias = df_familias.groupby(['Obra', 'Familia'], as_index=False).sum()
    # ---------------------------------------------------

    return df_familias

# ========================================================
# FUNÇÃO PARA BUSCAR DATAS LIMITE DA OBRA (ESPECÍFICA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_datas_limite_etapas(obra_nome):
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT 
            MIN(data_Projeto) as ini_proj, MAX(data_Projeto) as fim_proj,
            MIN(data_Acabamento) as ini_fab, MAX(data_Acabamento) as fim_fab,
            MIN(dataMontada) as ini_mont, MAX(dataMontada) as fim_mont
        FROM `plannix-db`.`plannix`
        WHERE nomeObra = '{obra_nome}'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ========================================================
# FUNÇÃO PARA CALCULAR MÉDIAS GERAIS DE DURAÇÃO
# ========================================================
@st.cache_data(ttl=300)
def calcular_medias_cronograma():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            AVG(DATEDIFF(fim_p, ini_p)) as dias_duracao_proj,
            AVG(DATEDIFF(ini_f, ini_p)) as dias_lag_fab, 
            AVG(DATEDIFF(fim_f, ini_f)) as dias_duracao_fab,
            AVG(DATEDIFF(ini_m, ini_p)) as dias_lag_mont, 
            AVG(DATEDIFF(fim_m, ini_m)) as dias_duracao_mont
        FROM (
            SELECT
                nomeObra,
                MIN(data_Projeto) as ini_p, MAX(data_Projeto) as fim_p,
                MIN(data_Acabamento) as ini_f, MAX(data_Acabamento) as fim_f,
                MIN(dataMontada) as ini_m, MAX(dataMontada) as fim_m
            FROM `plannix-db`.`plannix`
            GROUP BY nomeObra
            HAVING ini_p IS NOT NULL AND ini_f IS NOT NULL AND ini_m IS NOT NULL
        ) as sub
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ========================================================
# FUNÇÃO PARA CARREGAR DADOS SALVOS DO USUÁRIO
# ========================================================
def carregar_dados_usuario():
    engine = create_engine(DB_URL)
    df_orcamentos_salvos = pd.DataFrame(columns=["Obra", "Orcamento", "Orcamento Lajes"])
    df_previsoes_salvas = pd.DataFrame(columns=["Obra", "Semana", "Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"])

    try:
        df_orcamentos_salvos = pd.read_sql("SELECT * FROM orcamentos_usuario", con=engine)
    except Exception as e:
        st.info("Nota: Tabela 'orcamentos_usuario' não encontrada (será criada ao salvar).")
    
    try:
        df_previsoes_salvas = pd.read_sql("SELECT * FROM previsoes_usuario", con=engine)
        if not df_previsoes_salvas.empty:
            df_previsoes_salvas['Semana'] = pd.to_datetime(df_previsoes_salvas['Semana'])
    except Exception as e:
        st.info("Nota: Tabela 'previsoes_usuario' não encontrada (será criada ao salvar).")

    engine.dispose()
    return df_orcamentos_salvos, df_previsoes_salvas

# ========================================================
# FUNÇÃO HELPER PARA FORMATAR A SEMANA (SEG-DOM)
# ========================================================
def formatar_semana(date):
    if pd.isna(date):
        return None
    if isinstance(date, str):
        try:
            date = pd.to_datetime(date)
        except:
            return date
    start_date = date 
    end_date = start_date + pd.Timedelta(days=6)
    start_str = start_date.strftime('%d/%m')
    end_str = end_date.strftime('%d/%m')
    year_str = start_date.strftime('%Y')
    return f"{start_str} á {end_str} ({year_str})"

# ========================================================
# FUNÇÃO PARA SALVAR DADOS NO MYSQL
# ========================================================
def salvar_dados_usuario(df_previsoes, df_orcamentos):
    engine = create_engine(DB_URL)
    try:
        df_previsoes_limpo = df_previsoes.dropna(subset=['Obra', 'Semana'])
        df_save_previsoes = df_previsoes_limpo[[
            "Obra", "Semana", "Projeto Previsto %", 
            "Fabricação Prevista %", "Montagem Prevista %"
        ]].copy()
        df_save_previsoes['Semana'] = pd.to_datetime(df_save_previsoes['Semana']).dt.strftime('%Y-%m-%d')
        
        df_save_previsoes.to_sql('previsoes_usuario', con=engine, if_exists='replace', index=False)
        df_orcamentos.to_sql('orcamentos_usuario', con=engine, if_exists='replace', index=False)
        st.success("✅ **Alterações salvas com sucesso no banco de dados!**")
    except Exception as e:
        st.error(f"❌ Erro ao salvar dados no banco de dados: {e}")
    finally:
        engine.dispose()
        
# ========================================================
#                INTERFACE STREAMLIT
# ========================================================
st.set_page_config(page_title="Reunião de Prazos", layout="wide")
st.title("📊 Reunião de Prazos")

# --- 1. CARREGAMENTO INICIAL ---
try:
    df_base = carregar_dados()
    df_orcamentos_salvos, df_previsoes_salvas = carregar_dados_usuario()
except Exception as e:
    st.error(f"Erro fatal ao carregar dados do MySQL: {e}")
    st.stop()

# --- 2. PROCESSAMENTO DE DATAS ---
df_base['Semana'] = pd.to_datetime(df_base['Semana'])

# --- 2.5 INICIALIZAÇÃO DO SESSION STATE (ATUALIZADO PARA DATAS INICIO/FIM) ---
todas_obras_lista = df_base["Obra"].unique().tolist()

if 'orcamentos' not in st.session_state:
    df_orcamentos_base = pd.DataFrame({"Obra": todas_obras_lista})
    
    # Faz o merge com o que veio do banco de dados
    df_orcamentos_para_editor = df_orcamentos_base.merge(
        df_orcamentos_salvos, on="Obra", how="left"
    )
    
    # Lista de colunas de Data que precisamos garantir
    cols_datas = [
        "Ini Projeto", "Fim Projeto", 
        "Ini Fabricacao", "Fim Fabricacao", 
        "Ini Montagem", "Fim Montagem"
    ]
    
    # Garante que as colunas existem
    for col in cols_datas:
        if col not in df_orcamentos_para_editor.columns:
            df_orcamentos_para_editor[col] = None
    
    # Valores padrão para números
    defaults_num = {'Orcamento': 100.0, 'Orcamento Lajes': 0.0}
    for col, val in defaults_num.items():
        if col not in df_orcamentos_para_editor.columns:
            df_orcamentos_para_editor[col] = val
        else:
            df_orcamentos_para_editor[col] = df_orcamentos_para_editor[col].fillna(val)

    st.session_state['orcamentos'] = df_orcamentos_para_editor

# --- 3. FILTRO GLOBAL ---
st.subheader("⚙️ Filtros Globais")

min_date = df_base['Semana'].min()
max_date = df_base['Semana'].max()
filtro_data_inicio_default = min_date - pd.Timedelta(weeks=10)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    obras_selecionadas = st.multiselect(
        "Selecione as Obras:",
        options=todas_obras_lista,
        default=todas_obras_lista
    )
with col2:
    data_inicio = st.date_input("Data de Início:", 
                                value=filtro_data_inicio_default,
                                min_value=None, max_value=None)
with col3:
    data_fim = st.date_input("Data Final:", 
                             value=max_date, 
                             min_value=None, max_value=None)

data_inicio = pd.to_datetime(data_inicio)
data_fim = pd.to_datetime(data_fim)
    
# --- 4. CÁLCULOS E PREPARAÇÃO DOS DADOS ---
df_para_cumsum = df_base[
    (df_base["Obra"].isin(obras_selecionadas))
].copy()

# ... (Lógica de preenchimento de semanas - mantida igual para economizar espaço, 
# se precisar alterar avise, mas o foco agora é o Cadastro e Tabela Geral) ...
zero_rows = []
weeks_to_add = 10 
for obra in obras_selecionadas:
    obra_df = df_para_cumsum[df_para_cumsum['Obra'] == obra]
    if not obra_df.empty:
        min_obra_date = obra_df['Semana'].min()
        current_date = min_obra_date
        for _ in range(weeks_to_add):
            current_date = current_date - pd.Timedelta(days=7)
            zero_rows.append({'Obra': obra, 'Semana': current_date, 'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0})
        max_obra_date = obra_df['Semana'].max()
        current_date_future = max_obra_date
        for _ in range(weeks_to_add):
            current_date_future = current_date_future + pd.Timedelta(days=7)
            zero_rows.append({'Obra': obra, 'Semana': current_date_future, 'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0})

if zero_rows:
    df_zero = pd.DataFrame(zero_rows)
    df_para_cumsum = pd.concat([df_zero, df_para_cumsum], ignore_index=True)

dfs_preenchidos = []
if not df_para_cumsum.empty:
    for obra, dados in df_para_cumsum.groupby("Obra"):
        dados = dados.groupby("Semana").sum(numeric_only=True).reset_index()
        dados = dados.set_index("Semana").sort_index()
        idx_completo = pd.date_range(start=dados.index.min(), end=dados.index.max(), freq='7D')
        dados_reindex = dados.reindex(idx_completo)
        dados_reindex['Obra'] = obra
        dados_reindex[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']] = dados_reindex[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']].fillna(0)
        dfs_preenchidos.append(dados_reindex)
    df_para_cumsum = pd.concat(dfs_preenchidos).reset_index().rename(columns={'index': 'Semana'})

df_para_cumsum = df_para_cumsum.sort_values(["Obra", "Semana"]) 
df_para_cumsum["Volume_Projetado"] = df_para_cumsum.groupby("Obra")["Volume_Projetado"].cumsum()
df_para_cumsum["Volume_Fabricado"] = df_para_cumsum.groupby("Obra")["Volume_Fabricado"].cumsum()
df_para_cumsum["Volume_Montado"] = df_para_cumsum.groupby("Obra")["Volume_Montado"].cumsum()

df = df_para_cumsum[
    (df_para_cumsum["Semana"] >= data_inicio) & 
    (df_para_cumsum["Semana"] <= data_fim)
].copy()

if df.empty and not obras_selecionadas:
    st.warning("Nenhuma obra encontrada.")
    st.stop()

df['Semana_Display'] = df['Semana'].apply(formatar_semana)
    
# --- 5. ABAS ---
tab_cadastro, tab_tabelas, tab_graficos, tab_geral, tab_planejador = st.tabs([
    "📁 Cadastro", 
    "📊 Tabelas", 
    "📈 Gráficos",
    "🌍 Tabela Geral",
    "📅 Planejador"
])

# --- ABA 1: CADASTRO (NOVA LÓGICA: DATA INICIO E FIM POR ETAPA) ---
with tab_cadastro:
    st.subheader("💰 1. Orçamento e Datas da Obra")
    st.info("Cadastre o orçamento e as datas de INÍCIO e FIM para cada etapa.")
    
    # Copia e filtra
    orcamentos_filtrado = st.session_state['orcamentos'][st.session_state['orcamentos']['Obra'].isin(obras_selecionadas)].copy()
    
    # --- CONVERSÃO SEGURA DE TIPOS (ESSENCIAL PARA EVITAR ERRO DE API) ---
    cols_datas = ["Ini Projeto", "Fim Projeto", "Ini Fabricacao", "Fim Fabricacao", "Ini Montagem", "Fim Montagem"]
    
    for col in cols_datas:
        # Se a coluna não existir, cria
        if col not in orcamentos_filtrado.columns:
            orcamentos_filtrado[col] = None
        # Força conversão para datetime
        orcamentos_filtrado[col] = pd.to_datetime(orcamentos_filtrado[col], errors='coerce')

    df_orcamentos_editado = st.data_editor(
        orcamentos_filtrado, 
        key="orcamento_editor", 
        hide_index=True, 
        width="stretch", 
        disabled=["Obra"], 
        column_config={
            "Orcamento": st.column_config.NumberColumn("Orçamento (Vol)", min_value=0.01, format="%.2f"),
            "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", min_value=0.00, format="%.2f"),
            
            # --- PROJETO ---
            "Ini Projeto": st.column_config.DateColumn("Início Proj.", format="DD/MM/YYYY"),
            "Fim Projeto": st.column_config.DateColumn("Fim Proj.", format="DD/MM/YYYY"),
            
            # --- FABRICAÇÃO ---
            "Ini Fabricacao": st.column_config.DateColumn("Início Fab.", format="DD/MM/YYYY"),
            "Fim Fabricacao": st.column_config.DateColumn("Fim Fab.", format="DD/MM/YYYY"),
            
            # --- MONTAGEM ---
            "Ini Montagem": st.column_config.DateColumn("Início Mont.", format="DD/MM/YYYY"),
            "Fim Montagem": st.column_config.DateColumn("Fim Mont.", format="DD/MM/YYYY"),
        }
    )
    # Atualiza session state
    st.session_state['orcamentos'].update(df_orcamentos_editado)


# --- 6. MERGE E PREPARAÇÃO DO DATAFRAME PRINCIPAL ---
df_orcamentos_atual = st.session_state['orcamentos']
df = df.merge(df_orcamentos_atual, on="Obra", how="left")
df["Projetado %"] = (df["Volume_Projetado"] / df["Orcamento"]) * 100
df["Fabricado %"] = (df["Volume_Fabricado"] / df["Orcamento"]) * 100
df["Montado %"] = (df["Volume_Montado"] / df["Orcamento"]) * 100

if not df_previsoes_salvas.empty:
    df = df.merge(df_previsoes_salvas, on=["Obra", "Semana"], how="left")

# Preenche vazios das previsões
for col in ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]:
    df[col] = df[col].fillna(0.0)
df_para_edicao = df.copy() 

# --- ABA 2: TABELAS ---
with tab_tabelas:
    st.subheader("Controles de Visualização")
    col_vis1, col_vis2 = st.columns(2)
    with col_vis1: show_editor = st.checkbox("Mostrar Tabela de Entrada de Previsões", value=True)
    with col_vis2: show_result_table = st.checkbox("Mostrar Tabela de Resultado Completa", value=True)
    
    df_editado = df_para_edicao 
    if show_editor:
        st.markdown("---")
        st.subheader("✏️ 2. Edite as Porcentagens de Previsão")
        st.info("Previsões de avanço físico semanal.")
        # Colunas que NÃO devem ser editadas aqui
        colunas_desabilitadas = ["Obra", "Semana", "Semana_Display", "Volume_Projetado", "Projetado %", "Volume_Fabricado", "Fabricado %", "Volume_Montado", "Montado %", "Orcamento", "Orcamento Lajes"] + cols_datas
        
        df_editado = st.data_editor(
            df_para_edicao, key="dados_editor", width="stretch", hide_index=True, disabled=colunas_desabilitadas,
            column_config={
                "Semana_Display": "Semana", 
                "Projeto Previsto %": st.column_config.NumberColumn(format="%.0f%%"),
                "Fabricação Prevista %": st.column_config.NumberColumn(format="%.0f%%"),
                "Montagem Prevista %": st.column_config.NumberColumn(format="%.0f%%"),
                "Volume_Projetado": None, "Volume_Fabricado": None, "Volume_Montado": None, 
                "Orcamento": None, "Orcamento Lajes": None, "Semana": None,
                # Oculta colunas de data aqui para limpar a visão
                "Ini Projeto": None, "Fim Projeto": None, 
                "Ini Fabricacao": None, "Fim Fabricacao": None, 
                "Ini Montagem": None, "Fim Montagem": None
            }
        )
        st.markdown("---")

    # Logica de corte após 100% (Mantida)
    df_calculado = df_editado.copy().sort_values(['Obra', 'Semana'])
    cols_previstas = ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]
    for col in cols_previstas:
        df_calculado[col] = df_calculado[col].replace(0.0, np.nan)
        df_calculado[col] = df_calculado.groupby('Obra')[col].ffill()
        df_calculado[col] = df_calculado[col].fillna(0.0)
        prev_vals = df_calculado.groupby('Obra')[col].shift(1)
        mask_concluido = prev_vals >= 100.0
        df_calculado.loc[mask_concluido, col] = np.nan

    df_calculado["Volume Projetado Previsto"] = (df_calculado["Orcamento"] * (df_calculado["Projeto Previsto %"] / 100))
    df_calculado["Volume Fabricado Previsto"] = (df_calculado["Orcamento"] * (df_calculado["Fabricação Prevista %"] / 100))
    df_calculado["Volume Montado Previsto"] = (df_calculado["Orcamento"] * (df_calculado["Montagem Prevista %"] / 100))

    if st.button("💾 Salvar Alterações no Banco de Dados", type="primary"):
        salvar_dados_usuario(df_editado, st.session_state['orcamentos'])

    if show_result_table:
        st.subheader("✅ 3. Tabela de Resultado Completa")
        # Mostra colunas relevantes
        cols_res = ["Obra", "Semana_Display", "Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"]
        st.dataframe(df_calculado[[c for c in cols_res if c in df_calculado.columns]], width="stretch", hide_index=True)

# --- ABA 3: GRÁFICOS ---
with tab_graficos:
    st.subheader("📈 4. Gráfico de Tendências")
    id_vars = ["Obra", "Semana", "Semana_Display"]
    value_vars = ["Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"]
    
    if not df_calculado.empty:
        df_grafico = df_calculado.copy() 
        for col in value_vars:
            if col in df_grafico.columns: df_grafico[col] = pd.to_numeric(df_grafico[col], errors='coerce')
        value_vars_existentes = [c for c in value_vars if c in df_grafico.columns]
        df_chart = df_grafico.melt(id_vars=id_vars, value_vars=value_vars_existentes, var_name="Tipo_Metrica", value_name="Porcentagem")
        chart = alt.Chart(df_chart).mark_line(point=True).encode(
            x=alt.X('Semana_Display:N', title='Semana', sort=alt.SortField(field="Semana", order='ascending')),
            y=alt.Y('Porcentagem:Q', title='Porcentagem (%)'), 
            color=alt.Color('Tipo_Metrica:N', title="Métrica"),
            strokeDash=alt.StrokeDash('Obra:N', title="Obra"),
            tooltip=['Obra', 'Semana_Display', 'Tipo_Metrica', alt.Tooltip('Porcentagem', format='.1f')]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Gráfico não disponível.")

# --- ABA 4: TABELA GERAL (COM NOVAS DATAS E CÁLCULOS) ---
with tab_geral:
    st.subheader("🏗️ Resumo Geral Detalhado")
    try:
        df_geral = carregar_dados_gerais()
        
        # 1. PEGAR ORÇAMENTOS (SEM DUPLICATAS)
        df_orcamentos_clean = st.session_state['orcamentos'].drop_duplicates(subset=['Obra'], keep='first')
        
        # Merge
        df_geral = df_geral.merge(df_orcamentos_clean, on="Obra", how="left")

        # 2. CÁLCULO DE PORCENTAGENS E VOLUMES
        cols_numericas = ["Orcamento", "Orcamento Lajes", "Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]
        for col in cols_numericas:
            if col in df_geral.columns:
                df_geral[col] = df_geral[col].fillna(0.0)

        etapas = ["Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]
        for etapa in etapas:
            df_geral[f"{etapa} %"] = df_geral.apply(
                lambda row: (row[etapa] / row["Orcamento"] * 100) if row["Orcamento"] > 0 else 0, axis=1
            )

        # 3. LIMPEZA DE DATAS
        cols_datas = ["Ini Projeto", "Fim Projeto", "Ini Fabricacao", "Fim Fabricacao", "Ini Montagem", "Fim Montagem"]
        for col in cols_datas:
            if col in df_geral.columns:
                df_geral[col] = df_geral[col].astype(str).str.replace(r'[\[\]\']', '', regex=True)
                df_geral[col] = df_geral[col].replace({'NaT': None, 'nan': None, 'None': None, '': None})
                df_geral[col] = pd.to_datetime(df_geral[col], errors='coerce')

        # 4. CÁLCULO DE DIAS RESTANTES (Baseado na Data FIM da etapa)
        hoje = pd.to_datetime(datetime.date.today())

        def calcular_restante_fim(row, coluna_fim):
            if pd.isna(row[coluna_fim]):
                return None
            try:
                # Diferença direta: Data Fim - Hoje
                return (row[coluna_fim] - hoje).days
            except:
                return None

        df_geral['Restante Proj'] = df_geral.apply(lambda row: calcular_restante_fim(row, 'Fim Projeto'), axis=1)
        df_geral['Restante Fab'] = df_geral.apply(lambda row: calcular_restante_fim(row, 'Fim Fabricacao'), axis=1)
        df_geral['Restante Mont'] = df_geral.apply(lambda row: calcular_restante_fim(row, 'Fim Montagem'), axis=1)

        # 5. ORGANIZAÇÃO VISUAL
        colunas_ordenadas = [
            "Obra", "Orcamento", "Orcamento Lajes",
            
            # PROJETO
            "Ini Projeto", "Fim Projeto", 
            "Projetado", "Projetado %", "Restante Proj",
            "Taxa de Aço",
            
            # FABRICAÇÃO
            "Ini Fabricacao", "Fim Fabricacao",
            "Fabricado", "Fabricado %", "Restante Fab",
            
            # OUTROS
            "Acabado", "Acabado %",
            "Expedido", "Expedido %",
            
            # MONTAGEM
            "Ini Montagem", "Fim Montagem",
            "Montado", "Montado %", "Restante Mont"
        ]
        
        cols_final = [c for c in colunas_ordenadas if c in df_geral.columns]

        st.dataframe(
            df_geral[cols_final], 
            width="stretch", 
            hide_index=True, 
            column_config={
                "Orcamento": st.column_config.NumberColumn("Orçamento", format="%.2f"),
                "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", format="%.2f"),
                "Taxa de Aço": st.column_config.NumberColumn("Taxa Aço", format="%.2f"),
                
                # DATAS
                "Ini Projeto": st.column_config.DateColumn("Ini Proj", format="DD/MM"),
                "Fim Projeto": st.column_config.DateColumn("Fim Proj", format="DD/MM"),
                "Ini Fabricacao": st.column_config.DateColumn("Ini Fab", format="DD/MM"),
                "Fim Fabricacao": st.column_config.DateColumn("Fim Fab", format="DD/MM"),
                "Ini Montagem": st.column_config.DateColumn("Ini Mont", format="DD/MM"),
                "Fim Montagem": st.column_config.DateColumn("Fim Mont", format="DD/MM"),

                # PROJETO
                "Projetado": st.column_config.NumberColumn("Vol. Proj.", format="%.2f"),
                "Projetado %": st.column_config.NumberColumn("Proj. %", format="%.1f%%"), 
                "Restante Proj": st.column_config.NumberColumn("⏳ Dias", format="%d"),

                # FABRICAÇÃO
                "Fabricado": st.column_config.NumberColumn("Vol. Fab.", format="%.2f"),
                "Fabricado %": st.column_config.NumberColumn("Fab. %", format="%.1f%%"),
                "Restante Fab": st.column_config.NumberColumn("⏳ Dias", format="%d"),

                # ACABAMENTO / EXP
                "Acabado": st.column_config.NumberColumn("Vol. Acab.", format="%.2f"),
                "Acabado %": st.column_config.NumberColumn("Acab. %", format="%.1f%%"),
                "Expedido": st.column_config.NumberColumn("Vol. Exp.", format="%.2f"),
                "Expedido %": st.column_config.NumberColumn("Exp. %", format="%.1f%%"),

                # MONTAGEM
                "Montado": st.column_config.NumberColumn("Vol. Mont.", format="%.2f"),
                "Montado %": st.column_config.NumberColumn("Mont. %", format="%.1f%%"),
                "Restante Mont": st.column_config.NumberColumn("⏳ Dias", format="%d"),
            }
        )
    except Exception as e:
        st.error(f"Erro ao gerar tabela geral: {e}")

# --- ABA 5: PLANEJADOR ---
with tab_planejador:
    st.info("Planejador em desenvolvimento...")
