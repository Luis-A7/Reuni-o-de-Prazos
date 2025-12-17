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

    # --- LÓGICA DE UNIFICAÇÃO ---
    df.loc[df['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df = df.groupby(['Obra', 'Semana'], as_index=False)[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']].sum()
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

    # --- LÓGICA DE UNIFICAÇÃO ---
    df_geral.loc[df_geral['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df_geral = df_geral.groupby('Obra', as_index=False).agg({
        'Projetado': 'sum', 'Fabricado': 'sum', 'Acabado': 'sum',
        'Expedido': 'sum', 'Montado': 'sum', 'Taxa de Aço': 'mean' 
    })
    return df_geral

# ========================================================
# FUNÇÃO PARA LER DADOS (POR FAMÍLIA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados_familias():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT nomeObra AS Obra, familia AS Familia, COUNT(nomePeca) AS unidade, SUM(volumeReal) AS Volume
        FROM `plannix-db`.`plannix`
        WHERE familia IS NOT NULL AND nomePeca IS NOT NULL AND volumeReal IS NOT NULL
        GROUP BY nomeObra, familia ORDER BY Obra, Familia;
    """
    df_familias = pd.read_sql(query, conn)
    conn.close()
    
    # --- LÓGICA DE UNIFICAÇÃO ---
    df_familias.loc[df_familias['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df_familias = df_familias.groupby(['Obra', 'Familia'], as_index=False).sum()
    return df_familias

# ========================================================
# FUNÇÃO PARA CARREGAR DADOS SALVOS DO USUÁRIO
# ========================================================
def carregar_dados_usuario():
    engine = create_engine(DB_URL)
    df_orcamentos_salvos = pd.DataFrame(columns=["Obra", "Orcamento", "Orcamento Lajes"])
    df_previsoes_salvas = pd.DataFrame(columns=["Obra", "Semana", "Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"])

    try:
        df_orcamentos_salvos = pd.read_sql("SELECT * FROM orcamentos_usuario", con=engine)
    except:
        st.info("Nota: Tabela 'orcamentos_usuario' será criada ao salvar.")
    
    try:
        df_previsoes_salvas = pd.read_sql("SELECT * FROM previsoes_usuario", con=engine)
        if not df_previsoes_salvas.empty:
            df_previsoes_salvas['Semana'] = pd.to_datetime(df_previsoes_salvas['Semana'])
    except:
        st.info("Nota: Tabela 'previsoes_usuario' será criada ao salvar.")

    engine.dispose()
    return df_orcamentos_salvos, df_previsoes_salvas

# ========================================================
# FUNÇÃO HELPER PARA FORMATAR A SEMANA
# ========================================================
def formatar_semana(date):
    if pd.isna(date): return None
    if isinstance(date, str):
        try: date = pd.to_datetime(date)
        except: return date
    start_str = date.strftime('%d/%m')
    end_str = (date + pd.Timedelta(days=6)).strftime('%d/%m')
    return f"{start_str} á {end_str} ({date.strftime('%Y')})"

# ========================================================
# FUNÇÃO PARA SALVAR DADOS NO MYSQL
# ========================================================
def salvar_dados_usuario(df_previsoes, df_orcamentos):
    engine = create_engine(DB_URL)
    try:
        df_previsoes_limpo = df_previsoes.dropna(subset=['Obra', 'Semana'])
        df_save_previsoes = df_previsoes_limpo[[
            "Obra", "Semana", "Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"
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

df_base['Semana'] = pd.to_datetime(df_base['Semana'])
todas_obras_lista = df_base["Obra"].unique().tolist()

# --- 2.5 INICIALIZAÇÃO DO SESSION STATE ---
if 'orcamentos' not in st.session_state:
    df_orcamentos_base = pd.DataFrame({"Obra": todas_obras_lista})
    df_orcamentos_para_editor = df_orcamentos_base.merge(df_orcamentos_salvos, on="Obra", how="left")
    st.session_state['orcamentos'] = df_orcamentos_para_editor

# Blindagem de Colunas (Cria se não existir)
cols_datas_necessarias = ["Ini Projeto", "Fim Projeto", "Ini Fabricacao", "Fim Fabricacao", "Ini Montagem", "Fim Montagem"]
for col in cols_datas_necessarias:
    if col not in st.session_state['orcamentos'].columns:
        st.session_state['orcamentos'][col] = None

defaults_num = {'Orcamento': 100.0, 'Orcamento Lajes': 0.0}
for col, val in defaults_num.items():
    if col not in st.session_state['orcamentos'].columns:
        st.session_state['orcamentos'][col] = val
    else:
        st.session_state['orcamentos'][col] = st.session_state['orcamentos'][col].fillna(val)

# --- 3. FILTRO GLOBAL ---
st.subheader("⚙️ Filtros Globais")
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    obras_selecionadas = st.multiselect("Selecione as Obras:", options=todas_obras_lista, default=todas_obras_lista)
with col2:
    data_inicio = st.date_input("Data de Início:", value=df_base['Semana'].min() - pd.Timedelta(weeks=10))
with col3:
    data_fim = st.date_input("Data Final:", value=df_base['Semana'].max())

data_inicio = pd.to_datetime(data_inicio)
data_fim = pd.to_datetime(data_fim)
    
# --- 4. PREPARAÇÃO DOS DADOS ---
df_para_cumsum = df_base[(df_base["Obra"].isin(obras_selecionadas))].copy()

# Preenchimento de Lacunas (Compactado)
zero_rows = []
weeks_to_add = 10 
for obra in obras_selecionadas:
    obra_df = df_para_cumsum[df_para_cumsum['Obra'] == obra]
    if not obra_df.empty:
        curr = obra_df['Semana'].min()
        for _ in range(weeks_to_add):
            curr -= pd.Timedelta(days=7)
            zero_rows.append({'Obra': obra, 'Semana': curr, 'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0})
        curr_fut = obra_df['Semana'].max()
        for _ in range(weeks_to_add):
            curr_fut += pd.Timedelta(days=7)
            zero_rows.append({'Obra': obra, 'Semana': curr_fut, 'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0})

if zero_rows:
    df_para_cumsum = pd.concat([pd.DataFrame(zero_rows), df_para_cumsum], ignore_index=True)

dfs_preenchidos = []
if not df_para_cumsum.empty:
    for obra, dados in df_para_cumsum.groupby("Obra"):
        dados = dados.groupby("Semana").sum(numeric_only=True).reset_index().set_index("Semana").sort_index()
        idx_completo = pd.date_range(start=dados.index.min(), end=dados.index.max(), freq='7D')
        dados = dados.reindex(idx_completo).fillna(0)
        dados['Obra'] = obra
        dfs_preenchidos.append(dados)
    df_para_cumsum = pd.concat(dfs_preenchidos).reset_index().rename(columns={'index': 'Semana'})

df_para_cumsum = df_para_cumsum.sort_values(["Obra", "Semana"]) 
for col in ["Volume_Projetado", "Volume_Fabricado", "Volume_Montado"]:
    df_para_cumsum[col] = df_para_cumsum.groupby("Obra")[col].cumsum()

df = df_para_cumsum[(df_para_cumsum["Semana"] >= data_inicio) & (df_para_cumsum["Semana"] <= data_fim)].copy()

if df.empty and not obras_selecionadas:
    st.warning("Nenhuma obra encontrada.")
    st.stop()

df['Semana_Display'] = df['Semana'].apply(formatar_semana)
    
# --- 5. ABAS ---
tab_cadastro, tab_tabelas, tab_graficos, tab_geral = st.tabs(["📁 Cadastro", "📊 Tabelas", "📈 Gráficos", "🌍 Tabela Geral"])

# --- ABA 1: CADASTRO (DATA INICIO REMOVIDA, APENAS ETAPAS) ---
with tab_cadastro:
    st.subheader("💰 1. Orçamento e Datas das Etapas")
    st.info("Cadastre o orçamento e as datas de **Início e Fim** de cada etapa.")
    
    orcamentos_filtrado = st.session_state['orcamentos'][st.session_state['orcamentos']['Obra'].isin(obras_selecionadas)].copy()
    
    # Conversão de tipos para evitar erro do editor
    cols_datas = ["Ini Projeto", "Fim Projeto", "Ini Fabricacao", "Fim Fabricacao", "Ini Montagem", "Fim Montagem"]
    for col in cols_datas:
        if col not in orcamentos_filtrado.columns: orcamentos_filtrado[col] = None
        orcamentos_filtrado[col] = pd.to_datetime(orcamentos_filtrado[col], errors='coerce')

    df_orcamentos_editado = st.data_editor(
        orcamentos_filtrado, 
        key="orcamento_editor", hide_index=True, width="stretch", disabled=["Obra"], 
        column_config={
            "Orcamento": st.column_config.NumberColumn("Orçamento (Vol)", min_value=0.01, format="%.2f"),
            "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", min_value=0.00, format="%.2f"),
            
            # DATAS POR ETAPA (INICIO E FIM)
            "Ini Projeto": st.column_config.DateColumn("Ini Proj.", format="DD/MM/YYYY"),
            "Fim Projeto": st.column_config.DateColumn("Fim Proj.", format="DD/MM/YYYY"),
            
            "Ini Fabricacao": st.column_config.DateColumn("Ini Fab.", format="DD/MM/YYYY"),
            "Fim Fabricacao": st.column_config.DateColumn("Fim Fab.", format="DD/MM/YYYY"),
            
            "Ini Montagem": st.column_config.DateColumn("Ini Mont.", format="DD/MM/YYYY"),
            "Fim Montagem": st.column_config.DateColumn("Fim Mont.", format="DD/MM/YYYY"),
            
            # REMOVE QUALQUER COLUNA 'DATA INICIO' GENÉRICA QUE EXISTA
            "Data Inicio": None
        }
    )
    st.session_state['orcamentos'].update(df_orcamentos_editado)

# --- 6. MERGE FINAL ---
df_orcamentos_atual = st.session_state['orcamentos']
df = df.merge(df_orcamentos_atual, on="Obra", how="left")
for col in ["Projetado", "Fabricado", "Montado"]:
    df[f"{col} %"] = (df[f"Volume_{col}"] / df["Orcamento"]) * 100

if not df_previsoes_salvas.empty:
    df = df.merge(df_previsoes_salvas, on=["Obra", "Semana"], how="left")
for col in ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]:
    df[col] = df[col].fillna(0.0)
df_para_edicao = df.copy() 

# --- ABA 2: TABELAS ---
with tab_tabelas:
    st.subheader("Controles de Visualização")
    c1, c2 = st.columns(2)
    with c1: show_editor = st.checkbox("Mostrar Edição de Previsões", value=True)
    with c2: show_result_table = st.checkbox("Mostrar Tabela Completa", value=True)
    
    df_editado = df_para_edicao 
    if show_editor:
        st.markdown("---")
        st.subheader("✏️ 2. Edite as Previsões")
        cols_ocultar = ["Obra", "Semana", "Semana_Display", "Volume_Projetado", "Projetado %", "Volume_Fabricado", "Fabricado %", "Volume_Montado", "Montado %", "Orcamento", "Orcamento Lajes"] + cols_datas
        df_editado = st.data_editor(
            df_para_edicao, key="dados_editor", width="stretch", hide_index=True, disabled=cols_ocultar,
            column_config={
                "Semana_Display": "Semana", 
                "Projeto Previsto %": st.column_config.NumberColumn(format="%.0f%%"),
                "Fabricação Prevista %": st.column_config.NumberColumn(format="%.0f%%"),
                "Montagem Prevista %": st.column_config.NumberColumn(format="%.0f%%"),
                "Ini Projeto": None, "Fim Projeto": None, 
                "Ini Fabricacao": None, "Fim Fabricacao": None, 
                "Ini Montagem": None, "Fim Montagem": None
            }
        )
        st.markdown("---")

    # Lógica de Corte e Salvar
    df_calculado = df_editado.copy().sort_values(['Obra', 'Semana'])
    for col in ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]:
        df_calculado[col] = df_calculado[col].replace(0.0, np.nan)
        df_calculado[col] = df_calculado.groupby('Obra')[col].ffill().fillna(0.0)
        mask_concluido = df_calculado.groupby('Obra')[col].shift(1) >= 100.0
        df_calculado.loc[mask_concluido, col] = np.nan

    if st.button("💾 Salvar Alterações no Banco de Dados", type="primary"):
        salvar_dados_usuario(df_editado, st.session_state['orcamentos'])

    if show_result_table:
        cols_res = ["Obra", "Semana_Display", "Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"]
        st.dataframe(df_calculado[[c for c in cols_res if c in df_calculado.columns]], width="stretch", hide_index=True)

# --- ABA 3: GRÁFICOS ---
with tab_graficos:
    st.subheader("📈 Tendências")
    if not df_calculado.empty:
        df_melt = df_calculado.melt(id_vars=["Obra", "Semana_Display", "Semana"], 
                                    value_vars=[c for c in ["Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"] if c in df_calculado.columns],
                                    var_name="Métrica", value_name="Porcentagem")
        chart = alt.Chart(df_melt).mark_line(point=True).encode(
            x=alt.X('Semana_Display:N', sort=alt.SortField(field="Semana", order='ascending'), title='Semana'),
            y='Porcentagem:Q', color='Métrica:N', strokeDash='Obra:N', tooltip=['Obra', 'Semana_Display', 'Métrica', alt.Tooltip('Porcentagem', format='.1f')]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

# --- ABA 4: TABELA GERAL (VISÃO DETALHADA + SALDO DIAS) ---
with tab_geral:
    st.subheader("🏗️ Resumo Geral Detalhado")
    try:
        df_geral = carregar_dados_gerais()
        df_orc_clean = st.session_state['orcamentos'].drop_duplicates(subset=['Obra'], keep='first')
        df_geral = df_geral.merge(df_orc_clean, on="Obra", how="left")

        # Cálculos Numéricos
        cols_num = ["Orcamento", "Orcamento Lajes", "Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]
        for col in cols_num: 
            if col in df_geral.columns: df_geral[col] = df_geral[col].fillna(0.0)

        for etapa in ["Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]:
            df_geral[f"{etapa} %"] = df_geral.apply(lambda r: (r[etapa]/r["Orcamento"]*100) if r["Orcamento"]>0 else 0, axis=1)

        # Cálculo Saldo de Dias
        hoje = pd.to_datetime(datetime.date.today())
        cols_fim = ["Fim Projeto", "Fim Fabricacao", "Fim Montagem"]
        
        for col in cols_fim:
            if col not in df_geral.columns: df_geral[col] = None
            df_geral[col] = pd.to_datetime(df_geral[col], errors='coerce')

        def calc_saldo(row, col_prazo):
            if pd.isna(row[col_prazo]): return None
            return (row[col_prazo] - hoje).days

        df_geral['Saldo Proj'] = df_geral.apply(lambda r: calc_saldo(r, 'Fim Projeto'), axis=1)
        df_geral['Saldo Fab'] = df_geral.apply(lambda r: calc_saldo(r, 'Fim Fabricacao'), axis=1)
        df_geral['Saldo Mont'] = df_geral.apply(lambda r: calc_saldo(r, 'Fim Montagem'), axis=1)

        # --- ORDEM FINAL ---
        colunas_ordenadas = [
            "Obra", "Orcamento", "Orcamento Lajes",
            
            "Projetado", "Projetado %", "Saldo Proj",
            "Taxa de Aço",
            
            "Fabricado", "Fabricado %", "Saldo Fab",
            
            "Acabado", "Acabado %",
            "Expedido", "Expedido %",
            
            "Montado", "Montado %", "Saldo Mont"
        ]
        
        cols_final = [c for c in colunas_ordenadas if c in df_geral.columns]

        st.dataframe(
            df_geral[cols_final], width="stretch", hide_index=True,
            column_config={
                "Orcamento": st.column_config.NumberColumn("Orçamento", format="%.2f"),
                "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", format="%.2f"),
                "Taxa de Aço": st.column_config.NumberColumn("Aço (kg/m³)", format="%.2f"),
                
                # Etapas com Saldo
                "Projetado": st.column_config.NumberColumn("Vol. Proj.", format="%.2f"),
                "Projetado %": st.column_config.NumberColumn("Proj. %", format="%.1f%%"), 
                "Saldo Proj": st.column_config.NumberColumn("⏳ Dias", format="%d d"),

                "Fabricado": st.column_config.NumberColumn("Vol. Fab.", format="%.2f"),
                "Fabricado %": st.column_config.NumberColumn("Fab. %", format="%.1f%%"),
                "Saldo Fab": st.column_config.NumberColumn("⏳ Dias", format="%d d"),

                "Montado": st.column_config.NumberColumn("Vol. Mont.", format="%.2f"),
                "Montado %": st.column_config.NumberColumn("Mont. %", format="%.1f%%"),
                "Saldo Mont": st.column_config.NumberColumn("⏳ Dias", format="%d d"),

                # Etapas sem saldo
                "Acabado": st.column_config.NumberColumn("Vol. Acab.", format="%.2f"),
                "Acabado %": st.column_config.NumberColumn("Acab. %", format="%.1f%%"),
                "Expedido": st.column_config.NumberColumn("Vol. Exp.", format="%.2f"),
                "Expedido %": st.column_config.NumberColumn("Exp. %", format="%.1f%%"),
            }
        )
    except Exception as e:
        st.error(f"Erro ao gerar tabela: {e}")
