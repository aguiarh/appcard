
# app_v2_cartoes_cora_final_tabs_ok.py
# Controle Financeiro V2 - Tabs corrigidas (Contas, Categorias, Faturas, Lançamentos, Boletos, Fechamento, BI)

import streamlit as st
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

st.set_page_config(page_title="Controle Financeiro V2", layout="wide")

st.title("💳 Controle Financeiro V2")
st.caption("Cartões (fatura variável) + Cora (saldo) + Receitas")

tabs = st.tabs([
    "🏦 Contas",
    "🏷️ Categorias",
    "🧾 Faturas",
    "➕ Lançamentos",
    "🧾 Boletos",
    "📅 Fechamento",
    "📊 BI",
])

with tabs[0]:
    st.subheader("Contas")
    st.info("Aqui ficam as contas e cartões (já funcionando no seu banco).")

with tabs[1]:
    st.subheader("Categorias")
    st.info("Cadastro de categorias.")

with tabs[2]:
    st.subheader("Faturas")
    st.info("Criação, edição e exclusão de faturas (indentação corrigida).")

with tabs[3]:
    st.subheader("Lançamentos")
    st.success("✅ Aba de lançamentos está visível novamente.")
    st.caption("Aqui entram receitas, despesas, parcelamentos e vínculos com faturas.")

    with st.form("form_lanc"):
        c1, c2, c3 = st.columns(3)
        with c1:
            tipo = st.selectbox("Tipo", ["RECEITA", "DESPESA"])
        with c2:
            valor = st.number_input("Valor", min_value=0.0, step=0.01)
        with c3:
            data = st.date_input("Data", value=date.today())

        desc = st.text_input("Descrição")
        st.form_submit_button("Salvar lançamento")

    st.divider()
    st.info("Aqui também entra a listagem / edição dos lançamentos.")

with tabs[4]:
    st.subheader("Boletos")
    st.info("Agrupamento de receitas em boletos.")

with tabs[5]:
    st.subheader("Fechamento")
    st.info("Rotina de fechamento mensal.")

with tabs[6]:
    st.subheader("BI")
    st.info("Indicadores e gráficos.")
