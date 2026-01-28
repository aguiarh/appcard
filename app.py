# app_v2_cartoes_cora.py
# Controle Financeiro V2 (Streamlit + Postgres/Neon)
# - Contas: Cora (CONTA) + Cartões (CARTAO)
# - Faturas: por mês com datas reais (fecha varia)
# - Lançamentos: Receita/Despesa
# - Pagamento de fatura: cria saída no Cora e marca fatura como PAGA
#
# Requisitos:
#   pip install streamlit pandas python-dateutil psycopg2-binary
#
# ENV:
#   APP_USERS="hugo:Senha;admin:Senha"
#   DATABASE_URL="postgresql://user:pass@host/db?sslmode=require"

import os
import re
import time
import hmac
import hashlib
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

import psycopg2
from psycopg2.extras import RealDictCursor

# =========================
# Segurança / login simples
# =========================
def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _parse_users(raw: str) -> Dict[str, str]:
    users: Dict[str, str] = {}
    raw = (raw or "").strip()
    for part in raw.split(";"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        u, p = part.split(":", 1)
        u, p = u.strip(), p.strip()
        if u and p:
            users[u] = _sha256(p)
    return users

def require_login() -> None:
    raw = os.getenv("APP_USERS", "")
    users = _parse_users(raw)
    if not users:
        st.error("APP_USERS não configurado. Ex: hugo:Senha;admin:Senha")
        st.stop()

    if st.session_state.get("auth_ok"):
        return

    st.markdown("<h2 style='text-align:center;'>🔒 Acesso restrito</h2>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")

    if st.button("Entrar", type="primary", use_container_width=True):
        u = (u or "").strip()
        ok = u in users and hmac.compare_digest(users[u], _sha256(p or ""))
        if ok:
            st.session_state["auth_ok"] = True
            st.session_state["auth_user"] = u
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")
    st.stop()

def logout_button() -> None:
    c1, c2 = st.columns([0.75, 0.25])
    with c1:
        st.caption(f"👤 Logado como: **{st.session_state.get('auth_user','')}**")
    with c2:
        if st.button("Sair", use_container_width=True):
            st.session_state["auth_ok"] = False
            st.session_state["auth_user"] = ""
            st.rerun()

# =========================
# Helpers
# =========================
def toast_ok(msg: str, seconds: int = 3) -> None:
    # Não bloqueia a UI com sleep (Streamlit roda script inteiro a cada interação)
    try:
        st.toast(msg, icon="✅")
    except Exception:
        st.success(msg)

def br_money(v: float) -> str:
    return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def parse_brl(s: Any) -> float:
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).strip()
    if not t:
        return 0.0
    t = t.replace("R$", "").strip()
    t = re.sub(r"[^\d,.\-]", "", t)
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return 0.0

def month_start(d: date) -> date:
    return date(d.year, d.month, 1)

# =========================
# Banco
# =========================
def get_database_url() -> str:
    return (os.getenv("DATABASE_URL", "") or "").strip()

def get_conn():
    url = get_database_url()
    if not url:
        st.error("DATABASE_URL não configurada (Neon/Postgres).")
        st.stop()
    return psycopg2.connect(url)

def init_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS contas (
              id            BIGSERIAL PRIMARY KEY,
              nome          TEXT NOT NULL UNIQUE,
              tipo          TEXT NOT NULL CHECK (tipo IN ('CONTA','CARTAO')),
              ativo         BOOLEAN NOT NULL DEFAULT TRUE,
              saldo_inicial NUMERIC(14,2) NOT NULL DEFAULT 0
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS categorias (
              id BIGSERIAL PRIMARY KEY,
              nome TEXT NOT NULL UNIQUE,
              ativo BOOLEAN NOT NULL DEFAULT TRUE
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS faturas (
              id            BIGSERIAL PRIMARY KEY,
              conta_id      BIGINT NOT NULL REFERENCES contas(id),
              competencia   DATE NOT NULL,
              dt_inicio     DATE NOT NULL,
              dt_fim        DATE NOT NULL,
              dt_fechamento DATE NOT NULL,
              dt_vencimento DATE NOT NULL,
              status        TEXT NOT NULL DEFAULT 'ABERTA' CHECK (status IN ('ABERTA','FECHADA','PAGA')),
              created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE (conta_id, competencia)
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_faturas_periodo ON faturas(conta_id, dt_inicio, dt_fim);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_faturas_status ON faturas(conta_id, status);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS lancamentos (
              id BIGSERIAL PRIMARY KEY,
              tipo           TEXT NOT NULL CHECK (tipo IN ('RECEITA','DESPESA')),
              descricao      TEXT NOT NULL,
              valor          NUMERIC(14,2) NOT NULL CHECK (valor >= 0),
              dt_competencia DATE NOT NULL,
              dt_liquidacao  DATE,
              conta_id       BIGINT NOT NULL REFERENCES contas(id),
              fatura_id      BIGINT REFERENCES faturas(id),
              categoria_id   BIGINT REFERENCES categorias(id),
              forma_pagamento TEXT,
              status         TEXT,
              prestacao      TEXT,
              created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lanc_conta_dt ON lancamentos(conta_id, dt_competencia);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lanc_fatura ON lancamentos(fatura_id);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS pagamentos_fatura (
              id BIGSERIAL PRIMARY KEY,
              fatura_id BIGINT NOT NULL UNIQUE REFERENCES faturas(id),
              lancamento_saida_id BIGINT NOT NULL UNIQUE REFERENCES lancamentos(id),
              dt_pagamento DATE NOT NULL,
              valor NUMERIC(14,2) NOT NULL CHECK (valor >= 0),
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
        conn.commit()

def seed_basico() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO contas (nome, tipo, saldo_inicial)
            VALUES ('Cora','CONTA',0),
                   ('Cartão XP','CARTAO',0),
                   ('Cartão Itaú','CARTAO',0)
            ON CONFLICT (nome) DO NOTHING;
            """)
            cur.execute("""
            INSERT INTO categorias (nome) VALUES
              ('Saúde'), ('Alimentação'), ('Transporte'), ('Farmácia'), ('Educação'),
              ('Lazer'), ('Pessoal'), ('Investimentos'), ('Trabalho'), ('Outros'),
              ('Pagamento de Fatura')
            ON CONFLICT (nome) DO NOTHING;
            """)
        conn.commit()

init_db()
seed_basico()

# =========================
# Consultas utilitárias
# =========================
def fetch_df(sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
    params = params or []
    with get_conn() as conn:
        return pd.read_sql_query(sql, conn, params=params)

def fetch_one(sql: str, params: Optional[List[Any]] = None) -> Optional[Dict[str, Any]]:
    params = params or []
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

@st.cache_data(ttl=30, show_spinner=False)
def cached_df(query: str, params: tuple = ()) -> pd.DataFrame:
    """Cache simples para reduzir reruns lentos ao mexer em filtros/widgets."""
    return fetch_df(query, list(params) if params else None)

def cached_one(query: str, params: tuple = ()):
    rows = cached_df(query, params)
    if rows is None or len(rows) == 0:
        return None
    return rows.iloc[0].to_dict()

def clear_cache():
    try:
        cached_df.clear()
    except Exception:
        pass


def exec_sql(sql: str, params: Optional[List[Any]] = None) -> None:
    params = params or []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

def exec_many(sql: str, rows: List[Tuple[Any, ...]]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()

def list_contas(only_active: bool = True) -> pd.DataFrame:
    w = "WHERE ativo = TRUE" if only_active else ""
    return fetch_df(f"SELECT id, nome, tipo, saldo_inicial::float8 AS saldo_inicial, ativo FROM contas {w} ORDER BY tipo, nome")

def list_categorias() -> pd.DataFrame:
    return fetch_df("SELECT id, nome FROM categorias WHERE ativo = TRUE ORDER BY nome")

def list_faturas(conta_id: Optional[int] = None) -> pd.DataFrame:
    where = ""
    params: List[Any] = []
    if conta_id:
        where = "WHERE f.conta_id = %s"
        params.append(int(conta_id))
    return fetch_df(f"""
      SELECT f.id,
             c.nome AS cartao,
             f.competencia,
             f.dt_inicio, f.dt_fim, f.dt_fechamento, f.dt_vencimento,
             f.status
      FROM faturas f
      JOIN contas c ON c.id = f.conta_id
      {where}
      ORDER BY f.competencia DESC, c.nome ASC
    """, params)

def total_fatura(fatura_id: int) -> float:
    row = fetch_one("""
      SELECT COALESCE(SUM(valor),0)::float8 AS total
      FROM lancamentos
      WHERE fatura_id = %s AND tipo='DESPESA'
    """, [int(fatura_id)])
    return float(row["total"]) if row else 0.0

def saldo_conta_real(conta_nome: str) -> float:
    """Saldo REAL da conta (impacta caixa): só considera lançamentos liquidados.
    - RECEITA entra no saldo quando status='Recebido' OU dt_liquidacao preenchida
    - DESPESA sai do saldo quando status='Pago' OU dt_liquidacao preenchida
    """
    row = fetch_one(
        """
        SELECT
            c.saldo_inicial::float8
            + COALESCE(SUM(CASE
                WHEN l.tipo='RECEITA'
                 AND (COALESCE(l.status,'Pendente') ILIKE 'recebido' OR l.dt_liquidacao IS NOT NULL)
                THEN l.valor ELSE 0 END),0)::float8
            - COALESCE(SUM(CASE
                WHEN l.tipo='DESPESA'
                 AND (COALESCE(l.status,'Pendente') ILIKE 'pago' OR l.dt_liquidacao IS NOT NULL)
                THEN l.valor ELSE 0 END),0)::float8
          AS saldo
        FROM contas c
        LEFT JOIN lancamentos l ON l.conta_id = c.id
        WHERE c.nome = %s
        GROUP BY c.saldo_inicial
        """,
        [conta_nome],
    )
    return float(row["saldo"]) if row else 0.0

def previsao_receber_conta(conta_nome: str) -> float:
    """Previsão de RECEBIMENTO (receitas pendentes) - não entra no saldo real."""
    row = fetch_one(
        """
        SELECT COALESCE(SUM(l.valor),0)::float8 AS total
          FROM contas c
          JOIN lancamentos l ON l.conta_id = c.id
         WHERE c.nome=%s
           AND l.tipo='RECEITA'
           AND COALESCE(l.status,'Pendente') ILIKE 'pendente'
        """,
        [conta_nome],
    )
    return float(row["total"]) if row else 0.0

def previsao_pagar_conta(conta_nome: str) -> float:
    """Previsão de PAGAMENTO (despesas pendentes) - não sai do saldo real."""
    row = fetch_one(
        """
        SELECT COALESCE(SUM(l.valor),0)::float8 AS total
          FROM contas c
          JOIN lancamentos l ON l.conta_id = c.id
         WHERE c.nome=%s
           AND l.tipo='DESPESA'
           AND COALESCE(l.status,'Pendente') ILIKE 'pendente'
        """,
        [conta_nome],
    )
    return float(row["total"]) if row else 0.0

def saldo_cora() -> float:
    return saldo_conta_real("Cora")


def suggest_fatura_for_date(cartao_id: int, dt: date) -> Optional[int]:
    row = fetch_one("""
      SELECT id
      FROM faturas
      WHERE conta_id = %s
        AND %s BETWEEN dt_inicio AND dt_fim
      ORDER BY dt_fim DESC
      LIMIT 1
    """, [int(cartao_id), dt.isoformat()])
    return int(row["id"]) if row else None

# =========================
# App UI
# =========================
st.set_page_config(page_title="Controle Financeiro V2", page_icon="💳", layout="wide")
require_login()
logout_button()

st.markdown(
    """
<div style="text-align:center; margin-bottom: 1rem;">
  <h1 style="margin-bottom:0.25rem;">💳 Controle Financeiro V2</h1>
  <small style="color:#666;">Cartões (fatura variável) + Cora (saldo) + Receitas</small>
</div>
""",
    unsafe_allow_html=True,
)

# KPIs topo
colA, colB, colC = st.columns(3)
with colA:
    st.metric("Saldo Cora (REAL) (R$)", br_money(saldo_cora()))
    st.caption(f"Previsão a receber: {br_money(previsao_receber_conta('Cora'))} • a pagar: {br_money(previsao_pagar_conta('Cora'))}")
with colB:
    # Próximas faturas a pagar (total aberto/fechado não pago)
    df_next = fetch_df("""
      SELECT c.nome, f.dt_vencimento, f.id
      FROM faturas f
      JOIN contas c ON c.id=f.conta_id
      WHERE f.status IN ('ABERTA','FECHADA')
      ORDER BY f.dt_vencimento ASC
      LIMIT 2
    """)
    if df_next.empty:
        st.metric("Próxima fatura", "—")
    else:
        fid = int(df_next.iloc[0]["id"])
        st.metric("Próxima fatura", f"{df_next.iloc[0]['nome']} • {pd.to_datetime(df_next.iloc[0]['dt_vencimento']).strftime('%d/%m/%Y')} • R$ {br_money(total_fatura(fid))}")
with colC:
    st.metric("Hoje", date.today().strftime("%d/%m/%Y"))

tabs = st.tabs(["🏦 Contas", "🏷️ Categorias", "🧾 Faturas", "➕ Lançamentos", "🧾 Boletos", "💳 Fechamento", "📊 BI"])

# ---------------- Contas ----------------
with tabs[0]:
    st.subheader("Contas")
    st.caption("Dica: saldo inicial é usado só para CONTA (ex: Cora). Para cartões, deixe 0,00.")

    dfc = list_contas(only_active=False)
    if dfc.empty:
        st.info("Nenhuma conta cadastrada.")
    else:
        st.markdown("### Ajustar contas (saldo inicial / ativar-desativar)")
        df_edit = dfc.copy()
        df_edit = df_edit[["id", "nome", "tipo", "saldo_inicial", "ativo"]]
        df_edit = df_edit.set_index("id")
        df_edit["saldo_inicial"] = df_edit["saldo_inicial"].fillna(0.0).astype(float)

        edited = st.data_editor(
            df_edit,
            use_container_width=True,
            hide_index=True,
            disabled=["nome", "tipo"],
            column_config={
                "saldo_inicial": st.column_config.NumberColumn("Saldo inicial", help="Apenas para contas do tipo CONTA", format="%.2f"),
                "ativo": st.column_config.CheckboxColumn("Ativo"),
            },
            key="contas_editor",
        )

        c1, c2 = st.columns([0.35, 0.65])
        with c1:
            if st.button("Salvar alterações", type="primary", use_container_width=True, key="contas_save"):
                rows = []
                for _, r in edited.iterrows():
                    rows.append((float(r["saldo_inicial"]), bool(r["ativo"]), int(r.name)))
                exec_many("UPDATE contas SET saldo_inicial=%s, ativo=%s WHERE id=%s", rows)
                toast_ok("Contas atualizadas", 2)
                st.rerun()
        with c2:
            st.info("Se o saldo do Cora parecer errado, confirme: saldo inicial + receitas - despesas.")

    st.divider()
    st.markdown("### Nova conta/cartão")
    c1, c2, c3 = st.columns(3)
    with c1:
        nome = st.text_input("Nome (ex: Cora, Cartão XP)", key="c_nome")
    with c2:
        tipo = st.selectbox("Tipo", ["CONTA", "CARTAO"], key="c_tipo")
    with c3:
        saldo_ini = st.text_input("Saldo inicial (apenas CONTA)", value="0,00", key="c_saldo")

    if st.button("Adicionar", type="primary", use_container_width=True, key="c_add"):
        if not nome.strip():
            st.error("Informe o nome.")
        else:
            v = parse_brl(saldo_ini) if tipo == "CONTA" else 0.0
            exec_sql(
                "INSERT INTO contas (nome,tipo,saldo_inicial) VALUES (%s,%s,%s) ON CONFLICT (nome) DO NOTHING",
                [nome.strip(), tipo, float(v)],
            )
            toast_ok("Conta criada")
            st.rerun()

# ---------------- Categorias ----------------
with tabs[1]:
    st.subheader("Categorias")
    st.caption("Cadastre e organize suas categorias. Você pode desativar sem apagar histórico.")

    df_cat = fetch_df("SELECT id, nome, ativo FROM categorias ORDER BY nome")
    if df_cat.empty:
        st.info("Nenhuma categoria cadastrada.")
    else:
        st.markdown("### Editar categorias")
        edited = st.data_editor(
            df_cat.set_index("id"),
            use_container_width=True,
            hide_index=True,
            disabled=[],
            column_config={
                "nome": st.column_config.TextColumn("Nome"),
                "ativo": st.column_config.CheckboxColumn("Ativo"),
            },
            key="cat_editor",
        )
        c1, c2 = st.columns([0.35, 0.65])
        with c1:
            if st.button("Salvar categorias", type="primary", use_container_width=True, key="cat_save"):
                rows = []
                for _, r in edited.iterrows():
                    rows.append((str(r["nome"]).strip(), bool(r["ativo"]), int(r.name)))
                exec_many("UPDATE categorias SET nome=%s, ativo=%s WHERE id=%s", rows)
                toast_ok("Categorias atualizadas", 2)
                st.rerun()
        with c2:
            st.info("Dica: desativar mantém os lançamentos antigos intactos.")

    st.divider()
    st.markdown("### Nova categoria")
    nova = st.text_input("Nome da categoria", key="cat_new_name")
    if st.button("Adicionar categoria", type="primary", use_container_width=True, key="cat_add"):
        if not (nova or "").strip():
            st.error("Informe um nome.")
        else:
            exec_sql("INSERT INTO categorias (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING", [nova.strip()])
            toast_ok("Categoria criada", 2)
            st.rerun()

# ---------------- Faturas ----------------
with tabs[2]:
    st.subheader("Faturas (datas reais por mês)")
    st.caption("Edite o período (início/fim/fechamento/vencimento) das faturas existentes.")

    df_fat_edit = fetch_df(
        """
        SELECT f.id,
               c.nome AS cartao,
               f.competencia,
               f.dt_inicio,
               f.dt_fim,
               f.dt_fechamento,
               f.dt_vencimento,
               f.status
          FROM faturas f
          JOIN contas c ON c.id = f.conta_id
         ORDER BY c.nome, f.competencia DESC
        """
    )

    if not df_fat_edit.empty:
        # Visão rápida (datas em DD/MM/AAAA)
        df_view = df_fat_edit.copy()
        for col in ["competencia","dt_inicio","dt_fim","dt_fechamento","dt_vencimento"]:
            df_view[col] = pd.to_datetime(df_view[col]).dt.strftime("%d/%m/%Y")
        df_view = df_view.rename(columns={
            "cartao":"Cartão","competencia":"Competência","dt_inicio":"Início","dt_fim":"Fim","dt_fechamento":"Fechamento","dt_vencimento":"Vencimento","status":"Status"
        })
        cols = ["Cartão","Competência","Início","Fim","Fechamento","Vencimento","Status"]
        df_view = df_view[cols]
        st.dataframe(df_view, use_container_width=True, hide_index=True)

        df_show = df_fat_edit.copy()
        for col in ["competencia", "dt_inicio", "dt_fim", "dt_fechamento", "dt_vencimento"]:
            df_show[col] = pd.to_datetime(df_show[col]).dt.date

        df_show = df_show.set_index("id")

        edited_fat = st.data_editor(
            df_show,
            use_container_width=True,
            hide_index=True,
            disabled=["cartao", "competencia"],
            column_config={
                "dt_inicio": st.column_config.DateColumn("Início"),
                "dt_fim": st.column_config.DateColumn("Fim"),
                "dt_fechamento": st.column_config.DateColumn("Fechamento"),
                "dt_vencimento": st.column_config.DateColumn("Vencimento"),
                "status": st.column_config.SelectboxColumn("Status", options=["ABERTA", "FECHADA", "PAGA"]),
            },
            key="fat_editor",
        )

        if st.button("Salvar alterações das faturas", type="primary", use_container_width=True, key="fat_save"):
            rows = []
            for _, r in edited_fat.iterrows():
                rows.append(
                    (
                        pd.to_datetime(r["dt_inicio"]).date().isoformat(),
                        pd.to_datetime(r["dt_fim"]).date().isoformat(),
                        pd.to_datetime(r["dt_fechamento"]).date().isoformat(),
                        pd.to_datetime(r["dt_vencimento"]).date().isoformat(),
                        str(r["status"]),
                        int(r.name),
                    )
                )

            exec_many(
                """
                UPDATE faturas
                   SET dt_inicio=%s,
                       dt_fim=%s,
                       dt_fechamento=%s,
                       dt_vencimento=%s,
                       status=%s
                 WHERE id=%s
                """,
                rows,
            )
            toast_ok("Faturas atualizadas", 2)
            st.rerun()
    else:
        st.info("Nenhuma fatura cadastrada ainda.")

    st.divider()

st.markdown("### Excluir fatura")
st.caption("Regra: só permite excluir se não existir nenhum lançamento vinculado a ela.")

df_fat_del = fetch_df(
    """
    SELECT f.id,
           c.nome AS cartao,
           f.competencia,
           f.dt_inicio,
           f.dt_fim,
           f.dt_vencimento,
           f.status
      FROM faturas f
      JOIN contas c ON c.id = f.conta_id
     ORDER BY c.nome, f.competencia DESC
    """
)

if df_fat_del.empty:
    st.info("Nenhuma fatura para excluir.")
else:
    # Monta label amigável (sem expor ID)
    df_lbl = df_fat_del.copy()
    df_lbl["competencia"] = pd.to_datetime(df_lbl["competencia"]).dt.strftime("%m/%Y")
    df_lbl["dt_vencimento"] = pd.to_datetime(df_lbl["dt_vencimento"]).dt.strftime("%d/%m/%Y")
    df_lbl["label"] = df_lbl["cartao"].astype(str) + " • " + df_lbl["competencia"] + " • Venc: " + df_lbl["dt_vencimento"] + " • " + df_lbl["status"].astype(str)

    fatura_id = st.selectbox(
        "Selecione a fatura",
        options=df_lbl["id"].tolist(),
        format_func=lambda k: df_lbl.loc[df_lbl["id"] == k, "label"].iloc[0],
        key="fat_del_id",
    )

    row_cnt = fetch_one("SELECT COUNT(*)::int AS qtd FROM lancamentos WHERE fatura_id=%s", [int(fatura_id)])
    qtd = int(row_cnt["qtd"]) if row_cnt else 0

    if qtd > 0:
        st.warning(f"Esta fatura possui {qtd} lançamento(s) vinculado(s). Exclua/ajuste os lançamentos primeiro.")
    else:
        confirm = st.checkbox("Confirmo que quero excluir esta fatura", key="fat_del_confirm")
        if st.button("Excluir fatura", type="primary", use_container_width=True, key="fat_del_btn"):
            if not confirm:
                st.error("Marque a confirmação.")
            else:
                exec_sql("DELETE FROM faturas WHERE id=%s", [int(fatura_id)])
                clear_cache()
                st.toast("Fatura excluída", icon="✅")
                st.session_state.pop("fat_del_id", None)
                st.session_state.pop("fat_del_confirm", None)
                st.rerun()

    st.divider()

    contas_cartao = fetch_df("SELECT id, nome FROM contas WHERE tipo='CARTAO' AND ativo=TRUE ORDER BY nome")
    if contas_cartao.empty:
        st.info("Cadastre pelo menos 1 cartão em Contas.")
    else:
        cartao_nome = st.selectbox("Cartão", contas_cartao["nome"].tolist(), key="f_cartao")
        cartao_id = int(contas_cartao.loc[contas_cartao["nome"] == cartao_nome, "id"].iloc[0])

        st.markdown("#### Criar/Atualizar fatura do mês")
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            competencia = st.date_input("Competência (dia 01)", value=month_start(date.today()), key="f_comp")
            competencia = month_start(competencia)
        with c2:
            dt_inicio = st.date_input("Início", value=(competencia - relativedelta(months=1) + relativedelta(days=2)), key="f_ini")
        with c3:
            dt_fim = st.date_input("Fim", value=(competencia + relativedelta(days=1)), key="f_fim")
        with c4:
            dt_fech = st.date_input("Fechamento", value=dt_fim, key="f_fech")
        with c5:
            dt_venc = st.date_input("Vencimento", value=(competencia + relativedelta(days=24)), key="f_venc")

        if st.button("Salvar fatura", type="primary", use_container_width=True, key="f_save"):
            if dt_inicio > dt_fim:
                st.error("Início não pode ser maior que Fim.")
            else:
                exec_sql(
                    """
                    INSERT INTO faturas (conta_id,competencia,dt_inicio,dt_fim,dt_fechamento,dt_vencimento,status)
                    VALUES (%s,%s,%s,%s,%s,%s,'ABERTA')
                    ON CONFLICT (conta_id, competencia) DO UPDATE
                    SET dt_inicio=EXCLUDED.dt_inicio,
                        dt_fim=EXCLUDED.dt_fim,
                        dt_fechamento=EXCLUDED.dt_fechamento,
                        dt_vencimento=EXCLUDED.dt_vencimento
                    """,
                    [cartao_id, competencia.isoformat(), dt_inicio.isoformat(), dt_fim.isoformat(), dt_fech.isoformat(), dt_venc.isoformat()],
                )
                toast_ok("Fatura salva")
                st.rerun()

        st.markdown("#### Lista de faturas")
        dff = list_faturas(cartao_id)
        if dff.empty:
            st.info("Nenhuma fatura cadastrada para esse cartão.")
        else:
            dff_show = dff.copy()
            for col in ["competencia","dt_inicio","dt_fim","dt_fechamento","dt_vencimento"]:
                dff_show[col] = pd.to_datetime(dff_show[col]).dt.strftime("%d/%m/%Y")
            st.dataframe(dff_show, use_container_width=True, hide_index=True)

# ---------------- Lançamentos ----------------
with tabs[3]:
    st.subheader("Lançamentos (Receitas e Despesas)")
    contas = list_contas(only_active=True)
    cats = list_categorias()

    if contas.empty:
        st.info("Cadastre contas primeiro.")
    else:
        st.markdown("### Novo lançamento")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            tipo_l = st.selectbox("Tipo", ["DESPESA", "RECEITA"], key="l_tipo")
        with c2:
            conta_nome = st.selectbox("Conta", contas["nome"].tolist(), key="l_conta")
            conta_row = contas.loc[contas["nome"] == conta_nome].iloc[0]
            conta_id = int(conta_row["id"])
            conta_tipo = str(conta_row["tipo"])
        with c3:
            dt_comp = st.date_input("Data (competência)", value=date.today(), key="l_dt")
        with c4:
            parcelas = st.number_input("Parcelas", min_value=1, max_value=60, value=1, step=1, key="l_parc")

        desc = st.text_input("Descrição", key="l_desc")

        c5, c6, c7 = st.columns(3)
        with c5:
            cat_nome = st.selectbox("Categoria", cats["nome"].tolist(), key="l_cat")
            cat_id = int(cats.loc[cats["nome"] == cat_nome, "id"].iloc[0])
        with c6:
            forma = st.text_input("Forma (opcional)", value="", key="l_forma")
        with c7:
            status = st.text_input("Status", value="Pendente", key="l_status")

        modo_valor = st.radio("Valor informado é", ["Total", "Parcela"], horizontal=True, key="l_modo_valor")
        if modo_valor == "Total":
            valor_txt = st.text_input("Valor total (R$)", value="0,00", key="l_valor_total")
        else:
            valor_txt = st.text_input("Valor da parcela (R$)", value="0,00", key="l_valor_parcela")

        fatura_id: Optional[int] = None
        if conta_tipo == "CARTAO" and tipo_l == "DESPESA":
            st.markdown("##### Fatura (para compras no cartão)")
            suggested = suggest_fatura_for_date(conta_id, dt_comp)
            dff = list_faturas(conta_id)
            if dff.empty:
                st.warning("Cadastre a fatura desse cartão na aba Faturas para vincular as compras.")
            else:
                opts = []
                for _, r in dff.iterrows():
                    label = f"{r['cartao']} • {pd.to_datetime(r['competencia']).strftime('%m/%Y')} • vence {pd.to_datetime(r['dt_vencimento']).strftime('%d/%m/%Y')} • {r['status']}"
                    opts.append((int(r.name), label))
                default_idx = 0
                if suggested:
                    for i, (fid, _) in enumerate(opts):
                        if fid == suggested:
                            default_idx = i
                            break
                choice = st.selectbox(
                    "Vincular à fatura (1ª parcela)",
                    options=list(range(len(opts))),
                    format_func=lambda i: opts[i][1],
                    index=default_idx,
                    key="l_fatura_sel",
                )
                fatura_id = opts[choice][0]

        dt_liq = st.date_input("Data liquidação (opcional)", value=None, key="l_dtliq")

        def _calc_valores_parcelas(v_in: float, n: int, modo: str):
            if n <= 1:
                return [round(v_in, 2)]
            if modo == "Total":
                base = round(v_in / n, 2)
                vals = [base] * n
                vals[-1] = round(vals[-1] + (v_in - sum(vals)), 2)
                return vals
            return [round(v_in, 2)] * n

        if st.button("Gerar prévia", use_container_width=True, key="l_previa"):
            erros = []
            if not desc.strip():
                erros.append("Descrição obrigatória.")
            v = parse_brl(valor_txt)
            if v <= 0:
                erros.append("Valor deve ser maior que 0.")
            if tipo_l == "RECEITA" and int(parcelas) != 1:
                erros.append("Receita parcelada: por enquanto use parcelas = 1 (podemos evoluir depois).")
            if conta_tipo == "CARTAO" and tipo_l == "DESPESA" and not fatura_id:
                erros.append("Selecione uma fatura para compras no cartão.")
            if erros:
                st.error("Ajuste:\n\n- " + "\n- ".join(erros))
            else:
                vals = _calc_valores_parcelas(v, int(parcelas), modo_valor)
                linhas = []
                for i in range(int(parcelas)):
                    dt_i = dt_comp + relativedelta(months=i)
                    fat_i = None
                    if conta_tipo == "CARTAO" and tipo_l == "DESPESA":
                        fat_i = suggest_fatura_for_date(conta_id, dt_i)
                    linhas.append({
                        "tipo": tipo_l,
                        "descricao": desc.strip(),
                        "valor": float(vals[i]),
                        "dt_competencia": dt_i,
                        "dt_liquidacao": dt_liq,
                        "conta_id": conta_id,
                        "fatura_id": fat_i,
                        "categoria_id": cat_id,
                        "forma_pagamento": (forma or None),
                        "status": (status or None),
                        "prestacao": (f"{i+1}/{int(parcelas)}" if int(parcelas) > 1 else None),
                    })
                st.session_state["l_prev_df"] = pd.DataFrame(linhas)

        prev = st.session_state.get("l_prev_df")
        if isinstance(prev, pd.DataFrame) and not prev.empty:
            st.markdown("### Prévia (edite se quiser antes de salvar)")
            view = prev.copy()
            view["valor"] = view["valor"].apply(br_money)
            view["dt_competencia"] = pd.to_datetime(view["dt_competencia"]).dt.date

            edited = st.data_editor(
                view,
                use_container_width=True,
                hide_index=True,
                disabled=["tipo", "conta_id", "categoria_id"],
                column_config={
                    "valor": st.column_config.TextColumn("Valor (R$)"),
                    "dt_competencia": st.column_config.DateColumn("Data competência"),
                    "dt_liquidacao": st.column_config.DateColumn("Data liquidação"),
                    "fatura_id": st.column_config.NumberColumn("Fatura ID (auto)"),
                },
                key="l_prev_editor",
            )

            csa, csb = st.columns(2)
            with csa:
                if st.button("Salvar lançamento(s)", type="primary", use_container_width=True, key="l_save_multi"):
                    rows = []
                    erros = []
                    for _, r in edited.iterrows():
                        try:
                            rows.append((
                                r["tipo"],
                                r["descricao"],
                                float(parse_brl(r["valor"])),
                                pd.to_datetime(r["dt_competencia"]).date().isoformat(),
                                (pd.to_datetime(r["dt_liquidacao"]).date().isoformat() if r.get("dt_liquidacao") else None),
                                int(r["conta_id"]),
                                (int(r["fatura_id"]) if r.get("fatura_id") not in (None, "", 0) else None),
                                int(r["categoria_id"]),
                                r.get("forma_pagamento", None),
                                r.get("status", None),
                                r.get("prestacao", None),
                            ))
                        except Exception as e:
                            erros.append(str(e))

                    if erros:
                        st.error("Falha ao preparar dados:\n- " + "\n- ".join(erros))
                    else:
                        exec_many(
                            """
                            INSERT INTO lancamentos
                              (tipo,descricao,valor,dt_competencia,dt_liquidacao,conta_id,fatura_id,categoria_id,forma_pagamento,status,prestacao)
                            VALUES
                              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            rows,
                        )
                        st.session_state.pop("l_prev_df", None)
                        toast_ok("Lançamento(s) salvo(s)", 2)
                        st.rerun()
            with csb:
                if st.button("Limpar prévia", use_container_width=True, key="l_clear_prev"):
                    st.session_state.pop("l_prev_df", None)
                    st.rerun()

        st.divider()
        st.markdown("### Listagem")
        filtro = st.text_input("Buscar (descrição)", value="", key="l_busca")
        conta_f = st.selectbox("Filtrar por conta", ["Todas"] + contas["nome"].tolist(), key="l_fconta")
        where = "WHERE 1=1"
        params = []
        if filtro.strip():
            where += " AND l.descricao ILIKE %s"
            params.append(f"%{filtro.strip()}%")
        if conta_f != "Todas":
            where += " AND l.conta_id = (SELECT id FROM contas WHERE nome=%s)"
            params.append(conta_f)

        df = fetch_df(f"""
          SELECT l.id,
                 l.tipo,
                 l.descricao,
                 l.valor::float8 AS valor,
                 l.dt_competencia,
                 c.nome AS conta,
                 COALESCE(cat.nome,'') AS categoria,
                 COALESCE(l.prestacao,'') AS prestacao
          FROM lancamentos l
          JOIN contas c ON c.id=l.conta_id
          LEFT JOIN categorias cat ON cat.id=l.categoria_id
          {where}
          ORDER BY l.dt_competencia DESC, l.id DESC
          LIMIT 600
        """, params)

        if df.empty:
            st.info("Nada para mostrar.")
        else:
            df_show = df.copy()
            # Não exibir ID na tabela
            if "id" in df_show.columns:
                df_show = df_show.drop(columns=["id"])
            df_show = df_show.rename(columns={"dt_competencia":"Data", "tipo":"Tipo", "descricao":"Descrição", "valor":"Valor", "conta":"Conta", "categoria":"Categoria", "prestacao":"Parcela"})
            df_show["Data"] = pd.to_datetime(df_show["Data"]).dt.strftime("%d/%m/%Y")
            df_show["Valor"] = df_show["Valor"].apply(br_money)
            cols = ["Data","Tipo","Descrição","Valor","Conta","Categoria","Parcela"]
            cols = [c for c in cols if c in df_show.columns] + [c for c in df_show.columns if c not in cols]
            df_show = df_show[cols]
            st.dataframe(df_show, use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("## ✏️ Editar / Excluir lançamentos")
            st.caption("Use o ID da linha. Dá pra corrigir fatura, datas, valores e status. Excluir apaga a linha (cuidado).")

            with st.expander("Editar um lançamento por ID", expanded=False):
                edit_id = st.number_input("ID do lançamento", min_value=1, step=1, value=1, key="edit_lanc_id")
                if st.button("Carregar", use_container_width=True, key="edit_lanc_load"):
                    row = fetch_one(
                        """
                        SELECT id, tipo, descricao, valor::float8 AS valor, dt_competencia, dt_liquidacao,
                               conta_id, fatura_id, categoria_id, forma_pagamento, status, prestacao
                        FROM lancamentos WHERE id=%s
                        """,
                        [int(edit_id)],
                    )
                    st.session_state["edit_row"] = row

                row = st.session_state.get("edit_row")
                if row and int(row.get("id", 0)) == int(edit_id):
                    contas_all = list_contas(only_active=False)
                    cats_all = fetch_df("SELECT id, nome FROM categorias ORDER BY nome")

                    conta_map = {int(r.name): f'{r["nome"]} ({r["tipo"]})' for _, r in contas_all.iterrows()}
                    cat_map = {int(r.name): r["nome"] for _, r in cats_all.iterrows()}

                    c1, c2, c3 = st.columns(3)
                    with c1:
                        e_tipo = st.selectbox("Tipo", ["RECEITA", "DESPESA"], index=0 if row["tipo"]=="RECEITA" else 1, key="e_tipo")
                    with c2:
                        keys = list(conta_map.keys())
                        e_conta = st.selectbox("Conta", options=keys, format_func=lambda k: conta_map[k],
                                               index=(keys.index(int(row["conta_id"])) if int(row["conta_id"]) in keys else 0),
                                               key="e_conta")
                    with c3:
                        cat_keys = list(cat_map.keys()) if cat_map else []
                        e_cat = st.selectbox("Categoria", options=cat_keys, format_func=lambda k: cat_map[k],
                                             index=(cat_keys.index(int(row["categoria_id"])) if row["categoria_id"] and int(row["categoria_id"]) in cat_keys else 0),
                                             key="e_cat")

                    e_desc = st.text_input("Descrição", value=row["descricao"] or "", key="e_desc")
                    e_val = st.text_input("Valor (R$)", value=br_money(row["valor"]), key="e_val")
                    c4, c5, c6 = st.columns(3)
                    with c4:
                        e_dt = st.date_input("Data competência", value=pd.to_datetime(row["dt_competencia"]).date(), key="e_dt")
                    with c5:
                        e_dtliq = st.date_input("Data liquidação (receb/pag)", value=(pd.to_datetime(row["dt_liquidacao"]).date() if row["dt_liquidacao"] else None), key="e_dtliq")
                    with c6:
                        e_status = st.text_input("Status", value=(row["status"] or "Pendente"), key="e_status")

                    st.markdown("#### Fatura (opcional)")
                    e_fatura_in = st.text_input("Fatura ID (vazio remove)", value=("" if not row.get("fatura_id") else str(row.get("fatura_id"))), key="e_fatura")

                    e_forma = st.text_input("Forma (opcional)", value=row.get("forma_pagamento") or "", key="e_forma")
                    e_prest = st.text_input("Prestação (opcional)", value=row.get("prestacao") or "", key="e_prest")

                    if st.button("Salvar edição", type="primary", use_container_width=True, key="edit_lanc_save"):
                        v = parse_brl(e_val)
                        if v <= 0:
                            st.error("Valor inválido.")
                        else:
                            fat = (e_fatura_in or "").strip()
                            fat_id = int(fat) if fat.isdigit() else None
                            exec_sql(
                                """
                                UPDATE lancamentos
                                   SET tipo=%s,
                                       descricao=%s,
                                       valor=%s,
                                       dt_competencia=%s,
                                       dt_liquidacao=%s,
                                       conta_id=%s,
                                       fatura_id=%s,
                                       categoria_id=%s,
                                       forma_pagamento=%s,
                                       status=%s,
                                       prestacao=%s
                                 WHERE id=%s
                                """,
                                [
                                    e_tipo,
                                    e_desc.strip(),
                                    float(v),
                                    e_dt.isoformat(),
                                    (e_dtliq.isoformat() if e_dtliq else None),
                                    int(e_conta),
                                    fat_id,
                                    (int(e_cat) if e_cat else None),
                                    (e_forma.strip() or None),
                                    (e_status.strip() or None),
                                    (e_prest.strip() or None),
                                    int(edit_id),
                                ],
                            )
                            toast_ok("Lançamento atualizado", 2)
                            st.session_state.pop("edit_row", None)
                            st.rerun()

            with st.expander("Excluir lançamento por ID", expanded=False):
                del_id = st.number_input("ID para excluir", min_value=1, step=1, value=1, key="del_lanc_id")
                confirm = st.checkbox("Confirmo exclusão definitiva", key="del_confirm")
                if st.button("Excluir", type="primary", use_container_width=True, key="del_lanc_btn"):
                    if not confirm:
                        st.error("Marque a confirmação para excluir.")
                    else:
                        exec_sql("DELETE FROM lancamentos WHERE id=%s", [int(del_id)])
                        toast_ok("Lançamento excluído", 2)
                        st.rerun()

            st.divider()
            st.markdown("## 📦 Baixa em lote (ótimo para boletos)")
            st.caption("Crie várias RECEITAS com status Pendente e depois dê baixa em uma única data.")

            with st.expander("Dar baixa em lote (por IDs)", expanded=False):
                ids_txt = st.text_input("IDs (separados por vírgula)", value="", key="batch_ids")
                dt_baixa = st.date_input("Data de baixa (liquidação)", value=date.today(), key="batch_dt")
                novo_status = st.selectbox("Novo status", ["Recebido", "Pago", "Cancelado", "Pendente"], index=0, key="batch_status")

                if st.button("Aplicar baixa", type="primary", use_container_width=True, key="batch_apply"):
                    ids = []
                    for p in (ids_txt or "").split(","):
                        p = p.strip()
                        if p.isdigit():
                            ids.append(int(p))
                    if not ids:
                        st.error("Informe pelo menos um ID válido.")
                    else:
                        exec_sql(
                            "UPDATE lancamentos SET dt_liquidacao=%s, status=%s WHERE id = ANY(%s)",
                            [dt_baixa.isoformat(), novo_status, ids],
                        )
                        toast_ok("Baixa aplicada", 2)
                        st.rerun()

            with st.expander("Gerar várias RECEITAS pendentes (ex: boletos)", expanded=False):
                n = st.number_input("Quantidade", min_value=1, max_value=200, value=5, step=1, key="lot_rec_n")
                desc_base = st.text_input("Descrição base", value="Boleto", key="lot_rec_desc")
                dt_prev = st.date_input("Data prevista (competência)", value=date.today(), key="lot_rec_dt")
                v_txt = st.text_input("Valor (R$) de cada", value="0,00", key="lot_rec_val")

                contas_all = list_contas(only_active=True)
                conta_ids = contas_all.loc[contas_all["tipo"]=="CONTA", "id"].tolist()
                if not conta_ids:
                    st.error("Cadastre pelo menos uma CONTA (ex: Cora).")
                else:
                    conta_sel = st.selectbox("Conta (recebimento)", options=conta_ids,
                                             format_func=lambda k: contas_all.loc[contas_all["id"]==k, "nome"].iloc[0],
                                             key="lot_rec_conta")

                    cat_df = fetch_df("SELECT id, nome FROM categorias WHERE ativo=TRUE ORDER BY nome")
                    cat_choice = st.selectbox("Categoria", options=cat_df["id"].tolist(),
                                              format_func=lambda k: cat_df.loc[cat_df["id"]==k, "nome"].iloc[0],
                                              key="lot_rec_cat") if not cat_df.empty else None

                    if st.button("Gerar receitas", type="primary", use_container_width=True, key="lot_rec_go"):
                        v = parse_brl(v_txt)
                        if v <= 0:
                            st.error("Valor inválido.")
                        else:
                            rows = []
                            for i in range(int(n)):
                                rows.append((
                                    "RECEITA",
                                    f"{desc_base.strip()} #{i+1}",
                                    float(v),
                                    dt_prev.isoformat(),
                                    None,
                                    int(conta_sel),
                                    None,
                                    int(cat_choice) if cat_choice else None,
                                    None,
                                    "Pendente",
                                    None
                                ))
                            exec_many(
                                """
                                INSERT INTO lancamentos
                                  (tipo,descricao,valor,dt_competencia,dt_liquidacao,conta_id,fatura_id,categoria_id,forma_pagamento,status,prestacao)
                                VALUES
                                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """,
                                rows,
                            )
                            toast_ok("Receitas pendentes geradas", 2)
                            st.rerun()


# ---------------- Boletos ----------------
with tabs[4]:
    st.subheader("Boletos (Agrupar receitas)")
    st.caption(
        "Aqui você lista todas as receitas pendentes (com filtros) e marca (checkbox) quais quer agrupar em um único boleto. "
        "O saldo da conta só muda quando estiver Recebido — então o boleto fica como previsão até baixar."
    )

    contas = list_contas(only_active=True)
    cats = cached_df("SELECT id, nome FROM categorias WHERE ativo=TRUE ORDER BY nome")

    if contas.empty:
        st.info("Cadastre contas primeiro.")
    else:
        conta_confs = contas.loc[contas["tipo"] == "CONTA"]
        if conta_confs.empty:
            st.error("Você precisa de pelo menos uma conta do tipo CONTA (ex: Cora) para gerar o boleto.")
        else:
            # FORM: evita rerun a cada mexida e diminui “desfoco”
            with st.form("form_boletos", clear_on_submit=False):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    conta_id = st.selectbox(
                        "Conta do boleto (receber)",
                        options=conta_confs["id"].tolist(),
                        format_func=lambda k: conta_confs.loc[conta_confs["id"] == k, "nome"].iloc[0],
                        key="bol_conta",
                    )
                with c2:
                    ano = st.number_input("Ano (origem)", min_value=2000, max_value=2100, value=date.today().year, step=1, key="bol_ano")
                with c3:
                    mes = st.number_input("Mês (origem)", min_value=1, max_value=12, value=date.today().month, step=1, key="bol_mes")
                with c4:
                    venc = st.date_input("Vencimento do boleto", value=(date.today() + relativedelta(days=10)), key="bol_venc")

                c5, c6, c7 = st.columns(3)
                with c5:
                    cat_id = None
                    if not cats.empty:
                        cat_id = st.selectbox(
                            "Categoria do boleto",
                            options=cats["id"].tolist(),
                            format_func=lambda k: cats.loc[cats["id"] == k, "nome"].iloc[0],
                            key="bol_cat",
                        )
                with c6:
                    texto = st.text_input("Filtrar descrição (contém)", value="", key="bol_txt")
                with c7:
                    mostrar_todos = st.checkbox("Mostrar tudo (ignora mês/ano)", value=False, key="bol_all")

                desc = st.text_input("Descrição do boleto", value=f"Boleto agrupado {int(mes):02d}/{int(ano)}", key="bol_desc")
                st.caption("Clique em **Aplicar filtros** depois de ajustar os campos acima (melhora performance).")

                st.form_submit_button("Aplicar filtros", use_container_width=True)

            # período
            if mostrar_todos:
                dt_ini = date(2000, 1, 1)
                dt_fim = date(2100, 12, 31)
            else:
                dt_ini = date(int(ano), int(mes), 1)
                dt_fim = (dt_ini + relativedelta(months=1)) - relativedelta(days=1)

            params = [dt_ini.isoformat(), dt_fim.isoformat()]
            q = """
                SELECT id, descricao, valor::float8 AS valor, dt_competencia
                  FROM lancamentos
                 WHERE tipo='RECEITA'
                   AND COALESCE(status,'Pendente')='Pendente'
                   AND dt_competencia BETWEEN %s AND %s
            """

            if (texto or "").strip():
                q += " AND descricao ILIKE %s"
                params.append(f"%{texto.strip()}%")

            q += " ORDER BY dt_competencia, id"

            df_pend = cached_df(q, tuple(params))

            if df_pend.empty:
                st.info("Nenhuma RECEITA pendente encontrada com os filtros atuais.")
            else:
                df_tbl = df_pend.copy()
                df_tbl["Selecionar"] = False
                df_tbl = df_tbl.rename(columns={"descricao": "Descrição", "valor": "Valor", "dt_competencia": "Data"})
                df_tbl["Data"] = pd.to_datetime(df_tbl["Data"]).dt.strftime("%d/%m/%Y")
                df_tbl["Valor"] = df_tbl["Valor"].apply(br_money)

                edited = st.data_editor(
                    df_tbl[["Selecionar", "Data", "Descrição", "Valor"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Selecionar": st.column_config.CheckboxColumn("✔"),
                        "Data": st.column_config.TextColumn("Data"),
                        "Descrição": st.column_config.TextColumn("Descrição"),
                        "Valor": st.column_config.TextColumn("Valor"),
                    },
                    key="bol_table",
                )

                selected_mask = edited["Selecionar"].astype(bool).values
                ids = df_pend.loc[selected_mask, "id"].tolist()

                total = float(df_pend.loc[selected_mask, "valor"].sum()) if len(ids) else 0.0
                st.info(f"Selecionados: {len(ids)} • Total: {br_money(total)}")

                cbtn1, cbtn2 = st.columns([0.6, 0.4])
                with cbtn1:
                    gerar = st.button("Gerar boleto com selecionados", type="primary", use_container_width=True, key="bol_gerar")
                with cbtn2:
                    limpar = st.button("Limpar seleção", use_container_width=True, key="bol_limpar")

                if limpar:
                    st.session_state.pop("bol_table", None)
                    st.rerun()

                if gerar:
                    if not ids:
                        st.error("Marque pelo menos uma receita.")
                    elif total <= 0:
                        st.error("Total inválido.")
                    else:
                        row = fetch_one(
                            """
                            INSERT INTO lancamentos
                              (tipo,descricao,valor,dt_competencia,dt_liquidacao,conta_id,fatura_id,categoria_id,forma_pagamento,status,prestacao)
                            VALUES
                              ('RECEITA',%s,%s,%s,NULL,%s,NULL,%s,'Boleto','Pendente',NULL)
                            RETURNING id
                            """,
                            [desc.strip(), float(total), venc.isoformat(), int(conta_id), (int(cat_id) if cat_id else None)],
                        )
                        boleto_id = int(row["id"])

                        exec_sql(
                            "UPDATE lancamentos SET status='Agrupada', forma_pagamento=%s WHERE id = ANY(%s)",
                            [f"Boleto:{boleto_id}", ids],
                        )

                        st.toast(f"Boleto criado • Total {br_money(total)}", icon="✅")
                        clear_cache()
                        st.session_state.pop("bol_table", None)
                        st.rerun()

    st.divider()
    st.markdown("### Desagrupar boleto")
    st.caption("Se precisar desfazer, informe o ID interno do boleto (lançamento criado).")

    with st.expander("Desagrupar por ID do boleto", expanded=False):
        bid = st.number_input("ID do boleto", min_value=1, step=1, value=1, key="bol_des_id")
        confirm = st.checkbox("Confirmo que quero desfazer o agrupamento", key="bol_des_confirm")
        if st.button("Desagrupar", type="primary", use_container_width=True, key="bol_des_btn"):
            if not confirm:
                st.error("Marque a confirmação.")
            else:
                exec_sql(
                    "UPDATE lancamentos SET status='Pendente', forma_pagamento=NULL WHERE forma_pagamento=%s",
                    [f"Boleto:{int(bid)}"],
                )
                exec_sql(
                    "DELETE FROM lancamentos WHERE id=%s AND tipo='RECEITA' AND forma_pagamento='Boleto'",
                    [int(bid)],
                )
                clear_cache()
                st.toast("Agrupamento desfeito", icon="✅")
                st.rerun()
# ---------------- Fechamento ----------------
with tabs[5]:
    st.subheader("Fechamento e Pagamento de Faturas")
    contas_cartao = fetch_df("SELECT id, nome FROM contas WHERE tipo='CARTAO' AND ativo=TRUE ORDER BY nome")
    if contas_cartao.empty:
        st.info("Cadastre cartões em Contas.")
    else:
        cartao_nome = st.selectbox("Cartão", contas_cartao["nome"].tolist(), key="fc_cartao")
        cartao_id = int(contas_cartao.loc[contas_cartao["nome"] == cartao_nome, "id"].iloc[0])
        dff = list_faturas(cartao_id)
        if dff.empty:
            st.warning("Cadastre faturas para esse cartão.")
        else:
            # options
            opts = []
            for _, r in dff.iterrows():
                fid = int(r.name)
                total = total_fatura(fid)
                label = f"{pd.to_datetime(r['competencia']).strftime('%m/%Y')} • vence {pd.to_datetime(r['dt_vencimento']).strftime('%d/%m/%Y')} • {r['status']} • R$ {br_money(total)}"
                opts.append((fid, label, r["status"]))
            idx = 0
            choice = st.selectbox("Fatura", options=list(range(len(opts))), format_func=lambda i: opts[i][1], index=idx, key="fc_fatura")
            fatura_id = int(opts[choice][0])
            status_fat = str(opts[choice][2])
            total = total_fatura(fatura_id)

            c1, c2, c3 = st.columns(3)
            c1.metric("Total da fatura (R$)", br_money(total))
            row = fetch_one("""
              SELECT dt_inicio, dt_fim, dt_fechamento, dt_vencimento, status
              FROM faturas WHERE id=%s
            """, [fatura_id])
            if row:
                c2.metric("Período", f"{pd.to_datetime(row['dt_inicio']).strftime('%d/%m')} → {pd.to_datetime(row['dt_fim']).strftime('%d/%m')}")
                c3.metric("Vencimento", pd.to_datetime(row["dt_vencimento"]).strftime("%d/%m/%Y"))

            st.markdown("#### Ações")
            a1, a2 = st.columns(2)
            with a1:
                if st.button("Marcar como FECHADA", use_container_width=True, key="fc_fechar"):
                    if status_fat == "PAGA":
                        st.warning("Já está PAGA.")
                    else:
                        exec_sql("UPDATE faturas SET status='FECHADA' WHERE id=%s", [fatura_id])
                        toast_ok("Fatura fechada")
                        st.rerun()

            with a2:
                if st.button("Marcar como ABERTA", use_container_width=True, key="fc_abrir"):
                    if status_fat == "PAGA":
                        st.warning("Já está PAGA.")
                    else:
                        exec_sql("UPDATE faturas SET status='ABERTA' WHERE id=%s", [fatura_id])
                        toast_ok("Fatura aberta")
                        st.rerun()

            st.divider()
            st.markdown("#### Registrar pagamento (saindo do Cora)")
            cora = fetch_one("SELECT id FROM contas WHERE nome='Cora' AND ativo=TRUE")
            if not cora:
                st.error("Conta 'Cora' não encontrada.")
            else:
                dt_pg = st.date_input("Data do pagamento", value=date.today(), key="fc_pgdt")
                valor_pg_txt = st.text_input("Valor pago (R$)", value=br_money(total), key="fc_pgval")
                if st.button("Pagar fatura ✅", type="primary", use_container_width=True, key="fc_pagar"):
                    if status_fat == "PAGA":
                        st.warning("Fatura já está paga.")
                    else:
                        valor_pg = parse_brl(valor_pg_txt)
                        if valor_pg <= 0:
                            st.error("Valor pago inválido.")
                        else:
                            # categoria Pagamento de Fatura
                            cat = fetch_one("SELECT id FROM categorias WHERE nome='Pagamento de Fatura'")
                            cat_id = int(cat["id"]) if cat else None

                            # cria lançamento de saída no Cora
                            row = fetch_one("""
                              SELECT c.nome AS cartao, f.competencia
                              FROM faturas f JOIN contas c ON c.id=f.conta_id
                              WHERE f.id=%s
                            """, [fatura_id])
                            comp_lbl = pd.to_datetime(row["competencia"]).strftime("%m/%Y") if row else ""
                            desc = f"Pagamento Fatura - {cartao_nome} ({comp_lbl})"

                            # inserir lançamento e obter id
                            with get_conn() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("""
                                      INSERT INTO lancamentos
                                        (tipo,descricao,valor,dt_competencia,dt_liquidacao,conta_id,categoria_id,forma_pagamento,status)
                                      VALUES
                                        ('DESPESA',%s,%s,%s,%s,%s,%s,'Transferência','Pago')
                                      RETURNING id
                                    """, (
                                        desc,
                                        float(valor_pg),
                                        dt_pg.isoformat(),
                                        dt_pg.isoformat(),
                                        int(cora["id"]),
                                        cat_id,
                                    ))
                                    lanc_id = int(cur.fetchone()[0])

                                    cur.execute("""
                                      INSERT INTO pagamentos_fatura (fatura_id, lancamento_saida_id, dt_pagamento, valor)
                                      VALUES (%s,%s,%s,%s)
                                    """, (fatura_id, lanc_id, dt_pg.isoformat(), float(valor_pg)))

                                    cur.execute("UPDATE faturas SET status='PAGA' WHERE id=%s", (fatura_id,))
                                conn.commit()

                            toast_ok("Pagamento registrado e fatura marcada como PAGA", 4)
                            st.rerun()

            st.divider()
            st.markdown("#### Itens da fatura")
            df_it = fetch_df("""
              SELECT l.dt_competencia, l.descricao, l.valor::float8 AS valor, COALESCE(cat.nome,'') AS categoria
              FROM lancamentos l
              LEFT JOIN categorias cat ON cat.id=l.categoria_id
              WHERE l.fatura_id=%s
              ORDER BY l.dt_competencia ASC, l.id ASC
            """, [fatura_id])
            if df_it.empty:
                st.info("Sem lançamentos vinculados a essa fatura.")
            else:
                df_it["dt_competencia"] = pd.to_datetime(df_it["dt_competencia"]).dt.strftime("%d/%m/%Y")
                df_it["valor"] = df_it["valor"].apply(br_money)
                st.dataframe(df_it, use_container_width=True, hide_index=True)

# ---------------- BI ----------------
with tabs[6]:
    st.subheader("BI do mês (Receitas x Despesas + por categoria)")
    contas = list_contas(only_active=True)
    mes_ref = st.date_input("Mês de referência", value=month_start(date.today()), key="bi_mes")
    mes_ref = month_start(mes_ref)
    ini = mes_ref
    fim = (mes_ref + relativedelta(months=1) - relativedelta(days=1))

    df = fetch_df("""
      SELECT l.tipo,
             l.valor::float8 AS valor,
             l.dt_competencia,
             COALESCE(cat.nome,'') AS categoria,
             c.nome AS conta
      FROM lancamentos l
      JOIN contas c ON c.id=l.conta_id
      LEFT JOIN categorias cat ON cat.id=l.categoria_id
      WHERE l.dt_competencia BETWEEN %s AND %s
    """, [ini.isoformat(), fim.isoformat()])

    if df.empty:
        st.info("Sem dados nesse mês.")
    else:
        rec = float(df.loc[df["tipo"]=="RECEITA", "valor"].sum())
        desp = float(df.loc[df["tipo"]=="DESPESA", "valor"].sum())
        saldo = rec - desp

        c1, c2, c3 = st.columns(3)
        c1.metric("Receitas (R$)", br_money(rec))
        c2.metric("Despesas (R$)", br_money(desp))
        c3.metric("Saldo do mês (R$)", br_money(saldo))

        st.markdown("### Por categoria (Despesas)")
        df_cat = df[df["tipo"]=="DESPESA"].groupby("categoria", as_index=False)["valor"].sum().sort_values("valor", ascending=False)
        df_cat["valor"] = df_cat["valor"].apply(br_money)
        st.dataframe(df_cat, use_container_width=True, hide_index=True)

        st.markdown("### Por dia (Receitas x Despesas)")
        df_day = df.copy()
        df_day["dia"] = pd.to_datetime(df_day["dt_competencia"]).dt.strftime("%d/%m")
        piv = df_day.pivot_table(index="dia", columns="tipo", values="valor", aggfunc="sum", fill_value=0).reset_index()
        st.dataframe(piv, use_container_width=True, hide_index=True)

        st.markdown("### Saldo Cora (caixa real)")
        st.metric("Saldo Cora (REAL) (R$)", br_money(saldo_cora()))
        st.caption(f"Previsão a receber: {br_money(previsao_receber_conta('Cora'))} • a pagar: {br_money(previsao_pagar_conta('Cora'))}")
