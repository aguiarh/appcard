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
    ph = st.empty()
    ph.success(f"OK ✅ {msg}")
    time.sleep(max(int(seconds), 1))
    ph.empty()

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

def saldo_cora() -> float:
    row = fetch_one("""
      SELECT
        c.saldo_inicial::float8
        + COALESCE(SUM(CASE WHEN l.tipo='RECEITA' THEN l.valor ELSE 0 END),0)::float8
        - COALESCE(SUM(CASE WHEN l.tipo='DESPESA' THEN l.valor ELSE 0 END),0)::float8
        AS saldo
      FROM contas c
      LEFT JOIN lancamentos l ON l.conta_id = c.id
      WHERE c.nome='Cora'
      GROUP BY c.saldo_inicial
    """)
    return float(row["saldo"]) if row else 0.0

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
    st.metric("Saldo Cora (R$)", br_money(saldo_cora()))
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

tabs = st.tabs(["🏦 Contas", "🧾 Faturas", "➕ Lançamentos", "💳 Fechamento", "📊 BI"])

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
        df_edit["saldo_inicial"] = df_edit["saldo_inicial"].fillna(0.0).astype(float)

        edited = st.data_editor(
            df_edit,
            use_container_width=True,
            hide_index=True,
            disabled=["id", "nome", "tipo"],
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
                    rows.append((float(r["saldo_inicial"]), bool(r["ativo"]), int(r["id"])))
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

# ---------------- Faturas ----------------
with tabs[1]:
    st.subheader("Faturas (datas reais por mês)")
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
with tabs[2]:
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
            status = st.text_input("Status (opcional)", value="", key="l_status")

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
                    opts.append((int(r["id"]), label))
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
            df_show["dt_competencia"] = pd.to_datetime(df_show["dt_competencia"]).dt.strftime("%d/%m/%Y")
            df_show["valor"] = df_show["valor"].apply(br_money)
            st.dataframe(df_show, use_container_width=True, hide_index=True)

# ---------------- Fechamento ----------------
with tabs[3]:
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
                fid = int(r["id"])
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
with tabs[4]:
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

        st.markdown("### Saldo Cora (agora)")
        st.metric("Saldo Cora (R$)", br_money(saldo_cora()))
