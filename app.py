# app_corrigido_neon.py — Controle Financeiro (Streamlit) — Postgres (Neon)
# Requisitos:
#   python -m pip install streamlit pandas python-dateutil psycopg2-binary
#
# Secrets (Streamlit Cloud):
#   DATABASE_URL = "postgresql://user:pass@host/db?sslmode=require&channel_binding=require"
#   APP_USERS    = "silvia:Senha;admin:Senha"
#
# Rodar local (PowerShell):
#   $env:APP_USERS="silvia:Senha;admin:Senha"
#   $env:DATABASE_URL="postgresql://..."
#   python -m streamlit run app_corrigido_neon.py

import os
import re
import time
import hmac
import hashlib
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

# Postgres
import psycopg2
from psycopg2.extras import RealDictCursor


# =========================
# Helpers: segurança + login
# =========================
def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _parse_users(raw: str) -> dict:
    """
    raw: 'user:pass;user2:pass2'
    return: {user: sha256(pass)}
    """
    users = {}
    raw = (raw or "").strip()
    if not raw:
        return users
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
    raw = None
    try:
        raw = st.secrets.get("APP_USERS")  # type: ignore[attr-defined]
    except Exception:
        raw = None
    if not raw:
        raw = os.getenv("APP_USERS", "")

    users = _parse_users(raw)
    if not users:
        st.error("Credenciais não configuradas. Defina APP_USERS (ex: silvia:Senha;admin:Senha).")
        st.stop()

    if "auth_ok" not in st.session_state:
        st.session_state["auth_ok"] = False
    if "auth_user" not in st.session_state:
        st.session_state["auth_user"] = ""

    if st.session_state["auth_ok"]:
        return

    st.markdown("<h2 style='text-align:center;'>🔒 Acesso restrito</h2>", unsafe_allow_html=True)
    usuario = st.text_input("Usuário", key="login_user")
    senha = st.text_input("Senha", type="password", key="login_pwd")

    if st.button("Entrar", type="primary", use_container_width=True, key="login_btn"):
        usuario = (usuario or "").strip()
        senha_hash = _sha256(senha or "")
        ok = usuario in users and hmac.compare_digest(users[usuario], senha_hash)
        if ok:
            st.session_state["auth_ok"] = True
            st.session_state["auth_user"] = usuario
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")

    st.stop()

def logout_button() -> None:
    c1, c2 = st.columns([0.8, 0.2])
    with c1:
        st.caption(f"👤 Logado como: **{st.session_state.get('auth_user','')}**")
    with c2:
        if st.button("Sair", use_container_width=True, key="logout_btn"):
            st.session_state["auth_ok"] = False
            st.session_state["auth_user"] = ""
            st.rerun()


# =========================
# Toast fixo (5s)
# =========================
def toast_ok(msg: str, seconds: int = 5) -> None:
    """
    Streamlit 'toast' não permite tempo fixo.
    Aqui a gente usa um placeholder (success) que fica na tela e some depois.
    """
    ph = st.empty()
    ph.success(f"OK ✅ {msg}")
    time.sleep(max(int(seconds), 1))
    ph.empty()


# =========================
# BRL: parse + format
# =========================
def br_money(v: float) -> str:
    # 1630.0 -> "1.630,00"
    return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def parse_brl(s: Any) -> float:
    """
    Aceita:
      "1.630,00" -> 1630.0
      "1630,00"  -> 1630.0
      "1630.00"  -> 1630.0
      1630       -> 1630.0
    """
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).strip()
    if not t:
        return 0.0
    t = t.replace("R$", "").strip()
    # remove espaços, mantém dígitos, vírgula e ponto e sinal
    t = re.sub(r"[^\d,.\-]", "", t)

    # Se tem vírgula e ponto, assume BR (ponto milhar, vírgula decimal)
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        # Se só vírgula, vira decimal
        if "," in t and "." not in t:
            t = t.replace(",", ".")
        # Se só ponto, já é decimal
    try:
        return float(t)
    except Exception:
        return 0.0


# =========================
# Config + Layout
# =========================
st.set_page_config(page_title="Controle Financeiro", page_icon="💸", layout="wide")

# PC: largura total; Mobile: padding confortável
st.markdown(
    """
<style>
@media (max-width: 768px) {
  .block-container {
    padding-left: 1rem !important;
    padding-right: 1rem !important;
  }
}
.stTabs [data-baseweb="tab"] {
  font-weight: 800 !important;
  padding: 12px 16px !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# Login
require_login()
logout_button()

st.markdown(
    """
<div style="text-align:center; margin-bottom: 1.5rem;">
  <h1 style="font-weight:700; margin-bottom: 0.25rem;">
    💸 Controle Financeiro <span style="color:#e63946;">❤️</span>
  </h1>
  <small style="color:#777;">Feito com <span style="color:#e63946;">❤️</span> pra Silvia</small>
</div>
""",
    unsafe_allow_html=True,
)

modo_view = st.selectbox("Modo de visualização", ["Auto", "Celular", "PC"], index=0, key="view_mode")
is_mobile = (modo_view == "Celular")


# =========================
# Banco (Postgres)
# =========================
def get_database_url() -> str:
    url = ""
    try:
        url = st.secrets.get("DATABASE_URL", "")  # type: ignore[attr-defined]
    except Exception:
        url = ""
    if not url:
        url = os.getenv("DATABASE_URL", "")
    return (url or "").strip()

def get_conn():
    url = get_database_url()
    if not url:
        st.error("DATABASE_URL não configurada (Neon/Postgres).")
        st.stop()
    # conn como context manager
    return psycopg2.connect(url)

def init_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lancamentos (
                    id              BIGSERIAL PRIMARY KEY,
                    descricao       TEXT NOT NULL,
                    valor           NUMERIC(14,2) NOT NULL,
                    data            DATE NOT NULL,      -- competência
                    prestacao       TEXT,
                    forma_pagamento TEXT,
                    status          TEXT,
                    categoria       TEXT,
                    conta_corrente  TEXT,
                    data_pagamento  DATE,               -- liquidação
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
        conn.commit()

init_db()


# =========================
# CRUD
# =========================
CATEGORIAS = [
    "Saúde", "Alimentação", "Transporte (Gasolina)", "Farmácia", "Educação",
    "Lazer", "Crianças", "Pessoal", "Investimentos",
    "Desnecessário", "Beleza", "Funcionários", "Trabalho", "Outros"
]
FORMAS_PAGAMENTO = ["", "PIX", "Débito", "Crédito", "Boleto", "Dinheiro", "Transferência"]
STATUS_LISTA = ["Pago", "Pendente", "Atrasado", "Cancelado"]

def inserir_varios(linhas: List[Dict[str, Any]]) -> None:
    sql = """
        INSERT INTO lancamentos
        (descricao, valor, data, prestacao, forma_pagamento, status, categoria, conta_corrente, data_pagamento)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    vals = []
    for l in linhas:
        vals.append(
            (
                (l.get("Descricao") or "").strip(),
                float(l.get("Valor") or 0),
                str(l.get("Data")),  # YYYY-MM-DD
                (l.get("Prestacao") or "").strip(),
                (l.get("Forma_de_Pagamento") or "").strip(),
                (l.get("Status") or "").strip(),
                (l.get("Categoria") or "").strip(),
                (l.get("Conta_Corrente") or "").strip(),
                l.get("Data_Pagamento") or None,
            )
        )
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, vals)
        conn.commit()

def atualizar_lancamento(id_: int, dados: Dict[str, Any]) -> None:
    sql = """
        UPDATE lancamentos
           SET descricao=%s,
               valor=%s,
               data=%s,
               prestacao=%s,
               forma_pagamento=%s,
               status=%s,
               categoria=%s,
               conta_corrente=%s,
               data_pagamento=%s
         WHERE id=%s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    (dados.get("Descricao") or "").strip(),
                    float(dados.get("Valor") or 0),
                    str(dados.get("Data")),
                    (dados.get("Prestacao") or "").strip(),
                    (dados.get("Forma_de_Pagamento") or "").strip(),
                    (dados.get("Status") or "").strip(),
                    (dados.get("Categoria") or "").strip(),
                    (dados.get("Conta_Corrente") or "").strip(),
                    dados.get("Data_Pagamento") or None,
                    int(id_),
                ),
            )
        conn.commit()

def atualizar_varios(df_edit: pd.DataFrame) -> Tuple[int, List[str]]:
    """
    df_edit com colunas:
      ID, Descricao, Valor, DataISO, Prestacao, Forma_de_Pagamento, Categoria, Conta_Corrente, Data_PagamentoISO, Status
    """
    erros: List[str] = []
    ok = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for idx, row in df_edit.iterrows():
                try:
                    id_ = int(row["ID"])
                except Exception:
                    erros.append(f"Linha {idx+1}: ID inválido.")
                    continue

                # data
                try:
                    dt = pd.to_datetime(str(row["DataISO"])).date()
                except Exception:
                    erros.append(f"Linha {idx+1}: Data inválida.")
                    continue

                # valor
                v = parse_brl(row["Valor"])

                # data pagamento (opcional)
                dp = None
                dp_raw = str(row.get("Data_PagamentoISO", "")).strip()
                if dp_raw:
                    try:
                        dp = pd.to_datetime(dp_raw).date()
                    except Exception:
                        erros.append(f"Linha {idx+1}: Data_Pagamento inválida.")
                        continue

                try:
                    cur.execute(
                        """
                        UPDATE lancamentos
                           SET descricao=%s,
                               valor=%s,
                               data=%s,
                               prestacao=%s,
                               forma_pagamento=%s,
                               status=%s,
                               categoria=%s,
                               conta_corrente=%s,
                               data_pagamento=%s
                         WHERE id=%s
                        """,
                        (
                            str(row.get("Descricao", "")).strip(),
                            float(v),
                            dt.isoformat(),
                            str(row.get("Prestacao", "")).strip(),
                            str(row.get("Forma_de_Pagamento", "")).strip(),
                            str(row.get("Status", "")).strip(),
                            str(row.get("Categoria", "")).strip(),
                            str(row.get("Conta_Corrente", "")).strip(),
                            (dp.isoformat() if dp else None),
                            id_,
                        ),
                    )
                    ok += 1
                except Exception as e:
                    erros.append(f"Linha {idx+1}: erro ao salvar ({e}).")
            conn.commit()
    return ok, erros

def get_por_id(id_: int):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, descricao, valor, data, prestacao, forma_pagamento,
                       status, categoria, conta_corrente, data_pagamento
                  FROM lancamentos
                 WHERE id=%s
                """,
                (int(id_),),
            )
            return cur.fetchone()

def deletar(id_: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM lancamentos WHERE id=%s", (int(id_),))
        conn.commit()

def liquidar_ids(ids: List[int], data_pag: date) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE lancamentos
                   SET status='Pago',
                       data_pagamento=%s
                 WHERE id=%s
                """,
                [(data_pag.isoformat(), int(i)) for i in ids],
            )
        conn.commit()

def buscar_df(where_sql: str = "", params: Optional[List[Any]] = None, order_sql: str = "ORDER BY data DESC, id DESC") -> pd.DataFrame:
    params = params or []
    sql = f"""
        SELECT
            id AS "ID",
            descricao AS "Descricao",
            valor::float8 AS "Valor",
            data AS "DataISO",
            COALESCE(prestacao,'') AS "Prestacao",
            COALESCE(forma_pagamento,'') AS "Forma_de_Pagamento",
            COALESCE(categoria,'') AS "Categoria",
            COALESCE(conta_corrente,'') AS "Conta_Corrente",
            data_pagamento AS "Data_PagamentoISO",
            COALESCE(status,'') AS "Status"
        FROM lancamentos
        {where_sql}
        {order_sql}
    """
    with get_conn() as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame(columns=[
            "ID","Descricao","Valor","DataISO","Prestacao","Forma_de_Pagamento","Categoria","Conta_Corrente","Data_PagamentoISO","Status"
        ])

    # datas em BR pra view (quando precisar)
    df["Data"] = pd.to_datetime(df["DataISO"], errors="coerce").dt.strftime("%d/%m/%Y")
    df["Data_Pagamento"] = pd.to_datetime(df["Data_PagamentoISO"], errors="coerce").dt.strftime("%d/%m/%Y")
    return df

def listar_opcoes_para_edicao(filtro_texto: str = "") -> List[Tuple[int, str]]:
    where = ""
    params: List[Any] = []
    if filtro_texto.strip():
        where = "WHERE descricao ILIKE %s OR categoria ILIKE %s OR conta_corrente ILIKE %s"
        t = f"%{filtro_texto.strip()}%"
        params = [t, t, t]
    sql = f"""
        SELECT id, descricao, valor::float8 AS valor, data, prestacao, COALESCE(status,'') AS status
        FROM lancamentos
        {where}
        ORDER BY data DESC, id DESC
        LIMIT 300
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    opcoes: List[Tuple[int, str]] = []
    for (id_, desc, val, dt, prest, status) in rows:
        data_br = pd.to_datetime(dt).strftime("%d/%m/%Y")
        prest_txt = f" [{prest}]" if prest else ""
        label = f"{data_br} • R$ {br_money(val)} • {desc}{prest_txt} • {status or 'Pendente'}"
        opcoes.append((int(id_), label))
    return opcoes


# =========================
# Parcelas (Prévia)
# =========================
def dividir_total_em_parcelas(valor_total: float, qtd: int) -> List[float]:
    if qtd <= 1:
        return [round(valor_total, 2)]
    base = round(valor_total / qtd, 2)
    parcelas = [base] * qtd
    soma = round(sum(parcelas), 2)
    diff = round(valor_total - soma, 2)
    parcelas[-1] = round(parcelas[-1] + diff, 2)
    return parcelas

def gerar_previa_parcelas(
    descricao: str,
    qtd: int,
    data_inicial: date,
    modo: str,
    valor_total: float | None,
    valor_parcela: float | None,
    forma_pag: str,
    categoria: str,
    conta: str,
    status: str,
) -> pd.DataFrame:
    qtd = max(int(qtd), 1)
    if modo == "Total → dividir por parcelas":
        valores = dividir_total_em_parcelas(float(valor_total or 0.0), qtd)
    else:
        v = round(float(valor_parcela or 0.0), 2)
        valores = [v] * qtd

    linhas = []
    for i in range(qtd):
        dt = data_inicial + relativedelta(months=i)
        linhas.append({
            "Descricao": descricao.strip(),
            "Valor": round(float(valores[i]), 2),
            "Data": dt.strftime("%d/%m/%Y"),
            "Prestacao": f"{i+1}/{qtd}" if qtd > 1 else "",
            "Forma_de_Pagamento": forma_pag,
            "Categoria": categoria,
            "Conta_Corrente": conta,
            "Data_Pagamento": (date.today().strftime("%d/%m/%Y") if status == "Pago" else ""),
            "Status": status,
        })
    df = pd.DataFrame(linhas)
    ordem = ["Descricao","Valor","Data","Prestacao","Forma_de_Pagamento","Categoria","Conta_Corrente","Data_Pagamento","Status"]
    return df[[c for c in ordem if c in df.columns]]


# =========================
# UI: Tabs
# =========================
tab_lancar, tab_listar, tab_pendentes, tab_editar_lote, tab_excluir = st.tabs(
    ["➕ Lançar (prévia)", "📋 Listar", "✅ Pendentes", "📝 Editar em lote", "🗑️ Excluir"]
)

# ---------------- TAB: Lançar ----------------
with tab_lancar:
    st.subheader("Novo lançamento (com prévia de parcelas)")

    descricao = st.text_input("Descrição", placeholder="Ex: Mercado / Cartão / Curso...", key="l_desc")

    if is_mobile:
        qtd_parcelas = st.number_input("Qtd. parcelas", min_value=1, step=1, value=1, key="l_qtd")
        data_inicial = st.date_input("Data da 1ª parcela", value=date.today(), format="DD/MM/YYYY", key="l_dtini")
    else:
        c1, c2 = st.columns(2)
        with c1:
            qtd_parcelas = st.number_input("Qtd. parcelas", min_value=1, step=1, value=1, key="l_qtd")
        with c2:
            data_inicial = st.date_input("Data da 1ª parcela", value=date.today(), format="DD/MM/YYYY", key="l_dtini")

    modo = st.radio(
        "Como você quer informar o valor?",
        ["Total → dividir por parcelas", "Parcela fixa → repetir valor em todas"],
        horizontal=not is_mobile,
        key="l_modo"
    )

    # Troca o number_input por text_input pra não virar '0,001500,00' quando digita em cima do 0,00
    def valor_input(label: str, key: str, disabled: bool) -> str:
        return st.text_input(
            label,
            value=("0,00" if key not in st.session_state else st.session_state.get(key, "0,00")),
            placeholder="Ex: 1500,00",
            disabled=disabled,
            key=key,
        )

    if is_mobile:
        vtotal_txt = valor_input("Valor Total (R$)", "l_vtotal_txt", disabled=(modo != "Total → dividir por parcelas"))
        vparc_txt  = valor_input("Valor da Parcela (R$)", "l_vparc_txt", disabled=(modo != "Parcela fixa → repetir valor em todas"))
        forma_pag = st.selectbox("Forma de Pagamento", FORMAS_PAGAMENTO, index=0, key="l_forma")
        categoria = st.selectbox("Categoria", CATEGORIAS, index=0, key="l_cat")
        conta = st.text_input("Conta Corrente", placeholder="Ex: Nubank, Itaú...", key="l_conta")
        status = st.selectbox("Status", STATUS_LISTA, index=1, key="l_status")
    else:
        c3, c4 = st.columns(2)
        with c3:
            vtotal_txt = valor_input("Valor Total (R$)", "l_vtotal_txt", disabled=(modo != "Total → dividir por parcelas"))
        with c4:
            vparc_txt  = valor_input("Valor da Parcela (R$)", "l_vparc_txt", disabled=(modo != "Parcela fixa → repetir valor em todas"))

        c5, c6 = st.columns(2)
        with c5:
            forma_pag = st.selectbox("Forma de Pagamento", FORMAS_PAGAMENTO, index=0, key="l_forma")
        with c6:
            categoria = st.selectbox("Categoria", CATEGORIAS, index=0, key="l_cat")

        c7, c8 = st.columns(2)
        with c7:
            conta = st.text_input("Conta Corrente", placeholder="Ex: Nubank, Itaú...", key="l_conta")
        with c8:
            status = st.selectbox("Status", STATUS_LISTA, index=1, key="l_status")

    st.divider()
    b1, b2 = st.columns(2)
    with b1:
        btn_previa = st.button("Gerar prévia", type="primary", use_container_width=True, key="l_previa")
    with b2:
        btn_limpar = st.button("Limpar prévia", use_container_width=True, key="l_limpar")

    if btn_limpar:
        st.session_state.pop("previa_df", None)
        st.rerun()

    if btn_previa:
        if not descricao.strip():
            st.error("Preencha a **Descrição**.")
        else:
            v_total = parse_brl(vtotal_txt) if modo == "Total → dividir por parcelas" else None
            v_parc = parse_brl(vparc_txt) if modo == "Parcela fixa → repetir valor em todas" else None
            df_previa = gerar_previa_parcelas(
                descricao=descricao,
                qtd=int(qtd_parcelas),
                data_inicial=data_inicial,
                modo=modo,
                valor_total=v_total,
                valor_parcela=v_parc,
                forma_pag=forma_pag,
                categoria=categoria,
                conta=conta,
                status=status,
            )
            st.session_state["previa_df"] = df_previa
            toast_ok("Prévia gerada", 3)

    df_previa = st.session_state.get("previa_df")

    if isinstance(df_previa, pd.DataFrame) and not df_previa.empty:
        # Para evitar erro de compatibilidade do data_editor:
        # - mostramos "Valor" como texto (BRL), e parseamos de volta no salvar.
        df_previa_view = df_previa.copy()
        df_previa_view["Valor"] = df_previa_view["Valor"].apply(br_money)

        total_calc = float(df_previa["Valor"].sum())
        st.caption(f"Total da prévia: **R$ {br_money(total_calc)}**")

        st.write("### Prévia (ajuste se quiser e salve)")
        edited = st.data_editor(
            df_previa_view,
            use_container_width=True,
            hide_index=True,
            key="l_editor",
            column_config={
                "Valor": st.column_config.TextColumn("Valor (R$)", help="Aceita 1500,00 ou 1.500,00"),
                "Forma_de_Pagamento": st.column_config.SelectboxColumn("Forma de Pagamento", options=FORMAS_PAGAMENTO),
                "Categoria": st.column_config.SelectboxColumn("Categoria", options=CATEGORIAS),
                "Status": st.column_config.SelectboxColumn("Status", options=STATUS_LISTA),
                "Data": st.column_config.TextColumn("Data (dd/mm/aaaa)"),
                "Data_Pagamento": st.column_config.TextColumn("Data Pgto (dd/mm/aaaa)"),
            }
        )

        if st.button("Salvar parcelas 💾", type="primary", use_container_width=True, key="l_salvar"):
            linhas = []
            erros = []
            for idx, row in edited.iterrows():
                # Data
                try:
                    dt = pd.to_datetime(str(row["Data"]), dayfirst=True).date()
                except Exception:
                    erros.append(f"Linha {idx+1}: Data inválida '{row['Data']}' (use dd/mm/aaaa).")
                    continue

                # Valor (aceita BR) — agora vem como texto
                v = parse_brl(row["Valor"])

                # Data Pagamento (opcional)
                dp_iso = None
                dp_txt = str(row.get("Data_Pagamento", "")).strip()
                if dp_txt:
                    try:
                        dp = pd.to_datetime(dp_txt, dayfirst=True).date()
                        dp_iso = dp.isoformat()
                    except Exception:
                        erros.append(f"Linha {idx+1}: Data_Pagamento inválida '{row['Data_Pagamento']}' (use dd/mm/aaaa).")
                        continue

                linhas.append({
                    "Descricao": str(row["Descricao"]).strip(),
                    "Valor": float(v),
                    "Data": dt.isoformat(),
                    "Prestacao": str(row.get("Prestacao", "")).strip(),
                    "Forma_de_Pagamento": str(row.get("Forma_de_Pagamento", "")).strip(),
                    "Categoria": str(row.get("Categoria", "")).strip(),
                    "Conta_Corrente": str(row.get("Conta_Corrente", "")).strip(),
                    "Data_Pagamento": dp_iso,
                    "Status": str(row.get("Status", "")).strip(),
                })

        if erros:
            st.error("Corrija antes de salvar:\n\n- " + "\n- ".join(erros))
        else:
            inserir_varios(linhas)
            toast_ok("Parcelas salvas com sucesso", 5)
            st.session_state.pop("previa_df", None)
            st.rerun()
           
    else:
        st.info("Gere uma prévia para visualizar e salvar as parcelas.")


# ---------------- TAB: Listar ----------------
with tab_listar:
    st.subheader("Lançamentos")

    # Ordenação em PT-BR
    ordem = st.selectbox(
        "Ordenar por",
        ["Mais recentes", "Mais antigas", "Maior valor", "Menor valor", "Descrição (A→Z)", "Descrição (Z→A)"],
        index=0,
        key="li_ordem",
    )

    texto = st.text_input("Buscar", placeholder="mercado / lazer / nubank...", key="li_busca")
    status_f = st.selectbox("Status (filtro)", ["Todos"] + STATUS_LISTA, index=0, key="li_status")

    where = ""
    params: List[Any] = []
    if texto.strip():
        where = "WHERE (descricao ILIKE %s OR categoria ILIKE %s OR conta_corrente ILIKE %s)"
        t = f"%{texto.strip()}%"
        params += [t, t, t]
    if status_f != "Todos":
        where += (" AND " if where else "WHERE ") + "status = %s"
        params.append(status_f)

    if ordem == "Mais antigas":
        order_sql = "ORDER BY data ASC, id ASC"
    elif ordem == "Maior valor":
        order_sql = "ORDER BY valor DESC, data DESC, id DESC"
    elif ordem == "Menor valor":
        order_sql = "ORDER BY valor ASC, data DESC, id DESC"
    elif ordem == "Descrição (A→Z)":
        order_sql = "ORDER BY descricao ASC, data DESC, id DESC"
    elif ordem == "Descrição (Z→A)":
        order_sql = "ORDER BY descricao DESC, data DESC, id DESC"
    else:
        order_sql = "ORDER BY data DESC, id DESC"

    df = buscar_df(where_sql=where, params=params, order_sql=order_sql)
    total = float(df["Valor"].sum()) if not df.empty else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total (R$)", br_money(total))
    c2.metric("Qtd.", int(len(df)))
    c3.metric("Média (R$)", br_money(total / len(df) if len(df) else 0.0))

    # View: sem ID e com Valor BR
    if not df.empty:
        view = df.drop(columns=["ID"], errors="ignore").copy()
        view["Valor"] = view["Valor"].apply(br_money)
        view = view.drop(columns=["DataISO","Data_PagamentoISO"], errors="ignore")
        st.dataframe(view, use_container_width=True, hide_index=True, key="li_grid")
    else:
        st.info("Nada para mostrar com esses filtros.")


# ---------------- TAB: Pendentes ----------------
with tab_pendentes:
    st.subheader("Pendentes / Liquidação")

    texto = st.text_input("Buscar (descrição/categoria/conta)", placeholder="digita e filtra...", key="p_busca")

    if is_mobile:
        dt_ini = st.date_input("De", value=None, format="DD/MM/YYYY", key="p_ini")
        dt_fim = st.date_input("Até", value=None, format="DD/MM/YYYY", key="p_fim")
    else:
        c1, c2 = st.columns(2)
        with c1:
            dt_ini = st.date_input("De", value=None, format="DD/MM/YYYY", key="p_ini")
        with c2:
            dt_fim = st.date_input("Até", value=None, format="DD/MM/YYYY", key="p_fim")

    where = "WHERE (status IN ('Pendente','Atrasado') OR status IS NULL OR status='')"
    params: List[Any] = []

    if texto.strip():
        where += " AND (descricao ILIKE %s OR categoria ILIKE %s OR conta_corrente ILIKE %s)"
        t = f"%{texto.strip()}%"
        params += [t, t, t]
    if dt_ini:
        where += " AND data >= %s"
        params.append(dt_ini.isoformat())
    if dt_fim:
        where += " AND data <= %s"
        params.append(dt_fim.isoformat())

    df = buscar_df(where_sql=where, params=params, order_sql="ORDER BY data ASC, id ASC")

    if df.empty:
        st.info("Sem pendências nesse filtro.")
    else:
        df_sel = df.copy()
        df_sel.insert(0, "Selecionar", False)

        # Mostrar BR e sem ID (mas mantém ID separado p/ liquidar)
        df_show = df_sel.drop(columns=["ID","DataISO","Data_PagamentoISO"], errors="ignore").copy()
        df_show["Valor"] = df_show["Valor"].apply(br_money)

        edited = st.data_editor(
            df_show,
            use_container_width=True,
            hide_index=True,
            key="p_editor",
            column_config={"Selecionar": st.column_config.CheckboxColumn("Selecionar")},
            disabled=[c for c in df_show.columns if c != "Selecionar"],
        )

        linhas_marcadas = edited.index[edited["Selecionar"] == True].tolist()
        ids = df_sel.loc[linhas_marcadas, "ID"].tolist() if linhas_marcadas else []

        data_pag = st.date_input("Data de Pagamento", value=date.today(), format="DD/MM/YYYY", key="p_pgto")
        if st.button("Liquidar selecionados ✅", type="primary", use_container_width=True, key="p_btn"):
            if not ids:
                st.warning("Selecione pelo menos 1 lançamento.")
            else:
                liquidar_ids([int(i) for i in ids], data_pag)
                toast_ok(f"{len(ids)} lançamento(s) liquidados", 5)
                st.rerun()


# ---------------- TAB: Editar em lote ----------------
with tab_editar_lote:
    st.subheader("Editar em lote (planilha)")
    st.caption("Ideal pra corrigir várias parcelas de uma vez. Edite na planilha e salve no final.")

    filtro = st.text_input("🔎 Buscar", placeholder="mercado / nubank / lazer...", key="el_busca")
    status_f = st.selectbox("Status (filtro)", ["Todos"] + STATUS_LISTA, index=0, key="el_status")
    limite = st.slider("Quantos itens carregar", min_value=20, max_value=500, value=120, step=20, key="el_limite")

    where = ""
    params: List[Any] = []
    if filtro.strip():
        where = "WHERE (descricao ILIKE %s OR categoria ILIKE %s OR conta_corrente ILIKE %s)"
        t = f"%{filtro.strip()}%"
        params += [t, t, t]
    if status_f != "Todos":
        where += (" AND " if where else "WHERE ") + "status = %s"
        params.append(status_f)

    df = buscar_df(where_sql=where, params=params, order_sql=f"ORDER BY data DESC, id DESC LIMIT {int(limite)}")

    if df.empty:
        st.info("Nada encontrado com esse filtro.")
    else:
        # Para editar com segurança:
        # - DataISO e Data_PagamentoISO ficam em ISO e editáveis como texto
        df_edit = df[[
            "ID","Descricao","Valor","DataISO","Prestacao","Forma_de_Pagamento","Categoria","Conta_Corrente","Data_PagamentoISO","Status"
        ]].copy()

        # Valor mostra em BR, mas vai ser parseado no salvar
        df_edit["Valor"] = df_edit["Valor"].apply(br_money)

        edited = st.data_editor(
            df_edit,
            use_container_width=True,
            hide_index=True,
            key="el_editor",
            column_config={
                "ID": st.column_config.NumberColumn("ID", disabled=True),
                "Valor": st.column_config.TextColumn("Valor (R$)", help="Ex: 1.500,00"),
                "DataISO": st.column_config.TextColumn("Data (AAAA-MM-DD)"),
                "Data_PagamentoISO": st.column_config.TextColumn("Data Pgto (AAAA-MM-DD)"),
                "Forma_de_Pagamento": st.column_config.SelectboxColumn("Forma de Pagamento", options=FORMAS_PAGAMENTO),
                "Categoria": st.column_config.SelectboxColumn("Categoria", options=CATEGORIAS),
                "Status": st.column_config.SelectboxColumn("Status", options=STATUS_LISTA),
            },
        )

        c1, c2 = st.columns([0.7, 0.3])
        with c1:
            st.caption("Dica: você pode copiar e colar colunas inteiras (tipo Excel).")
        with c2:
            if st.button("Salvar alterações em lote 💾", type="primary", use_container_width=True, key="el_save"):
                ok, erros = atualizar_varios(edited)
                if erros:
                    st.error("Alguns itens não salvaram:\n\n- " + "\n- ".join(erros[:20]) + ("" if len(erros) <= 20 else f"\n\n(+{len(erros)-20} erros)"))
                toast_ok(f"{ok} item(ns) atualizado(s)", 5)
                st.rerun()


# ---------------- TAB: Excluir ----------------
with tab_excluir:
    st.subheader("Excluir lançamento")
    st.caption("Sem lixeira: excluiu, foi de base.")

    filtro = st.text_input("🔎 Buscar para excluir", placeholder="mercado / nubank / lazer...", key="d_busca")
    opcoes = listar_opcoes_para_edicao(filtro)

    if not opcoes:
        st.info("Nada encontrado com esse filtro.")
    else:
        # selectbox por ID (único) mas exibindo label amigável
        id_options = [int(i) for (i, _) in opcoes]
        label_map = {int(i): lbl for (i, lbl) in opcoes}
        id_del = st.selectbox(
            "Selecione",
            options=id_options,
            key="d_id",
            format_func=lambda i: label_map.get(int(i), str(i)),
            index=0,
        )

        confirmar = st.checkbox("Confirmo exclusão", key="d_conf")
        if st.button("Excluir 🗑️", type="primary", use_container_width=True, key="d_btn"):
            if not confirmar:
                st.warning("Marque a confirmação.")
            else:
                deletar(int(id_del))
                toast_ok("Lançamento excluído", 5)
                st.rerun()
