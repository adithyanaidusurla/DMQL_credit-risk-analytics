"""
Credit Risk Analytics Dashboard — Phase 3
Streamlit application connecting to Neon PostgreSQL via SQLAlchemy with connection pooling.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIG & CONNECTION
# ─────────────────────────────────────────────
load_dotenv()

st.set_page_config(
    page_title="Credit Risk Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────
st.markdown("""
<style>
    /* Import fonts */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

    /* Background */
    .stApp { background-color: #0f1117; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #161b27 0%, #0f1117 100%);
        border-right: 1px solid #1e2535;
    }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: #161b27;
        border: 1px solid #1e2535;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 24px rgba(0,0,0,0.3);
    }
    [data-testid="metric-container"] label { color: #6b7280 !important; font-size: 0.75rem !important; letter-spacing: 0.08em; text-transform: uppercase; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #e2e8f0 !important; font-size: 1.8rem !important; font-weight: 600; }
    [data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 0.8rem !important; }

    /* Section headers */
    .section-header {
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #4ade80;
        margin: 28px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #1e2535;
    }

    /* Page title */
    .page-title {
        font-size: 1.6rem;
        font-weight: 700;
        color: #f1f5f9;
        margin-bottom: 4px;
    }
    .page-subtitle {
        font-size: 0.85rem;
        color: #6b7280;
        margin-bottom: 24px;
    }

    /* Tables */
    .dataframe { font-family: 'DM Mono', monospace !important; font-size: 0.78rem !important; }

    /* Divider */
    hr { border-color: #1e2535 !important; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_engine():
    """Create a single SQLAlchemy engine with connection pooling, reused across requests."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        st.error("❌ DATABASE_URL environment variable is not set.")
        st.stop()
    engine = create_engine(
        db_url,
        pool_size=2,          # Keep only 2 connections open (Neon free tier safe)
        max_overflow=0,        # No extra connections beyond pool_size
        pool_pre_ping=True,    # Verify connection health before using
        pool_recycle=300,      # Recycle connections every 5 minutes
        connect_args={"connect_timeout": 10},
    )
    return engine


@st.cache_data(ttl=300)
def load_kpis():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*)                          AS total_applications,
                ROUND(AVG(target::numeric) * 100, 2) AS default_rate_pct,
                ROUND(AVG(income)::numeric, 2)    AS avg_income,
                ROUND(AVG(loan_amount)::numeric, 2) AS avg_loan_amount,
                SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) AS total_defaults
            FROM analytics.fact_loan_applications
        """))
        return result.fetchone()._asdict()


@st.cache_data(ttl=300)
def load_risk_by_education():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                education,
                COUNT(*)                              AS total_applications,
                ROUND(AVG(target::numeric) * 100, 2) AS default_rate_pct,
                ROUND(AVG(loan_amount)::numeric, 2)  AS avg_loan_amount
            FROM analytics.fact_loan_applications
            WHERE education IS NOT NULL
            GROUP BY education
            ORDER BY default_rate_pct DESC
        """), conn)
    return df


@st.cache_data(ttl=300)
def load_risk_segmentation():
    """Income quartile risk segmentation (Query 1)."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            WITH borrower_risk AS (
                SELECT
                    borrower_id,
                    income,
                    total_credit_amount,
                    total_overdue_amount,
                    previous_loan_count,
                    target,
                    NTILE(4) OVER (ORDER BY income) AS income_quartile
                FROM analytics.fact_loan_applications
            )
            SELECT
                income_quartile,
                COUNT(*)                                        AS total_applications,
                ROUND(AVG(target::numeric) * 100, 2)           AS default_rate_pct,
                ROUND(AVG(total_credit_amount)::numeric, 2)    AS avg_credit_exposure,
                ROUND(AVG(total_overdue_amount)::numeric, 2)   AS avg_overdue_amount,
                ROUND(AVG(previous_loan_count)::numeric, 2)    AS avg_previous_loans
            FROM borrower_risk
            GROUP BY income_quartile
            ORDER BY income_quartile
        """), conn)
    df["income_quartile"] = df["income_quartile"].map({
        1: "Q1 — Low Income",
        2: "Q2 — Mid-Low Income",
        3: "Q3 — Mid-High Income",
        4: "Q4 — High Income",
    })
    return df


@st.cache_data(ttl=300)
def load_top_risky_borrowers():
    """Top risky borrowers by overdue amount (Query 2)."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            WITH ranked_borrowers AS (
                SELECT
                    borrower_id,
                    application_id,
                    ROUND(loan_amount::numeric, 2)         AS loan_amount,
                    ROUND(income::numeric, 2)              AS income,
                    target,
                    ROUND(total_credit_amount::numeric, 2) AS total_credit_amount,
                    ROUND(total_overdue_amount::numeric, 2) AS total_overdue_amount,
                    RANK() OVER (
                        PARTITION BY target
                        ORDER BY total_overdue_amount DESC, total_credit_amount DESC
                    ) AS risk_rank
                FROM analytics.fact_loan_applications
            )
            SELECT *
            FROM ranked_borrowers
            WHERE risk_rank <= 10
            ORDER BY target DESC, risk_rank ASC
        """), conn)
    return df


@st.cache_data(ttl=300)
def load_payment_behavior():
    """Payment behavior analysis (Query 3)."""
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            WITH installment_summary AS (
                SELECT
                    pl.borrower_id,
                    i.previous_loan_id,
                    i.installment_amount,
                    i.payment_amount,
                    i.days_late,
                    AVG(i.days_late) OVER (PARTITION BY pl.borrower_id) AS avg_days_late_per_borrower,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.borrower_id
                        ORDER BY i.days_late DESC
                    ) AS lateness_rank
                FROM analytics.stg_installments i
                JOIN analytics.stg_previous_loans pl
                    ON i.previous_loan_id = pl.previous_loan_id
            )
            SELECT
                borrower_id,
                previous_loan_id,
                ROUND(installment_amount::numeric, 2) AS installment_amount,
                ROUND(payment_amount::numeric, 2)     AS payment_amount,
                days_late,
                ROUND(avg_days_late_per_borrower::numeric, 2) AS avg_days_late,
                lateness_rank
            FROM installment_summary
            WHERE lateness_rank <= 3
            ORDER BY days_late DESC
            LIMIT 500
        """), conn)
    return df


@st.cache_data(ttl=300)
def load_loan_distribution(gender_filter, education_filter, income_min, income_max):
    """Filtered loan amount distribution for interactive widget."""
    engine = get_engine()

    params = {
        "income_min": income_min,
        "income_max": income_max,
    }

    gender_clause = ""
    if gender_filter != "All":
        gender_clause = "AND b.gender = :gender"
        params["gender"] = gender_filter

    education_clause = ""
    if education_filter != "All":
        education_clause = "AND b.education = :education"
        params["education"] = education_filter

    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT
                f.loan_amount,
                f.target,
                f.income,
                f.occupation_type,
                f.years_employed,
                b.gender,
                b.education
            FROM analytics.fact_loan_applications f
            LEFT JOIN analytics.dim_borrowers b
                ON f.borrower_id = b.borrower_id
            WHERE f.income BETWEEN :income_min AND :income_max
            {gender_clause}
            {education_clause}
            AND f.loan_amount IS NOT NULL
            LIMIT 5000
        """), conn, params=params)

    return df


@st.cache_data(ttl=300)
def load_occupation_defaults():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                occupation_type,
                COUNT(*)                              AS total,
                ROUND(AVG(target::numeric) * 100, 2) AS default_rate_pct
            FROM analytics.fact_loan_applications
            WHERE occupation_type IS NOT NULL
            GROUP BY occupation_type
            HAVING COUNT(*) > 100
            ORDER BY default_rate_pct DESC
            LIMIT 15
        """), conn)
    return df


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Credit Risk Analytics")
    st.markdown("<hr>", unsafe_allow_html=True)

    page = st.radio(
        "Navigate",
        ["🏠 Overview", "📉 Risk Segmentation", "💳 Payment Behavior"],
        label_visibility="collapsed",
    )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("### Filters")

    gender_filter = st.selectbox("Gender", ["All", "M", "F"])

    @st.cache_data(ttl=600)
    def get_education_options():
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT DISTINCT education FROM analytics.dim_borrowers WHERE education IS NOT NULL ORDER BY education"
            ))
            return ["All"] + [r[0] for r in result]

    education_options = get_education_options()
    education_filter = st.selectbox("Education", education_options)

    income_range = st.slider(
        "Annual Income Range ($)",
        min_value=0,
        max_value=500_000,
        value=(0, 300_000),
        step=10_000,
        format="$%d",
    )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.caption("Data refreshes every 5 min · Neon PostgreSQL")


# ─────────────────────────────────────────────
# PAGE: OVERVIEW
# ─────────────────────────────────────────────
def render_overview():
    st.markdown('<div class="page-title">Credit Risk Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-subtitle">Portfolio overview · Loan applications & default analytics</div>', unsafe_allow_html=True)

    # ── KPI Cards ──
    kpis = load_kpis()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Applications", f"{kpis['total_applications']:,}")
    c2.metric("Default Rate", f"{kpis['default_rate_pct']}%", delta=None)
    c3.metric("Total Defaults", f"{kpis['total_defaults']:,}")
    c4.metric("Avg Income", f"${kpis['avg_income']:,.0f}")
    c5.metric("Avg Loan Amount", f"${kpis['avg_loan_amount']:,.0f}")

    st.markdown("<hr>", unsafe_allow_html=True)

    col_left, col_right = st.columns([1.2, 1])

    with col_left:
        st.markdown('<div class="section-header">Default Rate by Education Level</div>', unsafe_allow_html=True)
        edu_df = load_risk_by_education()
        fig = px.bar(
            edu_df,
            x="default_rate_pct",
            y="education",
            orientation="h",
            color="default_rate_pct",
            color_continuous_scale=["#1e3a5f", "#ef4444"],
            labels={"default_rate_pct": "Default Rate (%)", "education": ""},
            text="default_rate_pct",
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            coloraxis_showscale=False,
            margin=dict(l=10, r=30, t=10, b=10),
            height=340,
            yaxis=dict(tickfont=dict(size=11)),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown('<div class="section-header">Default Rate by Occupation (Top 15)</div>', unsafe_allow_html=True)
        occ_df = load_occupation_defaults()
        fig2 = px.bar(
            occ_df.sort_values("default_rate_pct"),
            x="default_rate_pct",
            y="occupation_type",
            orientation="h",
            color="default_rate_pct",
            color_continuous_scale=["#14532d", "#fbbf24"],
            labels={"default_rate_pct": "Default Rate (%)", "occupation_type": ""},
        )
        fig2.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            height=340,
            yaxis=dict(tickfont=dict(size=10)),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ── Interactive: Loan Distribution ──
    st.markdown('<div class="section-header">Loan Amount Distribution · Interactive Explorer</div>', unsafe_allow_html=True)
    st.caption(f"Showing: Gender = **{gender_filter}** · Education = **{education_filter}** · Income = **${income_range[0]:,}** – **${income_range[1]:,}**")

    dist_df = load_loan_distribution(gender_filter, education_filter, income_range[0], income_range[1])

    if dist_df.empty:
        st.warning("No data matches the selected filters.")
    else:
        fig3 = px.histogram(
            dist_df,
            x="loan_amount",
            color="target",
            barmode="overlay",
            nbins=60,
            color_discrete_map={0: "#3b82f6", 1: "#ef4444"},
            labels={"loan_amount": "Loan Amount ($)", "target": "Default"},
            opacity=0.75,
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            legend=dict(title="Default (1=Yes)", font=dict(color="#94a3b8")),
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
        )
        fig3.update_xaxes(gridcolor="#1e2535")
        fig3.update_yaxes(gridcolor="#1e2535")
        st.plotly_chart(fig3, use_container_width=True)
        st.caption(f"Showing {len(dist_df):,} records (capped at 5,000 for performance)")


# ─────────────────────────────────────────────
# PAGE: RISK SEGMENTATION
# ─────────────────────────────────────────────
def render_risk_segmentation():
    st.markdown('<div class="page-title">Risk Segmentation</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-subtitle">Income quartile analysis · Top high-risk borrowers</div>', unsafe_allow_html=True)

    seg_df = load_risk_segmentation()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">Default Rate by Income Quartile</div>', unsafe_allow_html=True)
        fig = px.bar(
            seg_df,
            x="income_quartile",
            y="default_rate_pct",
            color="default_rate_pct",
            color_continuous_scale=["#1e3a5f", "#ef4444"],
            text="default_rate_pct",
            labels={"income_quartile": "", "default_rate_pct": "Default Rate (%)"},
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=20, b=60),
            height=350,
            xaxis=dict(tickangle=-15, tickfont=dict(size=11)),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">Avg Credit Exposure vs Overdue by Quartile</div>', unsafe_allow_html=True)
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name="Avg Credit Exposure",
            x=seg_df["income_quartile"],
            y=seg_df["avg_credit_exposure"],
            marker_color="#3b82f6",
        ))
        fig2.add_trace(go.Bar(
            name="Avg Overdue Amount",
            x=seg_df["income_quartile"],
            y=seg_df["avg_overdue_amount"],
            marker_color="#ef4444",
        ))
        fig2.update_layout(
            barmode="group",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            legend=dict(font=dict(color="#94a3b8")),
            margin=dict(l=10, r=10, t=20, b=60),
            height=350,
            xaxis=dict(tickangle=-15, tickfont=dict(size=11)),
            yaxis=dict(gridcolor="#1e2535"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-header">Quartile Summary Table</div>', unsafe_allow_html=True)
    display_df = seg_df.rename(columns={
        "income_quartile": "Income Segment",
        "total_applications": "Applications",
        "default_rate_pct": "Default Rate (%)",
        "avg_credit_exposure": "Avg Credit ($)",
        "avg_overdue_amount": "Avg Overdue ($)",
        "avg_previous_loans": "Avg Prev. Loans",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Top 10 Highest-Risk Borrowers (by Default Status)</div>', unsafe_allow_html=True)

    risky_df = load_top_risky_borrowers()
    defaulters = risky_df[risky_df["target"] == 1].copy()
    non_defaulters = risky_df[risky_df["target"] == 0].copy()

    tab1, tab2 = st.tabs(["🔴 Defaulted (target=1)", "🟢 Non-Defaulted (target=0)"])

    def style_df(df):
        return df.drop(columns=["target"]).rename(columns={
            "borrower_id": "Borrower ID",
            "application_id": "App ID",
            "loan_amount": "Loan ($)",
            "income": "Income ($)",
            "total_credit_amount": "Total Credit ($)",
            "total_overdue_amount": "Overdue ($)",
            "risk_rank": "Risk Rank",
        })

    with tab1:
        st.dataframe(style_df(defaulters), use_container_width=True, hide_index=True)
    with tab2:
        st.dataframe(style_df(non_defaulters), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────
# PAGE: PAYMENT BEHAVIOR
# ─────────────────────────────────────────────
def render_payment_behavior():
    st.markdown('<div class="page-title">Payment Behavior Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-subtitle">Installment lateness patterns · Top late payments per borrower</div>', unsafe_allow_html=True)

    df = load_payment_behavior()

    if df.empty:
        st.warning("No payment behavior data available.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">Days Late Distribution</div>', unsafe_allow_html=True)
        fig = px.histogram(
            df[df["days_late"] > 0],
            x="days_late",
            nbins=50,
            color_discrete_sequence=["#f59e0b"],
            labels={"days_late": "Days Late", "count": "Count"},
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            xaxis=dict(gridcolor="#1e2535"),
            yaxis=dict(gridcolor="#1e2535"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">Installment Amount vs Payment Amount</div>', unsafe_allow_html=True)
        fig2 = px.scatter(
            df.sample(min(300, len(df))),
            x="installment_amount",
            y="payment_amount",
            color="days_late",
            color_continuous_scale=["#14532d", "#fbbf24", "#ef4444"],
            labels={
                "installment_amount": "Installment Due ($)",
                "payment_amount": "Actual Payment ($)",
                "days_late": "Days Late",
            },
            opacity=0.7,
        )
        fig2.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#94a3b8",
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            xaxis=dict(gridcolor="#1e2535"),
            yaxis=dict(gridcolor="#1e2535"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-header">Avg Days Late per Borrower · Top Late Payments</div>', unsafe_allow_html=True)

    # Avg days late per borrower (aggregated)
    avg_late = (
        df.groupby("borrower_id")["avg_days_late"]
        .first()
        .reset_index()
        .sort_values("avg_days_late", ascending=False)
        .head(20)
    )
    avg_late["borrower_id"] = avg_late["borrower_id"].astype(str)

    fig3 = px.bar(
        avg_late,
        x="borrower_id",
        y="avg_days_late",
        color="avg_days_late",
        color_continuous_scale=["#1e3a5f", "#ef4444"],
        labels={"borrower_id": "Borrower ID", "avg_days_late": "Avg Days Late"},
    )
    fig3.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#94a3b8",
        coloraxis_showscale=False,
        margin=dict(l=10, r=10, t=10, b=40),
        height=300,
        xaxis=dict(tickangle=-45, tickfont=dict(size=9), gridcolor="#1e2535"),
        yaxis=dict(gridcolor="#1e2535"),
    )
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<div class="section-header">Raw Payment Records</div>', unsafe_allow_html=True)
    st.dataframe(
        df.rename(columns={
            "borrower_id": "Borrower ID",
            "previous_loan_id": "Loan ID",
            "installment_amount": "Installment ($)",
            "payment_amount": "Payment ($)",
            "days_late": "Days Late",
            "avg_days_late": "Avg Days Late",
            "lateness_rank": "Rank",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ─────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────
if page == "🏠 Overview":
    render_overview()
elif page == "📉 Risk Segmentation":
    render_risk_segmentation()
elif page == "💳 Payment Behavior":
    render_payment_behavior()
