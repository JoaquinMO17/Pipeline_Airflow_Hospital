import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# Postgres connection
engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

st.title("HDHI Clinical Insights Dashboard")
st.markdown("""
This dashboard provides insights into cardiovascular hospitalizations,
risk factors, and resource utilization.  
It supports **health planning**, **public policy**, and **clinical decision-making**.
""")

# Load aggregated data
df_dash = pd.read_sql("SELECT * FROM analytics_hdhi_dashboard ORDER BY month", engine)
df_clean = pd.read_sql("SELECT * FROM analytics_hdhi_clean", engine)

# --- Trends ---
st.header("1. Admission Trends (Real-world problem: disease burden & seasonality)")
fig1 = px.line(df_dash, x='month', y='total_admissions', title='Monthly Admissions')
st.plotly_chart(fig1)

# Emergency vs OPD
fig2 = px.area(
    df_dash,
    x='month',
    y=['emergency_cases','opd_cases'],
    title="Emergency vs OPD Visits"
)
st.plotly_chart(fig2)

# --- Risk Scores ---
st.header("2. Population Risk Patterns (Real-world problem: lifestyle-driven cardiac disease)")
fig3 = px.histogram(
    df_clean,
    x='lifestyle_risk_score',
    nbins=7,
    title='Distribution of Lifestyle Risk Scores'
)
st.plotly_chart(fig3)

# --- ICU burden ---
st.header("3. Hospital Resource Pressure (Real-world problem: ICU scarcity)")
fig4 = px.line(df_dash, x='month', y='count_long_icu', title='Long ICU Stays Over Time')
st.plotly_chart(fig4)

# --- Metabolic/Cardiac Risk ---
st.header("4. Cardiac & Metabolic Stress Indicators")
fig5 = px.line(df_dash, x='month', y='count_cardiac_distress', title='Cardiac Distress Trend')
st.plotly_chart(fig5)

fig6 = px.line(df_dash, x='month', y='count_metabolic_risk', title='Metabolic Risk Trend')
st.plotly_chart(fig6)

# --- Diagnoses breakdown ---
st.header("5. Clinical Diagnoses Breakdown")
diagnosis_cols = [
    'acs','stemi','heart_failure','hfref','hfnef','cva_infarct','cva_bleed','aki','shock'
]

diag_counts = df_clean[diagnosis_cols].sum().reset_index()
diag_counts.columns = ["diagnosis","count"]

fig7 = px.bar(diag_counts, x="diagnosis", y="count", title="Common Diagnoses Frequency")
st.plotly_chart(fig7)
