import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# ======================
# CONFIG
# ======================
st.set_page_config(layout="wide")

st.title("🚗 Dashboard Crash Statistic")
st.caption("NHTSA : National Highway Traffic Safety Administration")

# ======================
# CONNECTION
# ======================
engine = create_engine("postgresql://admin:admin@postgres:5432/crashdb")

df = pd.read_sql("SELECT * FROM crash", engine)

# ======================
# FILTER
# ======================
years = sorted(df["year"].dropna().unique())
selected_year = st.selectbox("Select Year", years)

df = df[df["year"] == selected_year]

st.markdown(f"### 📅 Data Tahun: {selected_year}")

# ======================
# METRICS
# ======================
col1, col2 = st.columns(2)

col1.metric("Total Crash", len(df))
col2.metric("Unique Cases", df["case_id"].nunique())

# ======================
# 1. MONTHLY CRASH
# ======================
st.subheader("📊 Monthly Crash Count")

monthly = df.groupby("month").size().sort_index()

st.bar_chart(monthly)

st.write(
    f"📌 Insight: Puncak crash terjadi pada bulan {monthly.idxmax()} dengan total {monthly.max()} kejadian."
)

# ======================
# 2. COLLISION TYPE
# ======================
st.subheader("🥧 Collision Type Distribution")

collision = df["collision_type"].value_counts()

fig1, ax1 = plt.subplots()
collision.head(5).plot.pie(autopct="%1.1f%%", ax=ax1)
ax1.set_ylabel("")

st.pyplot(fig1)

st.write(
    f"📌 Insight: Tipe tabrakan paling dominan adalah '{collision.idxmax()}' dengan proporsi terbesar."
)

# ======================
# 3. HARM EVENT
# ======================
st.subheader("🥧 Harm Event Distribution")

harm = df["harm_event"].value_counts()

fig2, ax2 = plt.subplots()
harm.head(5).plot.pie(autopct="%1.1f%%", ax=ax2)
ax2.set_ylabel("")

st.pyplot(fig2)

st.write(
    f"📌 Insight: Harm event paling sering adalah '{harm.idxmax()}'."
)

# ======================
# 4. WEATHER x HARM EVENT
# ======================
st.subheader("🌦️ Weather vs Harm Event")

pivot1 = pd.crosstab(df["weather"], df["harm_event"])

st.dataframe(pivot1)

st.write(
    "📌 Insight: Kondisi cuaca tertentu menunjukkan pola kejadian harm event yang berbeda."
)

# ======================
# 5. WEATHER x COLLISION
# ======================
st.subheader("🌦️ Weather vs Collision Type")

pivot2 = pd.crosstab(df["weather"], df["collision_type"])

st.dataframe(pivot2)

st.write(
    "📌 Insight: Tipe tabrakan memiliki distribusi yang berbeda tergantung kondisi cuaca."
)

# ======================
# 6. RAW DATA
# ======================
st.subheader("📋 Raw Data")

st.dataframe(df)