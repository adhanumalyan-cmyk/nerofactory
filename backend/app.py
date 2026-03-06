import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os
import requests  # For Ollama

# Add the backend folder to path so we can import connectors
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from connectors import get_connectors

# Try importing optional ML libraries
try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    st.warning("scikit-learn not installed. Financial audit will be limited.")

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    st.warning("Prophet not installed. Inventory forecasting will be disabled.")

st.set_page_config(page_title="Factory Intelligence", layout="wide")
st.title("🏭 Factory Intelligence Dashboard")

# Initialize connectors
if 'connectors' not in st.session_state:
    with st.spinner("Connecting to factory systems..."):
        mes, erp, mqtt = get_connectors()
        st.session_state.connectors = (mes, erp, mqtt)
        st.success("✅ Connected to MES, ERP, and MQTT")
else:
    mes, erp, mqtt = st.session_state.connectors

# Sidebar
st.sidebar.header("Controls")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Load data
@st.cache_data(ttl=30)
def load_data():
    prod = mes.fetch_production_data()
    inv = erp.fetch_inventory_data()
    emp = erp.fetch_employee_data()      # Now from DuckDB
    fin = erp.fetch_financial_data()
    sensor = mqtt.fetch_production_data()
    return prod, inv, emp, fin, sensor

prod, inv, emp, fin, sensor = load_data()

# ---------- Alerting System ----------
def check_alerts(prod, inv):
    alerts = []

    # 1. Machine downtime alert (downtime >= 60 minutes in the most recent day)
    if not prod.empty:
        latest_date = prod['date'].max()
        recent_prod = prod[prod['date'] >= latest_date - pd.Timedelta(days=1)]
        long_downtime = recent_prod[recent_prod['downtime_minutes'] >= 60]
        if not long_downtime.empty:
            for _, row in long_downtime.iterrows():
                alerts.append(f"🔧 Machine {row['machine_id']} down for {row['downtime_minutes']} minutes on {row['date'].date()}")

    # 2. Supply alerts (stock < 3 days of average usage)
    if not inv.empty:
        avg_usage = inv.groupby('material_id')['daily_usage'].mean().to_dict()
        latest_stock = inv.groupby('material_id').last()['stock_quantity'].to_dict()
        for material in avg_usage:
            if material in latest_stock:
                days_left = latest_stock[material] / avg_usage[material] if avg_usage[material] > 0 else 999
                if days_left < 3:
                    alerts.append(f"📦 Low stock: {material} – only {days_left:.1f} days left (current stock: {latest_stock[material]})")

    return alerts

alerts = check_alerts(prod, inv)

# Display alerts in sidebar (always visible)
st.sidebar.markdown("---")
st.sidebar.markdown("### 🚨 Alerts")
if alerts:
    for alert in alerts:
        st.sidebar.warning(alert)
    for alert in alerts[-3:]:  # show last 3 as toasts
        st.toast(alert)
else:
    st.sidebar.info("✅ No critical alerts at this time. All systems normal.")

# ---------- Weekly Aggregates for Auditor ----------
if not prod.empty:
    prod['date'] = pd.to_datetime(prod['date'])
    prod['week'] = prod['date'].dt.to_period('W').apply(lambda r: r.start_time)

    weekly = prod.groupby('week').agg({
        'output_units': 'sum',
        'revenue': 'sum',
        'total_cost': 'sum',
        'profit': 'sum',
        'material_cost': 'sum',
        'labor_cost': 'sum',
        'downtime_cost': 'sum',
        'downtime_minutes': 'sum'
    }).reset_index()
    weekly['profit_margin'] = (weekly['profit'] / weekly['revenue'] * 100).fillna(0)

    # Get last two weeks
    last_week = weekly.iloc[-1] if len(weekly) > 0 else None
    prev_week = weekly.iloc[-2] if len(weekly) > 1 else None
else:
    weekly = pd.DataFrame()
    last_week = None
    prev_week = None

# ---------- Financial Audit Summary ----------
if not fin.empty and 'flagged' in fin.columns:
    recent_flags = fin[fin['flagged']].tail(10)
    flag_summary = recent_flags[['date', 'amount', 'type', 'vendor']].to_string(index=False)
else:
    flag_summary = "No flagged transactions recently."

# Display tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "🔍 Root Cause", "📦 Inventory", "👥 Workforce", "💰 Audit"])

with tab1:
    st.header("Production Overview")
    st.markdown("Key production metrics and recent data to quickly assess factory performance.")
    if not prod.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Production Records", len(prod))
        with col2:
            total_output = prod['output_units'].sum()
            st.metric("Total Output", f"{total_output:,} units")
        with col3:
            avg_downtime = prod['downtime_minutes'].mean()
            st.metric("Avg Downtime per Record", f"{avg_downtime:.1f} min")

        st.subheader("Recent Production Data")
        st.dataframe(prod.head(10))
    else:
        st.warning("No production data")

with tab2:
    st.header("🔍 Root Cause Analysis")
    st.markdown("This chart compares the selected day's productivity to the previous week's average. If a drop is detected, the system highlights which machines had increased downtime and checks absenteeism.")

    if not prod.empty:
        # Prepare daily productivity
        daily_prod = prod.groupby('date').agg(
            output=('output_units', 'sum'),
            target=('target_units', 'sum'),
            downtime=('downtime_minutes', 'sum'),
            employees=('employees_present', 'mean')
        ).reset_index()
        daily_prod['productivity'] = daily_prod['output'] / daily_prod['target']

        # Date picker
        dates_list = daily_prod['date'].dt.date.unique()
        selected_date = st.selectbox("Select a date to analyze", dates_list)

        day_data = daily_prod[daily_prod['date'].dt.date == selected_date]
        if not day_data.empty:
            current_prod = day_data['productivity'].values[0]
            # Compare to previous week average
            prev_week = daily_prod[daily_prod['date'] < pd.Timestamp(selected_date)].tail(7)
            avg_prev = prev_week['productivity'].mean() if not prev_week.empty else current_prod

            delta = current_prod - avg_prev
            st.metric("Productivity", f"{current_prod:.2%}", delta=f"{delta:.2%} vs last week")

            # If drop detected
            if current_prod < avg_prev * 0.9:  # 10% drop
                st.subheader("🔧 Contributing Factors")

                # Machine downtime change
                day_machines = prod[prod['date'].dt.date == selected_date].groupby('machine_id')['downtime_minutes'].sum()
                week_machines = prod[prod['date'] < pd.Timestamp(selected_date)].groupby('machine_id')['downtime_minutes'].mean() * 7
                downtime_change = (day_machines - week_machines).dropna().sort_values(ascending=False)

                if not downtime_change.empty:
                    st.write("**Machine Downtime Increase (minutes):**")
                    st.bar_chart(downtime_change)

                # Absenteeism
                if not emp.empty:
                    day_emp = emp[emp['date'].dt.date == selected_date]
                    week_emp = emp[emp['date'] < pd.Timestamp(selected_date)]
                    day_absent = day_emp[day_emp['hours_worked'] == 0].shape[0]
                    week_absent_avg = week_emp[week_emp['hours_worked'] == 0].shape[0] / 7 if len(week_emp) > 0 else 0
                    delta_absent = day_absent - week_absent_avg
                    st.metric("Absenteeism (day)", day_absent, delta=f"{delta_absent:+.1f} vs avg week")
                else:
                    st.info("No employee data for absenteeism analysis")
            else:
                st.success("Productivity is normal.")
    else:
        st.warning("No production data for root cause analysis")

with tab3:
    st.header("📦 Inventory Forecast")
    st.markdown("Forecast future material usage using Prophet. The reorder alert triggers when stock is predicted to run low before new supply arrives.")

    if not inv.empty:
        if PROPHET_AVAILABLE:
            material = st.selectbox("Select material", inv['material_id'].unique())
            material_data = inv[inv['material_id'] == material].copy()
            material_data = material_data.rename(columns={'date': 'ds', 'daily_usage': 'y'})

            if len(material_data) > 30:
                with st.spinner("Training forecast model..."):
                    model = Prophet()
                    model.fit(material_data[['ds', 'y']])
                    future = model.make_future_dataframe(periods=30)
                    forecast = model.predict(future)

                st.subheader("Forecast")
                fig = model.plot(forecast)
                st.pyplot(fig)

                # Reorder alert
                current_stock = material_data['stock_quantity'].iloc[-1]
                reorder_point = material_data['reorder_point'].iloc[-1]
                predicted_usage = forecast['yhat'].iloc[-30:].sum()
                safety = predicted_usage * 0.2
                if current_stock < reorder_point + safety:
                    st.error(f"⚠️ **Reorder Soon!** Current stock: {current_stock} | Reorder point: {reorder_point} | Predicted 30d usage: {predicted_usage:.0f}")
                else:
                    st.success("✅ Stock level adequate.")
            else:
                st.warning("Not enough historical data for this material (need >30 days).")
        else:
            st.warning("Prophet not installed. Install with: pip install prophet")
            st.dataframe(inv.head(10))
    else:
        st.warning("No inventory data")

with tab4:
    st.header("👥 Workforce Analytics")
    st.markdown("Track employee attendance, output, and flag potential credit theft (low output despite high attendance). Teams are compared to identify outliers.")

    if not emp.empty:
        # Aggregate per employee
        emp_stats = emp.groupby('employee_id').agg(
            days_worked=('hours_worked', lambda x: (x > 0).sum()),
            total_hours=('hours_worked', 'sum'),
            total_output=('output_contributed', 'sum')
        ).reset_index()

        total_days = emp['date'].nunique()
        emp_stats['attendance_rate'] = emp_stats['days_worked'] / total_days
        emp_stats['avg_daily_output'] = emp_stats['total_output'] / emp_stats['days_worked']

        # Team mapping (adjust based on your data)
        team_map = {101: 'A', 102: 'A', 103: 'B', 104: 'B', 105: 'A',
                    106: 'B', 107: 'C', 108: 'C', 109: 'C', 110: 'A'}
        emp_stats['team'] = emp_stats['employee_id'].map(team_map).fillna('Unknown')

        # Team averages
        team_avg = emp_stats.groupby('team')['avg_daily_output'].mean().to_dict()
        emp_stats['team_avg_output'] = emp_stats['team'].map(team_avg)
        emp_stats['output_vs_team'] = emp_stats['avg_daily_output'] - emp_stats['team_avg_output']

        # Flag suspicious: low output despite high attendance
        emp_stats['flag'] = (emp_stats['output_vs_team'] < -5) & (emp_stats['attendance_rate'] > 0.8)

        st.subheader("Employee Performance Summary")
        st.dataframe(emp_stats[['employee_id', 'team', 'attendance_rate', 'avg_daily_output', 'output_vs_team', 'flag']])

        # Attendance trend
        daily_att = emp.groupby('date').apply(lambda x: (x['hours_worked'] > 0).mean()).reset_index(name='rate')
        fig = px.line(daily_att, x='date', y='rate', title='Daily Attendance Rate')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No employee data")

with tab5:
    st.header("💰 Financial Audit")
    st.markdown("Detect anomalies in financial transactions using Isolation Forest and flag duplicate payments. Helps identify potential fraud or errors.")

    if not fin.empty:
        if SKLEARN_AVAILABLE:
            # Feature: log amount
            fin['amount_log'] = np.log1p(fin['amount'])
            model_if = IsolationForest(contamination=0.1, random_state=42)
            fin['anomaly'] = model_if.fit_predict(fin[['amount_log']])  # -1 = anomaly

            # Rule: duplicate amounts on same day
            fin['duplicate'] = fin.duplicated(subset=['date', 'amount'], keep=False)

            fin['flagged'] = (fin['anomaly'] == -1) | fin['duplicate']

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Transactions", len(fin))
            with col2:
                st.metric("Flagged Transactions", fin['flagged'].sum())

            st.subheader("Flagged Transactions")
            st.dataframe(fin[fin['flagged']][['date', 'amount', 'type', 'vendor', 'anomaly', 'duplicate']])

            # Distribution plot
            fig = px.histogram(fin, x='amount', color=fin['anomaly'].map({1: 'Normal', -1: 'Anomaly'}),
                               title='Transaction Amount Distribution')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("scikit-learn not installed. Install with: pip install scikit-learn")
            st.dataframe(fin.head(10))
    else:
        st.warning("No financial data")

st.sidebar.markdown("---")
st.sidebar.write("Live sensor messages:", len(sensor))

# ---------- AI Assistant (Local Ollama) ----------
st.sidebar.markdown("---")
st.sidebar.header("🤖 Jarvis (AI Assistant)")

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3.2:latest"  # Change to your preferred model (e.g., "mistral", "llama3.2:3b")

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Display chat history
st.sidebar.markdown("### 💬 Conversation")
for msg in st.session_state.chat_history:
    if msg['user']:
        st.sidebar.markdown(f"**👤 You:** {msg['user']}")
    st.sidebar.markdown(f"**🤖 Jarvis:** {msg['bot']}")
    st.sidebar.markdown("---")

# ---------- Daily Briefing ----------
st.sidebar.markdown("### 🌅 Daily Briefing")
if 'briefing' not in st.session_state:
    st.session_state.briefing = None
if 'briefing_date' not in st.session_state:
    st.session_state.briefing_date = None

if not prod.empty:
    latest_date = prod['date'].max().date()
    if latest_date == datetime.now().date():
        briefing_date = (pd.Timestamp(latest_date) - pd.Timedelta(days=1)).date()
    else:
        briefing_date = latest_date

    if st.sidebar.button("🔄 Refresh Briefing") or st.session_state.briefing_date != briefing_date:
        with st.sidebar.spinner("Generating daily briefing..."):
            prod_yest = prod[prod['date'].dt.date == briefing_date]
            inv_yest = inv[inv['date'].dt.date == briefing_date] if not inv.empty else pd.DataFrame()
            emp_yest = emp[emp['date'].dt.date == briefing_date] if not emp.empty else pd.DataFrame()
            fin_yest = fin[fin['date'].dt.date == briefing_date] if not fin.empty else pd.DataFrame()

            total_output = prod_yest['output_units'].sum() if not prod_yest.empty else 0
            total_target = prod_yest['target_units'].sum() if not prod_yest.empty else 1
            productivity = f"{total_output / total_target:.2%}" if total_target > 0 else "N/A"
            downtime_by_machine = prod_yest.groupby('machine_id')['downtime_minutes'].sum().to_dict()
            low_stock_alerts = inv_yest[inv_yest['stock_quantity'] < inv_yest['reorder_point']]['material_id'].tolist() if not inv_yest.empty else []
            attendance_rate = (emp_yest['hours_worked'] > 0).mean() if not emp_yest.empty else 0
            flagged_count = fin_yest[fin_yest['flagged']].shape[0] if not fin_yest.empty and 'flagged' in fin_yest.columns else 0

            prompt = f"""
            You are a factory assistant. Provide a concise morning briefing for {briefing_date}.
            Include:
            - Total output: {total_output} units (productivity {productivity})
            - Machine downtime: {downtime_by_machine}
            - Inventory alerts: {low_stock_alerts}
            - Attendance rate: {attendance_rate:.2%}
            - Financial anomalies: {flagged_count} flagged transactions
            Keep it professional and brief.
            """
            try:
                response = requests.post(OLLAMA_URL, json={"model": MODEL_NAME, "prompt": prompt, "stream": False}, timeout=30)
                if response.status_code == 200:
                    st.session_state.briefing = response.json()["response"]
                    st.session_state.briefing_date = briefing_date
                else:
                    st.session_state.briefing = f"Error {response.status_code}"
                    st.session_state.briefing_date = briefing_date
            except Exception as e:
                st.session_state.briefing = f"Error: {e}"
                st.session_state.briefing_date = briefing_date

if st.session_state.briefing:
    st.sidebar.info(st.session_state.briefing)
else:
    st.sidebar.info("Click 'Refresh Briefing' to generate today's summary.")

# User input
user_query = st.sidebar.text_input("Ask Jarvis (e.g., 'How many machines do I have?'):")
if user_query:
    # Build conversation history (last 5 exchanges)
    conversation_history = ""
    user_msgs = [m for m in st.session_state.chat_history if m['user']]
    if user_msgs:
        for msg in user_msgs[-5:]:
            conversation_history += f"User: {msg['user']}\nAssistant: {msg['bot']}\n"
    else:
        conversation_history = "This is the start of the conversation."

    # Build context (same as before)
    prod_context = f"Total production records: {len(prod)}. Total output: {prod['output_units'].sum():,.0f} units."
    inv_context = f"Current inventory total: {inv['stock_quantity'].sum() if not inv.empty else 0} units. Low stock alerts: {inv[inv['stock_quantity'] < inv['reorder_point']].shape[0] if not inv.empty else 0}."
    emp_context = f"Total employees: {emp['employee_id'].nunique() if not emp.empty else 0}. Average attendance: {emp['hours_worked'].mean() if not emp.empty else 0:.1f} hours."

    financial_context = ""
    if last_week is not None and 'profit' in last_week.index:
        if prev_week is not None and 'profit' in prev_week.index:
            profit_change = last_week['profit'] - prev_week['profit']
        else:
            profit_change = 0
        financial_context = f"""
        Weekly Financial Summary (week starting {last_week['week'].date()}):
        - Revenue: ₹{last_week['revenue']:,.0f}
        - Total Cost: ₹{last_week['total_cost']:,.0f}
        - Profit: ₹{last_week['profit']:,.0f}
        - Profit Margin: {last_week['profit_margin']:.1f}%
        - Cost Breakdown:
          * Material: ₹{last_week['material_cost']:,.0f}
          * Labour: ₹{last_week['labor_cost']:,.0f}
          * Downtime: ₹{last_week['downtime_cost']:,.0f}
        """
        if prev_week is not None and 'profit' in prev_week.index:
            financial_context += f"\n        Profit change from previous week: ₹{profit_change:+,.0f}"

    flag_context = f"\n**Recent Flagged Transactions:**\n{flag_summary}"

    # Machine summary
    if not prod.empty:
        total_machines = prod['machine_id'].nunique()
        top_downtime_machines = prod.groupby('machine_id')['downtime_minutes'].sum().nlargest(3).to_dict()
        avg_downtime_per_machine = prod.groupby('machine_id')['downtime_minutes'].mean().to_dict()
        machine_summary = f"Total machines: {total_machines}. Machines with highest total downtime: {top_downtime_machines}. Average downtime per machine: {avg_downtime_per_machine}."
    else:
        machine_summary = "No machine data available."

    # Employee leave summary
    if not emp.empty:
        absent_days = emp[emp['hours_worked'] == 0].groupby('employee_id').size()
        top_absentees = absent_days.nlargest(3).to_dict()
        attendance_rates = emp.groupby('employee_id')['hours_worked'].apply(lambda x: (x > 0).mean()).to_dict()
        employee_summary = f"Top employees by absenteeism (days absent): {top_absentees}. Attendance rates: {attendance_rates}."
    else:
        employee_summary = "No employee data available."

    full_context = f"""
    Factory Data Summary:
    {prod_context}
    {inv_context}
    {emp_context}
    {machine_summary}
    {employee_summary}
    {financial_context}
    {flag_context}
    """

    # System prompt
    system_message = (
        "You are Jarvis, a friendly and helpful AI assistant for the factory manager. "
        "You have access to the factory data summary above. Answer questions **only** using the provided data. "
        "If the answer cannot be derived from the data, politely say so. "
        "Be concise, clear, and maintain a natural dialogue. "
        "Do not invent numbers or facts."
    )

    prompt = f"""{system_message}

    Previous conversation:
    {conversation_history}

    Current factory data:
    {full_context}

    User: {user_query}
    Assistant:"""

    try:
        response = requests.post(OLLAMA_URL, json={"model": MODEL_NAME, "prompt": prompt, "stream": False}, timeout=30)
        if response.status_code == 200:
            bot_reply = response.json()["response"]
            st.session_state.chat_history.append({"user": user_query, "bot": bot_reply})
            st.sidebar.markdown(f"**👤 You:** {user_query}")
            st.sidebar.markdown(f"**🤖 Jarvis:** {bot_reply}")
        else:
            st.sidebar.error(f"Error {response.status_code}: {response.text}")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

# ---------- Optional Text-to-Speech for Last Reply ----------
if st.session_state.chat_history and st.session_state.chat_history[-1]['bot']:
    last_reply = st.session_state.chat_history[-1]["bot"]
    if st.sidebar.button("🔊 Speak Last Reply"):
        try:
            import pyttsx3
            engine = pyttsx3.init()
            engine.say(last_reply)
            engine.runAndWait()
        except ImportError:
            st.sidebar.error("pyttsx3 not installed. Run: pip install pyttsx3")