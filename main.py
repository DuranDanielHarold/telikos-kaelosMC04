import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
from typing import List
import io
import xlsxwriter
import subprocess
from groq import Groq
import os
import sys
import requests


## --- LARK HELPERS (UPDATED — NO FILE UPLOAD REQUIRED) ---

LARK_WEBHOOK = "https://open.larksuite.com/open-apis/bot/v2/hook/23d1a34e-801f-4b6f-a78a-4298189e68bb"

def post_text_to_lark(webhook: str, text: str) -> bool:
    """Send plain text to Lark."""
    try:
        payload = {"msg_type": "text", "content": {"text": text}}
        r = requests.post(webhook, json=payload, timeout=10)
        return r.ok
    except Exception as e:
        st.warning(f"⚠️ Lark text post failed: {e}")
        return False


def post_formatted_to_lark(webhook: str, title: str, lines: list[str]) -> bool:
    """
    Send a formatted POST message to Lark.
    Each element in lines becomes its own line.
    """
    content = {
        "post": {
            "en_us": {
                "title": title,
                "content": [
                    [{"tag": "text", "text": line}] for line in lines
                ]
            }
        }
    }
    payload = {"msg_type": "post", "content": content}

    try:
        r = requests.post(webhook, json=payload, timeout=10)
        return r.ok
    except Exception as e:
        st.warning(f"⚠️ Lark formatted post failed: {e}")
        return False


# --- GROQ AI HELPERS ---
def get_groq_client():
    """Initialize Groq client with API key from secrets or environment"""
    try:
        api_key = st.secrets.get("GROQ_API_KEY", None)
        if not api_key:
            api_key = os.getenv("GROQ_API_KEY")
        if api_key:
            return Groq(api_key=api_key)
        return None
    except:
        return None

def generate_ai_analysis(data_summary, mode="normal"):
    """Generate AI analysis using Groq API"""
    client = get_groq_client()
    if client is None:
        return "⚠️ Groq API key not configured. Please add GROQ_API_KEY to your secrets or environment variables."
    try:
        if mode == "hsbc":
            data_text = f"""
            HSBC Mode Call Center Data Analysis:

            Total Collectors: {len(data_summary)}

            Top 3 Performers by Talk Time:
            {data_summary.nlargest(3, 'Total Talk Time')[['Collector', 'Total Calls', 'Total Talk Time (HH:MM:SS)', 'Average Daily Talk Time (HH:MM:SS)']].to_string(index=False)}

            Bottom 3 Performers by Talk Time:
            {data_summary.nsmallest(3, 'Total Talk Time')[['Collector', 'Total Calls', 'Total Talk Time (HH:MM:SS)', 'Average Daily Talk Time (HH:MM:SS)']].to_string(index=False)}

            Overall Statistics:
            - Average Total Calls per Collector: {data_summary['Total Calls'].mean():.2f}
            - Total Calls Across All Collectors: {data_summary['Total Calls'].sum()}
            - Average Total Talk Time: {pd.to_timedelta(data_summary['Total Talk Time'].mean()).total_seconds() / 3600:.2f} hours
            """
        else:
            data_text = f"""
            Call Center Performance Data Analysis:

            Total Collectors: {len(data_summary)}

            Top 3 Performers by Talk Time:
            {data_summary.nlargest(3, 'Total Talk Time')[['Collector', 'Total Calls', 'Total Talk Time (HH:MM:SS)', 'Average Daily Talk Time (HH:MM:SS)']].to_string(index=False)}

            Bottom 3 Performers by Talk Time:
            {data_summary.nsmallest(3, 'Total Talk Time')[['Collector', 'Total Calls', 'Total Talk Time (HH:MM:SS)', 'Average Daily Talk Time (HH:MM:SS)']].to_string(index=False)}

            Overall Statistics:
            - Average Total Calls per Collector: {data_summary['Total Calls'].mean():.2f}
            - Total Calls Across All Collectors: {data_summary['Total Calls'].sum()}
            - Average Total Talk Time: {pd.to_timedelta(data_summary['Total Talk Time'].mean()).total_seconds() / 3600:.2f} hours
            """
        prompt = f"""You are a friendly, encouraging, and emoji-loving call center performance analyst! 🎉😊
Please analyze the following data and provide:

1. **Executive Summary**: Give a warm, positive overview of the team's performance. Use emojis!
2. **Key Insights**: Share 3-4 interesting or surprising findings, each with a fun emoji.
3. **Performance Analysis**: Highlight the top and bottom performers with a supportive and constructive tone. Use medals 🥇🥈🥉 and encouragement for improvement!
4. **Potential Issues**: Point out any possible challenges, but keep it positive and solution-focused. Use warning or thinking emojis.
5. **Recommendations**: Offer actionable, upbeat suggestions for the team to do even better. Use lightbulb 💡, rocket 🚀, or thumbs up 👍 emojis.

Data:
{data_text}

Make your analysis lively, supportive, and easy to read. End with a motivational message for the team! 🚀😃
"""
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are a friendly, emoji-using call center performance analyst. Always be supportive, positive, and use emojis to make your analysis fun and engaging!"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.7,
            max_tokens=2000
        )
        return chat_completion.choices[0].message.content
    except Exception as e:
        return f"❌ Error generating AI analysis: {str(e)}\n\nPlease check your Groq API key and internet connection."

def generate_detailed_report_analysis(call_counts, total_calls, total_talk, avg_daily):
    """Generate AI analysis for detailed report mode with accurate call type data"""
    client = get_groq_client()
    if client is None:
        return "⚠️ Groq API key not configured."
    try:
        # Format the call counts data more clearly
        call_type_summary = "Call Type Distribution by Agent:\n"
        for _, row in call_counts.iterrows():
            collector = row['Collector']
            predictive = row.get('Predictive Call', 0)
            outgoing = row.get('Outgoing Call', 0)
            incoming = row.get('Incoming Call', 0)
            total = predictive + outgoing + incoming
            call_type_summary += f"  • {collector}: Predictive={predictive}, Outgoing={outgoing}, Incoming={incoming}, Total={total}\n"
        
        data_text = f"""
        Detailed Call Center Report Analysis:

        {call_type_summary}

        Total Calls per Agent:
        {total_calls.to_string(index=False)}

        Top 5 Agents by Talk Time:
        {total_talk.nlargest(5, 'Total Talk Time (sec)')[['Collector', 'Total Talk Time (HH:MM:SS)']].to_string(index=False)}

        Average Daily Talk Time Summary:
        {avg_daily.describe()['Avg Daily Talk Time (sec)'].to_string()}
        
        IMPORTANT CONTEXT:
        - "Predictive Call" = Automated dialer calls (system-initiated)
        - "Outgoing Call" = Manual outbound calls (agent-initiated)
        - "Incoming Call" = Inbound calls received from customers
        """
        
        prompt = f"""You are a friendly, emoji-using call center operations expert! 😃📞
Please analyze this detailed call center report and provide:

1. **Call Type Analysis**: 
   - Analyze the distribution of Predictive (automated), Outgoing (manual outbound), and Incoming (inbound) calls
   - Identify which agents handle more automated vs manual calls
   - Comment on the balance between call types

2. **Agent Performance Patterns**: 
   - Spot trends and outliers in call volume and talk time
   - Celebrate top performers with medals and positive emojis 🥇🥈🥉

3. **Workload Balance**: 
   - Assess if work is distributed evenly across the team
   - Encourage teamwork with group or handshake emojis 🤝👥

4. **Efficiency Opportunities**: 
   - Suggest areas for improvement using lightbulb 💡 or rocket 🚀 emojis
   - Focus on realistic, actionable insights

5. **Strategic Recommendations**: 
   - Give specific, upbeat actions for management
   - Use thumbs up 👍 and star ⭐ emojis

Data:
{data_text}

CRITICAL: Base your analysis ONLY on the exact numbers shown in the data above. Do not make assumptions about call types that aren't explicitly listed.

Keep your analysis lively, supportive, and full of emojis. End with a cheerful note for the team! 🎉
"""
        
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": """You are a friendly, emoji-using call center operations expert. 
                    
IMPORTANT RULES:
1. Only analyze the call types that are explicitly mentioned in the data (Predictive Call, Outgoing Call, Incoming Call)
2. Use the EXACT numbers from the data provided - do not invent or assume numbers
3. Be supportive, positive, and use emojis to make your analysis fun and engaging
4. If a call type has zero calls, acknowledge it but don't overemphasize it

Always be accurate with the data while maintaining a friendly, encouraging tone!"""
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.5,  # Lower temperature for more accurate data interpretation
            max_tokens=2000
        )
        
        return chat_completion.choices[0].message.content
    except Exception as e:
        return f"❌ Error generating AI analysis: {str(e)}"
def generate_tab10_prompt(df_perf, user_query="", visualization_type=None):
    """
    Generate a comprehensive, friendly, and descriptive prompt for Groq AI 
    to analyze Daily Collector Performance data.
    
    Parameters:
    -----------
    df_perf : pandas.DataFrame
        The collector performance dataframe
    user_query : str, optional
        Specific question from the user
    visualization_type : str, optional
        Type of visualization: 'daily_accounts', 'daily_talk_time', 
        'calls_comparison', 'talk_time_comparison', 'time_frame_analysis', 'agent_summary'
    """
    
    # Get basic statistics
    total_collectors = df_perf["Collector"].nunique()
    total_calls = int(df_perf["No. Calls"].sum())
    total_accounts = int(df_perf["No. Acc."].sum())
    
    # Get date range
    df_temp = df_perf.copy()
    df_temp["Date_dt"] = pd.to_datetime(df_temp["First Login Time"], errors="coerce", dayfirst=True)
    date_range = ""
    if not df_temp["Date_dt"].isna().all():
        min_date = df_temp["Date_dt"].min().strftime("%d-%m-%Y")
        max_date = df_temp["Date_dt"].max().strftime("%d-%m-%Y")
        date_range = f"from {min_date} to {max_date}"
    
    # Get top performers
    top_callers = df_perf.groupby("Collector")["No. Calls"].sum().nlargest(3)
    top_accounts = df_perf.groupby("Collector")["No. Acc."].sum().nlargest(3)
    
    # Build the base prompt
    prompt = f"""You are a friendly and insightful data analyst helping to interpret Daily Collector Performance data. Be specific, use actual numbers, and provide actionable recommendations.

📊 **DATASET OVERVIEW:**
The dataset contains performance metrics for {total_collectors} collectors {date_range}, tracking their daily activities, call patterns, and productivity.

**Summary Statistics:**
- Total Collectors: {total_collectors}
- Total Calls Made: {total_calls:,}
- Total Accounts Worked: {total_accounts:,}
- Average Calls per Collector: {total_calls // total_collectors:,}
- Average Accounts per Collector: {total_accounts // total_collectors:,}

**Top 3 Performers by Calls:**
{chr(10).join([f"  • {name}: {int(calls):,} calls" for name, calls in top_callers.items()])}

**Top 3 Performers by Accounts Worked:**
{chr(10).join([f"  • {name}: {int(accs):,} accounts" for name, accs in top_accounts.items()])}

📋 **AVAILABLE DATA COLUMNS:**

1. **Collector Information:**
   - Collector, Access, Branch

2. **Time & Login Metrics:**
   - First Login Time, Last Logout Time, Total Login Time
   - No. Login, Talk Time (HH:MM:SS format)

3. **Performance Metrics:**
   - No. Calls, No. Acc.

4. **Hourly Call Distribution (14 time frames):**
   - Before 8AM through After 8PM

📈 **AVAILABLE VISUALIZATIONS:**
1. Daily Work Account Comparison per Agent (Pivot Table)
2. Daily Talk Time Comparison per Agent (Pivot Table)
3. Aggregated Summary per Agent (Complete Profile)
4. Calls per Collector (Bar Chart)
5. Talk Time per Collector (Bar Chart)
6. Calls per Collector by Time Frame (Grouped Bar Chart)

"""

    # Add visualization-specific guidance
    if visualization_type:
        viz_specific = {
            'daily_accounts': """
🎯 **ANALYZING: Daily Work Account Comparison per Agent**
Focus on: consistency, daily patterns, trends, and ideal targets.
""",
            'daily_talk_time': """
🎯 **ANALYZING: Daily Talk Time Comparison per Agent**
Focus on: time investment, efficiency (talk time vs accounts), and optimal ranges.
""",
            'calls_comparison': """
🎯 **ANALYZING: Calls per Collector Bar Chart**
Focus on: top/bottom performers, gaps, balance, and targets.
""",
            'talk_time_comparison': """
🎯 **ANALYZING: Talk Time per Collector Bar Chart**
Focus on: efficiency (seconds per call), optimal ranges, and coaching needs.
""",
            'time_frame_analysis': """
🎯 **ANALYZING: Calls by Time Frame**
Focus on: peak hours, coverage gaps, and optimal scheduling.
""",
            'agent_summary': """
🎯 **ANALYZING: Aggregated Agent Summary**
Focus on: complete profiles, efficiency ratios, and comprehensive recommendations.
"""
        }
        prompt += viz_specific.get(visualization_type, "")
    
    # Add user query
    if user_query and user_query.strip():
        prompt += f"""

🎯 **USER'S QUESTION:**
"{user_query}"

Provide a detailed, friendly, actionable response with specific numbers and practical recommendations.
"""
    
    # Default task
    if not user_query and not visualization_type:
        prompt += """

🎯 **YOUR TASK:**
Provide comprehensive analysis including:
1. Overall Performance Snapshot
2. Top Performers (with specific metrics)
3. Areas for Improvement
4. Calling Patterns
5. Actionable Recommendations

Be specific with numbers and keep it friendly!
"""
    
    return prompt



# =============================
# Page config & Tool Selector
# =============================
st.set_page_config(page_title="Dialer Specialist Dashboard", layout="wide")

tool_choice = st.radio(
    "📂 Select a Tool",
    ["Client Analyzer & Leads Creation (Tabs 1–8)", "Extras (Tab 9-17)"]
)

if tool_choice == "Client Analyzer & Leads Creation (Tabs 1–8)":
    # =============================
    # Page config
    # =============================
    col1,col2 = st.columns(2)

    st.set_page_config(page_title="Dialer Specialist Dashboard", layout="wide")
    st.title("📞 TELIKOS KALEO – The All-in-One Dialer Specialist Toolkit") 
    st.markdown(
        """
        <div style="display:flex; align-items:center; gap:0px;">
            <h3 style="margin:0; font-size:22px;">🔥 Made my Dainyel</h3>
            <p style="margin:0; font-size:16px; color:gray;">NAH I'D WIN 🐱 - Deinyel</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    # =============================
    # Helpers & Utility
    # =============================

    def _today_date() -> pd.Timestamp:
        return pd.to_datetime(datetime.today().date())


    def has_cols(df: pd.DataFrame, cols: List[str]) -> bool:
        return all(c in df.columns for c in cols)


    @st.cache_data(show_spinner=False)
    def preprocess_data(file_bytes: bytes) -> pd.DataFrame:
        """Read Excel from bytes, coerce types, and enrich with derived fields.
        Tailored to the exact ML columns provided by the user.
        """
        xls = BytesIO(file_bytes)
        df = pd.read_excel(xls, engine="openpyxl")

        # --- Coerce date columns (from your ML)
        date_cols = [
            "Assign Date",
            "Next Call Date",
            "PTP Date",
            "Abort Date",
            "Latest Remark Date",
            "Claim Paid Date",
            "Last Called Date",
            "Transfer Date",
        ]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # --- Coerce numeric columns (from your ML)
        numeric_cols = [
            "Amount Referred",
            "Balance",
            "PTP Amount",
            "Monthly Instalment",
            "Claim Paid Amount",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- Derived fields
        today = _today_date()

        # Days since assign + aging bucket
        if "Assign Date" in df.columns:
            df["Days Since Assign"] = (today - df["Assign Date"]).dt.days

            if df["Days Since Assign"].notna().any():
                max_days_val = pd.to_numeric(df["Days Since Assign"], errors="coerce").dropna()
                max_days = int(max_days_val.max()) if not max_days_val.empty else 121
                bins = [0, 30, 60, 90, 120, max(121, max_days + 1)]
                labels = ["0-30", "31-60", "61-90", "91-120", "120+"]
                df["Aging Bucket"] = pd.cut(df["Days Since Assign"], bins=bins, labels=labels, right=False)

        # Placement Prefix from Batch No. (safe even if pattern not found)
        if "Batch No." in df.columns:
            pref = df["Batch No."].astype(str).str.extract(r"(^.+?)(?:\d{2}/\d{2}/\d{4})")[0]
            df["Placement Prefix"] = pref.fillna(df["Batch No."].astype(str))

        return df


    # =============================
    # File upload & load
    # =============================
    if "file_bytes" not in st.session_state:
        st.session_state.file_bytes = None

    uploaded_file = st.file_uploader("Upload Masterlist Excel", type=["xlsx"])
    if uploaded_file is not None:
        st.session_state.file_bytes = uploaded_file.getvalue()

    if st.session_state.file_bytes is None:
        st.info("Please upload your masterlist Excel file to start.")
        st.stop()

    # Load (cached)
    df = preprocess_data(st.session_state.file_bytes)

    # =============================
    # Sidebar: Global Filters & Options
    # =============================
    with st.sidebar:
        st.header("🔎 Global Filters")

        # Team / Collector / Placement filters applied globally across tabs
        selected_teams = None
        if "Team" in df.columns:
            teams = sorted([t for t in df["Team"].dropna().unique()])
            selected_teams = st.multiselect("Team", teams, default=teams)

        selected_collectors = None
        if "Collector" in df.columns:
            collectors = sorted([c for c in df["Collector"].dropna().unique()])
            selected_collectors = st.multiselect("Collector", collectors)

        selected_placements = None
        if "Placement Prefix" in df.columns:
            placements = sorted([p for p in df["Placement Prefix"].dropna().unique()])
            selected_placements = st.multiselect("Placement", placements)

        st.divider()
        st.subheader("Priority Scoring Weights")
        w_due = st.slider("PTP / Next Call Imminence", 0.0, 5.0, 2.0, 0.1)
        w_balance = st.slider("Balance Size", 0.0, 5.0, 1.5, 0.1)
        w_aging = st.slider("Aging (older = higher)", 0.0, 5.0, 1.0, 0.1)
        st.caption("These weights feed into the Priority Call List.")

    # Apply global filters to a working copy
    filtered_df = df.copy()
    if selected_teams:
        filtered_df = filtered_df[filtered_df["Team"].isin(selected_teams)]
    if selected_collectors:
        filtered_df = filtered_df[filtered_df["Collector"].isin(selected_collectors)]
    if selected_placements:
        filtered_df = filtered_df[filtered_df["Placement Prefix"].isin(selected_placements)]

    # Numeric columns available (strictly from your ML)
    numeric_cols_all = [
        "Amount Referred",
        "Balance",
        "PTP Amount",
        "Monthly Instalment",
        "Claim Paid Amount",

    ]
    available_numeric = [c for c in numeric_cols_all if c in filtered_df.columns]

    # Convenience dates
    today = _today_date()
    week_start = today - pd.Timedelta(days=int(today.weekday()))  # Monday of this week
    month_start = today.replace(day=1)

    # =============================
    # Precompute common PTP sets
    # =============================
    ptp_today = pd.DataFrame()
    ptp_today_count = 0
    ptp_today_amount = 0.0
    if has_cols(filtered_df, ["PTP Date", "PTP Amount"]):
        ptp_today = filtered_df[filtered_df["PTP Date"].dt.date == today.date()]
        ptp_today_count = len(ptp_today)
        ptp_today_amount = float(ptp_today["PTP Amount"].sum())

    broken_ptp = pd.DataFrame()
    broken_ptp_count = 0
    broken_ptp_amount = 0.0
    if has_cols(filtered_df, ["PTP Date", "Claim Paid Date", "PTP Amount"]):
        lpd = "Claim Paid Date"
        mask_broken = (
            (filtered_df["PTP Date"] < today)
            & (filtered_df[lpd].isna() | (filtered_df[lpd] < filtered_df["PTP Date"]))
        )
        broken_ptp = filtered_df[mask_broken]
        broken_ptp_count = len(broken_ptp)
        broken_ptp_amount = float(broken_ptp["PTP Amount"].sum())

    # PTP performance (approximate) for Today / Week / Month
    ptp_perf = {}
    if has_cols(filtered_df, ["PTP Date", "PTP Amount"]) and has_cols(
        filtered_df, ["Claim Paid Date", "Claim Paid Amount"]
    ):
        sched = filtered_df[["PTP Date", "PTP Amount"]].dropna(subset=["PTP Date"]).copy()

        # Prepare payments dataframe
        paid = filtered_df[["Claim Paid Date", "Claim Paid Amount"]].copy()
        paid = paid.rename(
            columns={
                "Claim Paid Date": "Pay Date",
                "Claim Paid Amount": "Pay Amount",
            }
        ).dropna(subset=["Pay Date"])

        def _period_mask(s, start, end):
            return (s >= start) & (s <= end)

        periods = {
            "Today": (today, today),
            "This Week": (week_start, today),
            "This Month": (month_start, today),
        }

        for k, (start, end) in periods.items():
            sched_sum = float(sched[_period_mask(sched["PTP Date"], start, end)]["PTP Amount"].sum())
            pay_sum = float(paid[_period_mask(paid["Pay Date"], start, end)]["Pay Amount"].sum())
            rate = (pay_sum / sched_sum * 100.0) if sched_sum > 0 else np.nan
            ptp_perf[k] = {"Scheduled": sched_sum, "Collected": pay_sum, "Rate %": rate}

    # =============================
    # Tabs
    # =============================
    TAB_TITLES = [
        "📊 Portfolio Overview",
        "📅 PTP Analysis",
        "🧑‍🤝‍🧑 Team & Collector Performance",
        "📞 Operational Pipeline",
        "📋 Lead Creation",
        "⭐ Priority Call List",
        "📤 EOD Export",
        "📖 User Manual",
    ]

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(TAB_TITLES)
    # ===== TAB 1: Portfolio Overview =====
    with tab1:
        st.subheader("Summary Statistics")
        if available_numeric and not filtered_df[available_numeric].empty:
            st.dataframe(filtered_df[available_numeric].describe())
            st.caption("Statistical summaries for numeric portfolio metrics.")
        else:
            st.info("No numeric columns available for summary. Please check if your file contains columns like 'Balance', 'PTP Amount', etc.")

        # ✅ Total Portfolio Balance with Peso sign and comma separator
        if "Balance" in filtered_df.columns:
            total_balance = filtered_df["Balance"].sum()
            st.subheader("Total Portfolio Balance")
            st.metric("Total Balance", f"₱{total_balance:,.2f}")
            st.caption("Total outstanding balance across all filtered accounts.")
        else:
            st.info("Portfolio Balance visualization is not available because the 'Balance' column is missing.")

        # ✅ Status code distribution
        if "Status Code" in filtered_df.columns:
            status_counts = filtered_df["Status Code"].value_counts(dropna=False).reset_index()
            status_counts.columns = ["Status Code", "Count"]
            if not status_counts.empty:
                fig = px.pie(
                    status_counts,
                    names="Status Code",
                    values="Count",
                    title="Status Code Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No Status Code data available.")
        else:
            st.info("Status Code visualization is not available because the 'Status Code' column is missing.")

        # ✅ Aging bucket – balance and count
        if has_cols(filtered_df, ["Cycle", "Balance"]):
            aging_summary = (
                filtered_df.groupby("Cycle").agg(
                    Balance_Total=("Balance", "sum"),
                    Accounts=("Cycle", "size")
                ).reset_index()
            )

            if not aging_summary.empty:
                # Format peso sign in labels
                aging_summary["Balance_Total_Label"] = aging_summary["Balance_Total"].apply(
                    lambda x: f"₱{x:,.2f}"
                )

                # 🔹 Identify top cycles
                top_accounts_cycle = aging_summary.loc[aging_summary["Accounts"].idxmax()]
                top_balance_cycle = aging_summary.loc[aging_summary["Balance_Total"].idxmax()]

                st.subheader("📊 Key Cycle Metrics")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "Cycle with Most Accounts",
                        f"{top_accounts_cycle['Cycle']} ({top_accounts_cycle['Accounts']:,})"
                    )
                with col2:
                    st.metric(
                        "Cycle with Highest Balance",
                        f"{top_balance_cycle['Cycle']} (₱{top_balance_cycle['Balance_Total']:,.2f})"
                    )

                # Charts
                col_a, col_b = st.columns(2)
                with col_a:
                    fig1 = px.bar(
                        aging_summary,
                        x="Cycle",
                        y="Balance_Total",
                        text="Balance_Total_Label",
                        title="Balance by Cycle"
                    )
                    fig1.update_traces(textposition="outside")
                    fig1.update_yaxes(tickprefix="₱", separatethousands=True)
                    st.plotly_chart(fig1, use_container_width=True)

                with col_b:
                    fig2 = px.bar(
                        aging_summary,
                        x="Cycle",
                        y="Accounts",
                        text_auto=True,
                        title="Account Count by Cycle"
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                st.caption("Breakdown of balances and account counts by age since assignment.")
            else:
                st.info("Aging bucket visualization is not available due to missing or empty 'Cycle' and/or 'Balance' columns.")
        else:
            st.info("Aging bucket visualization is not available because required columns ('Cycle', 'Balance') are missing.")

    # ===== TAB 2: PTP Analysis =====
    with tab2:
        st.subheader("📅 Key PTP KPIs (Today)")

        if has_cols(filtered_df, ["PTP Date", "PTP Amount"]):
            # =====================
            # Define Today
            # =====================
            today = pd.to_datetime("today").normalize()

            # =====================
            # PTP Scheduled Today
            # =====================
            ptp_today = filtered_df[filtered_df["PTP Date"].dt.date == today.date()]
            ptp_today_count = len(ptp_today)
            ptp_today_amount = ptp_today["PTP Amount"].sum() if not ptp_today.empty else 0

            # =====================
            # Broken PTP (Double Confirmation: Claim Paid Date must be empty)
            # =====================
            broken_ptp = filtered_df[
                (filtered_df["PTP Date"].notna()) &
                (filtered_df["PTP Date"] < today) &
                (filtered_df["Claim Paid Date"].isna())
            ]
            broken_ptp_count = len(broken_ptp)
            broken_ptp_amount = broken_ptp["PTP Amount"].sum() if not broken_ptp.empty else 0

            # =====================
            # KPI Metrics
            # =====================
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("PTP Scheduled Today (Count)", ptp_today_count)
            k2.metric("PTP Scheduled Today (Amount)", f"₱ {ptp_today_amount:,.2f}")
            k3.metric("Overdue PTPs Without Payment (Count)", broken_ptp_count)
            k4.metric("Overdue PTPs Without Payment (Amount)", f"₱ {broken_ptp_amount:,.2f}")

            # =====================
            # Expanders
            # =====================
            if ptp_today_count > 0:
                with st.expander("📌 PTP Scheduled Today Accounts"):
                    cols = [c for c in ["Name", "Old I.C.", "Batch No.", "PTP Amount", "PTP Date", "Team", "Collector"] if c in ptp_today.columns]
                    st.dataframe(ptp_today[cols].sort_values(by=["PTP Amount"], ascending=False))

            if broken_ptp_count > 0:
                with st.expander("❌ Overdue PTPs Without Payment"):
                    cols = [c for c in ["Name", "Old I.C.", "Batch No.", "PTP Amount", "PTP Date", "Claim Paid Date", "Team", "Collector","Status Code"] if c in broken_ptp.columns]
                    st.dataframe(broken_ptp[cols].sort_values(by=["PTP Date"]))

            # =====================
            # PTP Performance Table
            # =====================
            if ptp_perf:
                perf_df = pd.DataFrame(ptp_perf).T.reset_index().rename(columns={"index": "Period"})
                st.subheader("📊 PTP Performance (Approximate)")
                st.dataframe(perf_df)
                st.caption("💡 Collected uses Last Payment Date/Amount as a proxy; depends on data alignment with PTPs.")

            # =====================
            # Trend Visualizations
            # =====================
            if filtered_df["PTP Date"].notna().any():
                # Daily Trend
                ptp_trend = (
                    filtered_df.assign(PTP_Date_D=filtered_df["PTP Date"].dt.date)
                    .groupby("PTP_Date_D")["PTP Amount"].sum()
                    .reset_index()
                )
                if not ptp_trend.empty:
                    fig = px.line(
                        ptp_trend,
                        x="PTP_Date_D",
                        y="PTP Amount",
                        markers=True,
                        title="📈 Daily PTP Amount Trend"
                    )
                    fig.update_yaxes(title="Amount (₱)", tickprefix="₱ ", separatethousands=True)
                    fig.update_traces(
                        hovertemplate="Date: %{x}<br>Amount: ₱ %{y:,.2f}<extra></extra>"
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Monthly Trend (Collected vs Broken)
                monthly_ptp_status = (
                    filtered_df.assign(
                        PTP_M=filtered_df["PTP Date"].dt.to_period("M").astype(str),
                        Status=np.where(filtered_df["Claim Paid Date"].isna(), "Broken", "Collected")
                    )
                    .groupby(["PTP_M", "Status"])["PTP Amount"].sum()
                    .reset_index()
                )

                if not monthly_ptp_status.empty:
                    fig2 = px.bar(
                        monthly_ptp_status,
                        x="PTP_M",
                        y="PTP Amount",
                        color="Status",
                        barmode="stack",
                        text="PTP Amount",
                        title="📊 Monthly PTP Amount Trend (Collected vs Broken)"
                    )
                    fig2.update_traces(
                        texttemplate="₱ %{text:,.0f}",
                        hovertemplate="Month: %{x}<br>Status: %{color}<br>Amount: ₱ %{y:,.2f}<extra></extra>"
                    )
                    fig2.update_yaxes(title="Amount (₱)", tickprefix="₱ ", separatethousands=True)
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info("ℹ️ Monthly PTP trend not available (no data).")
            else:
                st.info("ℹ️ PTP trend not available because 'PTP Date' column is missing.")
        else:
            st.info("⚠️ PTP Analysis requires 'PTP Date' and 'PTP Amount' columns.")

    # ===== TAB 3: Team & Collector Performance =====
    with tab3:
        if "Team" in filtered_df.columns and not filtered_df.empty:
            team_summary = (
                filtered_df.groupby("Team").agg(total_balance=("Balance", "sum"), total_ptp=("PTP Amount", "sum")).reset_index()
            )
            if not broken_ptp.empty and "Team" in broken_ptp.columns:
                broken_by_team = broken_ptp.groupby("Team")["PTP Amount"].sum().rename("broken_ptp").reset_index()
                team_summary = team_summary.merge(broken_by_team, on="Team", how="left").fillna({"broken_ptp": 0})
                team_summary["Broken PTP %"] = np.where(
                    team_summary["total_ptp"] > 0, team_summary["broken_ptp"] / team_summary["total_ptp"] * 100, np.nan
                )

            if not team_summary.empty:
                st.subheader("Team Scorecard")
                st.dataframe(team_summary)
                st.caption("Compares teams on total balance handled, total PTP commitments, and broken PTP percentage.")
                fig_tb = px.bar(team_summary, x="Team", y="total_balance", title="Team Total Balance", text_auto=True)
                st.plotly_chart(fig_tb, use_container_width=True)
            else:
                st.info("Team Scorecard visualization is not available due to missing or empty data.")
        else:
            st.info("Team Scorecard visualization is not available because the 'Team' column is missing.")

        if "Collector" in filtered_df.columns and not broken_ptp.empty:
            broken_by_collector = broken_ptp.groupby("Collector")["PTP Amount"].sum().reset_index()
            if not broken_by_collector.empty:
                st.subheader("Broken PTP Amount by Collector")
                fig_bc = px.bar(broken_by_collector, x="Collector", y="PTP Amount", title="Broken PTP by Collector", text_auto=True)
                st.plotly_chart(fig_bc, use_container_width=True)
            else:
                st.info("Broken PTP by Collector visualization is not available due to missing or empty data.")
        else:
            st.info("Broken PTP by Collector visualization is not available because the 'Collector' column is missing or there are no broken PTPs.")

        if has_cols(filtered_df, ["Team", "Status Code"]):
            st.subheader("Team vs Status Code Heatmap")
            normalize = st.checkbox("Normalize by Team (row %)")
            heatmap_data = filtered_df.groupby(["Team", "Status Code"]).size().unstack(fill_value=0)

            if not heatmap_data.empty and heatmap_data.shape[0] > 0 and heatmap_data.shape[1] > 0:
                if normalize:
                    denom = heatmap_data.sum(axis=1).replace(0, np.nan)
                    heatmap_plot = heatmap_data.div(denom, axis=0)
                    fmt = ".2f"
                else:
                    heatmap_plot = heatmap_data
                    fmt = ".0f"

                try:
                    fig, ax = plt.subplots()
                    sns.heatmap(heatmap_plot, cmap="Blues", annot=True, fmt=fmt, ax=ax)
                    st.pyplot(fig)
                except Exception:
                    st.info("Not enough data to render heatmap.")
            else:
                st.info("Team vs Status Code Heatmap visualization is not available due to missing or empty data.")
        else:
            st.info("Team vs Status Code Heatmap visualization is not available because required columns ('Team', 'Status Code') are missing.")

    # ===== TAB 4: Operational Pipeline =====
    with tab4:
        st.subheader("Next Call Date Pipeline")
        if "Next Call Date" in filtered_df.columns and filtered_df["Next Call Date"].notna().any():
            pipeline = filtered_df.assign(NCD=filtered_df["Next Call Date"].dt.date).groupby("NCD").size().reset_index(name="Count")

            if not pipeline.empty and pipeline["Count"].sum() > 0:
                def _bucket(d):
                    if pd.isna(d):
                        return "Unknown"
                    if d < today.date():
                        return "Overdue"
                    elif d == today.date():
                        return "Today"
                    return "Upcoming"

                pipeline["Bucket"] = pipeline["NCD"].apply(_bucket)
                fig_nc = px.bar(pipeline, x="NCD", y="Count", color="Bucket", title="Scheduled Calls by Date", text_auto=True)
                st.plotly_chart(fig_nc, use_container_width=True)
            else:
                st.info("Next Call Date Pipeline visualization is not available due to missing or empty data.")
        else:
            st.info("Next Call Date Pipeline visualization is not available because the 'Next Call Date' column is missing or empty.")

        # Locked / Aborted
        mcols = st.columns(2)
        if "Locked Files" in filtered_df.columns:
            locked_count = filtered_df["Locked Files"].sum() if pd.api.types.is_numeric_dtype(filtered_df["Locked Files"]) else filtered_df["Locked Files"].notna().sum()
            mcols[0].metric("Locked Files Count", int(locked_count))
        else:
            mcols[0].info("Locked Files Count visualization is not available because the 'Locked Files' column is missing.")

        if "Abort Date" in filtered_df.columns:
            abort_count = int(filtered_df["Abort Date"].notna().sum())
            mcols[1].metric("Aborted Accounts", abort_count)
        else:
            mcols[1].info("Aborted Accounts visualization is not available because the 'Abort Date' column is missing.")

    # ===== TAB 5: Lead Creation (Placement Grouped) =====
    with tab5:
        st.subheader("Lead Creation Tool")
        required = ["Batch No.", "Old I.C.", "Status Code", "Account has PTP?","Account has claim paid?"]

        if not has_cols(filtered_df, required):
            st.error("The file must have 'Batch No.', 'Status Code','Account has PTP?','Account has claim paid?' and 'Old I.C.' columns.")
        else:
            placement_list = sorted(filtered_df["Placement Prefix"].dropna().unique()) if "Placement Prefix" in df.columns else []
            selected_placement = st.selectbox("📌 Select Placement", [""] + placement_list)

            # ✅ Fixed exclusion keywords (STRICT, UPPERCASE only)
            exclusion_keywords = ["PTP", "FOLLOW UP", "PAYMENT", "RETURN"]

            leads_df = filtered_df.copy()

            # Filter by placement
            if selected_placement:
                leads_df = leads_df[leads_df["Placement Prefix"] == selected_placement]

            # Exclude by Status Code (must be exact uppercase contains)
            if "Status Code" in leads_df.columns:
                pattern = "|".join([fr"\b{k}\b" for k in exclusion_keywords])  # strict word boundary
                leads_df = leads_df[~leads_df["Status Code"].astype(str).str.contains(pattern, case=True, na=False)]

            # ✅ Double check using "Account has PTP?" → must be blank or 0
            if "Account has PTP?" in leads_df.columns:
                leads_df = leads_df[leads_df["Account has PTP?"].isin([0, "0", "", None, np.nan])]

            # ✅ Double check using "Account has claim paid?" → must be blank or 0
            if "Account has claim paid?" in leads_df.columns:
                leads_df = leads_df[leads_df["Account has claim paid?"].isin([0, "0", "", None, np.nan])]

            # ✅ Optional Status Code filter (still available if user wants to INCLUDE)
            if "Status Code" in leads_df.columns:
                status_options = sorted(leads_df["Status Code"].dropna().unique())
                selected_status = st.multiselect("🎯 Include only these Status Codes (optional):", status_options)
                if selected_status:
                    leads_df = leads_df[leads_df["Status Code"].isin(selected_status)]

            # ✅ Optional Balance Filter
            if "Balance" in leads_df.columns and not leads_df["Balance"].dropna().empty:
                st.markdown("⚖️ **Balance Range Filter (optional)**")
                min_bal, max_bal = float(leads_df["Balance"].min()), float(leads_df["Balance"].max())
                balance_range = st.slider(
                    "Select Balance Range",
                    min_value=float(min_bal),
                    max_value=float(max_bal),
                    value=(float(min_bal), float(max_bal)),
                    step=100.0,
                )
                leads_df = leads_df[(leads_df["Balance"] >= balance_range[0]) & (leads_df["Balance"] <= balance_range[1])]

            # Remove duplicate Old I.C.
            if "Old I.C." in leads_df.columns:
                leads_df = leads_df.drop_duplicates(subset=["Old I.C."])

            # Final columns to show
            lead_cols = [
                c for c in ["Name", "Old I.C.", "Batch No.", "Status Code", "Balance", "Team", "Collector", "PTP Date", "PTP Amount"]
                if c in leads_df.columns
            ]

            # ✅ Format money columns with commas
            money_cols = [c for c in ["Balance", "PTP Amount"] if c in leads_df.columns]
            for col in money_cols:
                leads_df[col] = pd.to_numeric(leads_df[col], errors="coerce")
                leads_df[col] = leads_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "")

            if not leads_df.empty and len(lead_cols) > 0:
                st.success(f"✅ Found {len(leads_df)} leads for placement '{selected_placement}' after exclusions and filters.")
                st.dataframe(leads_df[lead_cols])

                output = BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    # 🔹 Keep raw numbers in Excel for formulas
                    export_df = leads_df.copy()
                    export_df.to_excel(writer, index=False, sheet_name="Leads")

                downloaded = st.download_button(
                    label="📥 Download Placement Leads Excel",
                    data=output.getvalue(),
                    file_name=f"{(selected_placement or 'placement').replace('/', '-').replace('\\', '-').replace(' ', '_')}_ALL.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                if downloaded:
                    st.balloons()
            else:
                st.warning(f"No leads found for placement '{selected_placement}' with current filters.")

    # ===== TAB 6: Priority Calls =====
    with tab6:
        st.subheader("📞 Priority Call List")
        st.caption("Accounts ranked by priority score using Balance, PTP, Assign Date, and Aging.")

        if "Balance" in filtered_df.columns and "Assign Date" in filtered_df.columns:
            df6 = filtered_df.copy()

            # Ensure date column is datetime
            df6["Assign Date"] = pd.to_datetime(df6["Assign Date"], errors="coerce")

            # Days since assign
            today = pd.Timestamp.today().normalize()
            df6["Days Since Assign"] = (today - df6["Assign Date"]).dt.days

            # Normalize scoring components
            df6["Balance_Score"] = (df6["Balance"] / df6["Balance"].max()).fillna(0)
            df6["Aging_Score"] = (df6["Days Since Assign"] / df6["Days Since Assign"].max()).fillna(0)
            df6["PTP_Score"] = np.where(df6["PTP Amount"] > 0, 1, 0)

            # Priority formula
            w_balance, w_aging, w_ptp = 0.5, 0.3, 0.2
            df6["Priority Score"] = (
                w_balance * df6["Balance_Score"] +
                w_aging * df6["Aging_Score"] +
                w_ptp * df6["PTP_Score"]
            )

            # Slider for top N
            topn = st.slider("Select number of top priority accounts", 5, 100, 20, step=5)
            top_accounts = df6.sort_values("Priority Score", ascending=False).head(topn)

            # Save to session_state for Tab 7
            st.session_state["top_calls"] = top_accounts

            # Display table
            st.dataframe(
                top_accounts[[
                    "No", "Name", "Collector", "Team", "Balance",
                    "Assign Date", "PTP Date", "Next Call Date", "Priority Score"
                ]],
                use_container_width=True
            )
        else:
            st.warning("Balance or Assign Date column not available. Cannot compute priority list.")
            
    # ===== TAB 7: EOD Export =====
    with tab7:
        st.subheader("End-of-Day (EOD) Report Export")
        st.caption("Exports key tables to a single Excel workbook. Charts are not embedded; this keeps file size small and generation fast.")

        def safe_df(df_in: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
            return df_in[[c for c in cols if c in df_in.columns]].copy()

        export_frames: dict[str, pd.DataFrame] = {}

        # Summary stats
        if available_numeric and not filtered_df[available_numeric].empty:
            export_frames["SummaryStats"] = filtered_df[available_numeric].describe()

        # Status counts
        if "Status Code" in filtered_df.columns:
            sc = filtered_df["Status Code"].value_counts(dropna=False).rename_axis("Status Code").reset_index(name="Count")
            if not sc.empty:
                export_frames["StatusCounts"] = sc

        # Aging summary
        if has_cols(filtered_df, ["Aging Bucket", "Balance"]):
            aging_summary = (
                filtered_df.groupby("Aging Bucket").agg(Balance_Total=("Balance", "sum"), Accounts=("Aging Bucket", "size")).reset_index()
            )
            if not aging_summary.empty:
                export_frames["AgingSummary"] = aging_summary

        # PTP today
        if not ptp_today.empty:
            export_frames["PTP_Today"] = safe_df(
                ptp_today,
                ["Name", "Old I.C.", "Batch No.", "PTP Amount", "PTP Date", "Team", "Collector"]
            )

        # Broken PTP (filtered same as priority call rules)
        if not broken_ptp.empty:
            bp = broken_ptp.copy()
            if "Status Code" in bp.columns:
                bp = bp[~bp["Status Code"].astype(str).str.contains("PAYMENT", case=False, na=False)]
            bp = bp[(bp["PTP Date"].notna()) & (bp["PTP Date"] < today)]
            week_start = today - pd.Timedelta(days=int(today.weekday()))
            last_week_start = week_start - pd.Timedelta(days=7)
            bp = bp[bp["PTP Date"].between(last_week_start, today)]
            export_frames["Broken_PTP"] = safe_df(bp, [
                "Name", "Old I.C.", "Batch No.", "PTP Amount", "PTP Date",
                "Last Payment Date (Uploaded to VOLARE)", "Team", "Collector", "Status Code"
            ])

        # PTP trends
        if has_cols(filtered_df, ["PTP Date", "PTP Amount"]) and filtered_df["PTP Date"].notna().any():
            daily = filtered_df.assign(PTP_Date_D=filtered_df["PTP Date"].dt.date).groupby("PTP_Date_D")["PTP Amount"].sum().reset_index()
            monthly = filtered_df.assign(PTP_M=filtered_df["PTP Date"].dt.to_period("M").astype(str)).groupby("PTP_M")["PTP Amount"].sum().reset_index()
            if not daily.empty:
                export_frames["PTP_DailyTrend"] = daily
            if not monthly.empty:
                export_frames["PTP_MonthlyTrend"] = monthly

        # Team scorecard
        if "Team" in filtered_df.columns and not filtered_df.empty:
            team_summary = (
                filtered_df.groupby("Team").agg(total_balance=("Balance", "sum"), total_ptp=("PTP Amount", "sum")).reset_index()
            )
            if not broken_ptp.empty and "Team" in broken_ptp.columns:
                broken_by_team = broken_ptp.groupby("Team")["PTP Amount"].sum().rename("broken_ptp").reset_index()
                team_summary = team_summary.merge(broken_by_team, on="Team", how="left").fillna({"broken_ptp": 0})
                team_summary["Broken PTP %"] = np.where(
                    team_summary["total_ptp"] > 0, team_summary["broken_ptp"] / team_summary["total_ptp"] * 100, np.nan
                )
            if not team_summary.empty:
                export_frames["TeamScorecard"] = team_summary

        # Broken by collector
        if "Collector" in filtered_df.columns and not broken_ptp.empty:
            bbc = broken_ptp.groupby("Collector")["PTP Amount"].sum().reset_index()
            if not bbc.empty:
                export_frames["BrokenByCollector"] = bbc

        # Next Call Pipeline
        if "Next Call Date" in filtered_df.columns and filtered_df["Next Call Date"].notna().any():
            pipeline = filtered_df.assign(NCD=filtered_df["Next Call Date"].dt.date).groupby("NCD").size().reset_index(name="Count")
            if not pipeline.empty:
                export_frames["NextCallPipeline"] = pipeline

        # ✅ Priority Calls (fetch from session_state)
        top_calls = st.session_state.get("top_calls", pd.DataFrame())
        if isinstance(top_calls, pd.DataFrame) and not top_calls.empty:
            export_frames["PriorityCalls"] = top_calls
        else:
            export_frames["PriorityCalls"] = pd.DataFrame(columns=[
                "Priority Score", "Name", "Old I.C.", "Team", "Collector",
                "Balance", "PTP Amount", "PTP Date", "Next Call Date", "Assign Date", "Status Code"
            ])

        # README sheet
        readme = pd.DataFrame({
            "Sheet": list(export_frames.keys()) + ["README"],
            "Notes": [
                "Numeric summary stats.",
                "Count of accounts by Status Code.",
                "Balances and account counts by aging bucket.",
                "Accounts with PTP scheduled today.",
                "Accounts with PTP in the past but no corresponding payment.",
                "Sum of PTP amounts by day.",
                "Sum of PTP amounts by month.",
                "Team-level KPIs, including Broken PTP %.",
                "Broken PTP totals by collector.",
                "Next Call Date counts.",
                "Priority call list with scores.",
                "This sheet explains the EOD export contents.",
            ][: len(list(export_frames.keys())) + 1],
        })

        # Export button
        exp_io = BytesIO()
        with pd.ExcelWriter(exp_io, engine="openpyxl") as writer:
            for name, frame in export_frames.items():
                if isinstance(frame, pd.DataFrame):
                    # Format money columns
                    if any(col in frame.columns for col in ["Balance", "PTP Amount", "Balance_Total", "total_balance", "total_ptp", "broken_ptp"]):
                        for col in frame.columns:
                            if col in ["Balance", "PTP Amount", "Balance_Total", "total_balance", "total_ptp", "broken_ptp"]:
                                frame[col] = frame[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else x)
                    frame.to_excel(writer, sheet_name=name[:31], index=False)
            readme.to_excel(writer, sheet_name="README", index=False)

        st.download_button(
            label="📦 Download EOD Excel Report",
            data=exp_io.getvalue(),
            file_name="EOD_Dialer_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # ===== TAB 8: User Manual =====
    with tab8:
        st.subheader("📖 Comprehensive User Manual & Feature Guide")
        st.markdown("""
    Welcome to the **Dialer Specialist Dashboard**!  
    This manual explains every tab, feature, and terminology in detail, including formulas and logic used in visualizations. Designed for new users and experienced specialists alike.

    ---

    ## 🟦 Sidebar: Global Filters & Options

    - **Team Filter:**  
      Lets you select one or more teams. All dashboard data will be filtered to only show accounts handled by the selected teams.

    - **Collector Filter:**  
      Lets you focus on specific collectors (agents). Useful for reviewing individual performance.

    - **Placement Filter:**  
      Filters accounts by their placement group (campaign or batch). This helps you analyze specific groups.

    - **Priority Scoring Weights:**  
      Sliders that adjust how the Priority Call List (Tab 6) ranks accounts.  
      - **PTP / Next Call Imminence:** Higher values prioritize accounts with upcoming promises or calls.
      - **Balance Size:** Higher values prioritize accounts with larger outstanding balances.
      - **Aging (older = higher):** Higher values prioritize accounts that have been assigned longer.

    ---

    ## 1. 📊 Portfolio Overview

    - **Summary Statistics:**  
      Shows basic statistics (mean, min, max, etc.) for all numeric columns (e.g., Balance, PTP Amount).  
      *Formula:* Uses pandas `.describe()`.

    - **Total Portfolio Balance:**  
      Displays the sum of all balances (`Balance`).  
      *Formula:* `Total Balance = sum(Balance)`.

    - **Status Code Distribution:**  
      Pie chart showing how accounts are distributed by status (e.g., PTP, FOLLOW UP).  
      *Terminology:*  
        - **Status Code:** Indicates the current state of an account (e.g., PTP means Promise to Pay).

    - **Aging Bucket Breakdown:**  
      Bar charts showing total balance and account count by "Cycle" (age since assignment).  
      *Formula:*  
        - **Cycle:** Grouped by days since "Assign Date" (e.g., 0-30, 31-60 days).
        - **Aging Bucket:** `pd.cut(Days Since Assign, bins=[0,30,60,90,120,121+])`

    - **Key Cycle Metrics:**  
      Highlights the cycle with the most accounts and the highest balance.

    ---

    ## 2. 📅 PTP Analysis

    - **PTP Scheduled Today:**  
      Shows how many accounts have a Promise-to-Pay (PTP) scheduled for today and the total amount.  
      *Formula:*  
        - **PTP Scheduled Today:** Accounts where `PTP Date == today`.

    - **Broken Promise Metrics:**  
      Shows overdue PTPs with no payment.  
      *Formula:*  
        - **Broken PTP:** `PTP Date < today` and `Claim Paid Date` is empty.

    - **PTP Performance Table:**  
      Compares scheduled PTPs and actual payments for today, this week, and this month.  
      *Formula:*  
        - **Rate %:** `(Collected / Scheduled) * 100`

    - **PTP Trends:**  
      Line and bar charts showing daily and monthly PTP amounts.  
      *Terminology:*  
        - **PTP Amount:** Amount promised to be paid.
        - **Collected:** Amount actually paid.

    - **Detailed Account Lists:**  
      Expandable tables for accounts scheduled for PTP today or with broken promises.

    ---

    ## 3. 🧑‍🤝‍🧑 Team & Collector Performance

    - **Team Scorecard:**  
      Table and bar chart comparing teams on total balance, total PTP, and broken PTP percentage.  
      *Formula:*  
        - **Broken PTP %:** `(Broken PTP / Total PTP) * 100`

    - **Broken PTP by Collector:**  
      Bar chart showing which collectors have the most broken promises.

    - **Team vs Status Code Heatmap:**  
      Visual heatmap showing how status codes are distributed across teams.  
      *Formula:*  
        - **Normalized Heatmap:** Each cell shows the percentage of accounts for a status code within a team.

    ---

    ## 4. 📞 Operational Pipeline

    - **Next Call Date Pipeline:**  
      Bar chart showing how many accounts are scheduled for calls on each date, color-coded by status (Overdue, Today, Upcoming).  
      *Terminology:*  
        - **Next Call Date:** When the next call is scheduled for an account.
        - **Overdue:** Next Call Date is before today.
        - **Today:** Next Call Date is today.
        - **Upcoming:** Next Call Date is after today.

    - **Locked Files Count:**  
      Shows the number of accounts currently locked (restricted from action).

    - **Aborted Accounts:**  
      Shows how many accounts have been marked as aborted (no further action).

    ---

    ## 5. 📋 Lead Creation

    - **Placement Selection:**  
      Lets you choose a placement group to generate a targeted lead list.

    - **Exclusion Rules:**  
      Automatically excludes accounts with certain status codes (PTP, PAYMENT, etc.) and those with active PTPs or claim payments.

    - **Optional Filters:**  
      - **Status Code Filter:** Include only specific status codes.
      - **Balance Range Filter:** Focus on accounts within a certain balance range.

    - **Duplicate Removal:**  
      Ensures each "Old I.C." (account ID) appears only once.

    - **Downloadable Excel:**  
      Export the filtered lead list for use in calling campaigns.

    ---

    ## 6. ⭐ Priority Call List

    - **Scoring Model:**  
      Ranks accounts using a weighted formula based on balance, aging, and PTP status.  
      *Formula:*  
        - **Priority Score:**  
          `Priority Score = (w_balance * Balance_Score) + (w_aging * Aging_Score) + (w_ptp * PTP_Score)`  
          - **Balance_Score:** Normalized balance (`Balance / max(Balance)`)
          - **Aging_Score:** Normalized days since assign (`Days Since Assign / max(Days Since Assign)`)
          - **PTP_Score:** 1 if PTP Amount > 0, else 0

    - **Adjustable Top N:**  
      Slider lets you choose how many top priority accounts to display.

    - **Detailed Table:**  
      Shows account details and their calculated priority score.

    - **Sidebar Weights:**  
      Adjust sidebar sliders to change how the scoring model prioritizes different factors.

    ---

    ## 7. 📤 EOD Export

    - **Excel Report Generation:**  
      Exports all key tables (summary stats, status counts, aging, PTPs, pipeline, priority calls, etc.) to a single Excel workbook.

    - **README Sheet:**  
      Includes a sheet explaining the contents and purpose of each exported table.

    - **No Charts:**  
      Only tables are exported to keep the file size small and export fast.

    - **Download Button:**  
      Click to download the report for sharing or record-keeping.

    ---

    ## 8. 📖 User Manual (You Are Here)

    - **Purpose:**  
      This tab provides detailed explanations for every dashboard feature and tab.  
      *Tip:* Refer here whenever you need help understanding a metric, chart, or filter.

    - **Excel File Inspector:**  
      Shows all columns in your uploaded Excel file and previews the first few rows.  
      *Purpose:* Debug file structure and ensure compatibility.

    - **Missing Columns Warning:**  
      If important columns are missing, you will see a warning listing exactly which columns are required for full dashboard functionality.

    ---

    ### 📚 Glossary of Terms

    - **PTP (Promise to Pay):**  
      A commitment from a customer to pay a certain amount by a specific date.

    - **Claim Paid:**  
      Indicates whether a payment has been made to settle a claim.

    - **Assign Date:**  
      The date the account was assigned to a collector or team.

    - **Cycle / Aging Bucket:**  
      Groups accounts by how long they've been assigned (e.g., 0-30 days).

    - **Status Code:**  
      Describes the current status of an account (e.g., PTP, FOLLOW UP, PAYMENT).

    - **Collector:**  
      The agent responsible for managing the account.

    - **Placement:**  
      The campaign or batch group to which an account belongs.

    - **Locked Files:**  
      Accounts that are restricted from further action.

    - **Aborted Accounts:**  
      Accounts marked as closed or no further action required.

    - **Lead:**  
      An account selected for follow-up or calling.

    - **Priority Score:**  
      A calculated value used to rank accounts for follow-up, based on balance, aging, and PTP status.

    ---

    ### 📐 Formulas Used

    - **Total Portfolio Balance:**  
      `sum(Balance)`

    - **PTP Scheduled Today:**  
      `PTP Date == today`

    - **Broken PTP:**  
      `PTP Date < today and Claim Paid Date is empty`

    - **PTP Performance Rate:**  
      `Rate % = (Collected / Scheduled) * 100`

    - **Aging Bucket:**  
      `pd.cut(Days Since Assign, bins=[0,30,60,90,120,121+])`

    - **Priority Score:**  
      `Priority Score = (w_balance * Balance_Score) + (w_aging * Aging_Score) + (w_ptp * PTP_Score)`

    ---

    ### ✅ Best Practice

    Always check for missing columns in the Excel File Inspector before using the dashboard.  
    Set your desired filters in the sidebar before exploring the tabs to ensure all data and analysis are relevant to your current focus.

    ---

    ### Need more help?

    Contact your dashboard administrator or refer to this manual for step-by-step guidance.
        """)

elif tool_choice == "Extras (Tab 9-17)":
    st.set_page_config(page_title="Dialer Specialist Dashboard", layout="wide")
    st.title("📞 TELIKOS KALEO – The All-in-One Dialer Specialist Toolkit") 
    st.markdown(
        """
        <div style="display:flex; align-items:center; gap:0px;">
            <h3 style="margin:0; font-size:22px;">🔥 Made my Dainyel</h3>
            <p style="margin:0; font-size:16px; color:gray;">NAH I'D WIN 🐱 - Deinyel</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.markdown("---")

    tab9, tab10, tab11, tab12, tab13, tab14, tab15, tab16, tab17, tab18 = st.tabs([
    "⏱️ Tab 9: Call Summarizer",
    "📞 Tab 10: Daily Performance Analyzer",
    "📊 Tab 11: VB Result Data Summarizer",
    "📋 Tab 12: DSU Report Generator",
    "💰 Tab 13: Claim Paid Analyzer",
    "🎯 Tab 14: PREDEL Personal Loan Target",
    "📌 Tab 15: Final DROPPED Status Checker",
    "📊 Tab 16: Intensity Report Analyzer",  # NEW TAB
    "🧪 Tab 17: Playwright Experiment",      # MOVED FROM TAB 16
    "📊 Tab 18:  Leads Counter"
    ])
   # --- Tab 9: Call Summarizer ---
    with tab9:
        st.title("⏱️ Tab 9: Call Summarizer")
        enable_ai = st.checkbox("🤖 Enable AI Analysis (Groq)", value=True, help="Generate AI-powered insights and recommendations")
        detailed_report = st.checkbox("📝 Generate Detailed Report (with Call Type Clustering)")
        hsbc_mode = st.checkbox("🏦 Enable HSBC Mode")
        uploaded_file2 = st.file_uploader("📥 Upload Excel File for Call Summarizer", type=["xlsx"], key="call_summarizer", accept_multiple_files=true)

        if uploaded_file2 is not None:
            try:
                if hsbc_mode:
                    df_hsbc = pd.read_excel(uploaded_file2, engine="openpyxl")

                    # KEEP RAW COPY FOR TOTAL CALLS + UNIQUE ACCOUNT COUNT
                    df_hsbc_raw = df_hsbc.copy()

                    # ✅ 1. Make column names unique
                    def make_unique_columns(columns):
                        seen = {}
                        unique_cols = []
                        for col in columns:
                            if col in seen:
                                seen[col] += 1
                                unique_cols.append(f"{col}_{seen[col]}")
                            else:
                                seen[col] = 0
                                unique_cols.append(col)
                        return unique_cols

                    df_hsbc.columns = make_unique_columns(df_hsbc.columns)

                    # ✅ 2. Validate essential columns
                    required_cols = ["User", "Duration of the Call", "Disposition Class", "Call Made Date", "Acct Number"]
                    missing = [col for col in required_cols if col not in df_hsbc.columns]
                    if missing:
                        st.error(f"❌ Missing required columns for HSBC mode: {missing}")
                        st.write("🧾 Columns found in file:", list(df_hsbc.columns))
                        st.stop()

                    # ---------- CLEANING DATA (FOR TALK TIME PURPOSES ONLY!) ----------
                    df_hsbc_clean = df_hsbc.dropna(subset=["User", "Duration of the Call", "Disposition Class", "Call Made Date"])

                    # Remove zero-talk only for TALK TIME MATH
                    df_hsbc_clean = df_hsbc_clean[df_hsbc_clean["Duration of the Call"].astype(str) != "00:00:00"]

                    if len(df_hsbc_clean) == 0:
                        st.warning("⚠️ No valid talk-time data found after filtering.")
                        st.stop()

                    # ---------- STANDARDIZE COLUMN NAMES ----------
                    df_hsbc_clean = df_hsbc_clean.rename(columns={
                        "User": "Collector",
                        "Duration of the Call": "Total Talk Time"
                    })

                    df_hsbc_raw = df_hsbc_raw.rename(columns={
                        "User": "Collector",
                        "Duration of the Call": "Total Talk Time"
                    })

                    # ---------- SAFE DURATION PARSER ----------
                    def convert_duration(duration):
                        if pd.isna(duration):
                            return pd.Timedelta(seconds=0)

                        # If Excel exported time as a float (fraction of a day)
                        if isinstance(duration, (int, float)):
                            return pd.to_timedelta(duration, unit="d")

                        try:
                            parts = str(duration).split(':')
                            if len(parts) == 3:
                                h, m, s = map(int, parts)
                                return pd.Timedelta(hours=h, minutes=m, seconds=s)
                        except:
                            pass

                        return pd.Timedelta(seconds=0)

                    df_hsbc_clean["Total Talk Time"] = df_hsbc_clean["Total Talk Time"].apply(convert_duration)
                    df_hsbc_clean["Call Made Date"] = pd.to_datetime(df_hsbc_clean["Call Made Date"], errors="coerce")
                    df_hsbc_clean = df_hsbc_clean.dropna(subset=["Total Talk Time", "Call Made Date"])

                    # ---------- GROUPING (TALK TIME ONLY) ----------
                    total_talk = (
                        df_hsbc_clean.groupby("Collector", as_index=False)["Total Talk Time"]
                        .sum()
                    )

                    df_daily = (
                        df_hsbc_clean.groupby(["Collector", "Call Made Date"], as_index=False)["Total Talk Time"]
                        .sum()
                    )

                    avg_daily = (
                        df_daily.groupby("Collector", as_index=False)["Total Talk Time"]
                        .mean()
                        .rename(columns={"Total Talk Time": "Average Daily Talk Time"})
                    )

                    # ---------- TOTAL CALLS (INCLUDES ZERO DURATION!) ----------
                    total_calls = (
                        df_hsbc_raw.groupby("Collector", as_index=False)["Disposition Class"]
                        .count()
                        .rename(columns={"Disposition Class": "Total Calls"})
                    )

                    # ---------- MERGE TALK TIME + TOTAL CALLS ----------
                    summary_hsbc = (
                        total_talk
                        .merge(total_calls, on="Collector", how="outer")
                        .merge(avg_daily, on="Collector", how="outer")
                    )

                    # Format timedelta to HH:MM:SS
                    def fmt(td):
                        if pd.isna(td):
                            return "00:00:00"
                        s = int(td.total_seconds())
                        return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

                    summary_hsbc["Total Talk Time (HH:MM:SS)"] = summary_hsbc["Total Talk Time"].apply(fmt)
                    summary_hsbc["Average Daily Talk Time (HH:MM:SS)"] = summary_hsbc["Average Daily Talk Time"].apply(fmt)

                    # ---------- VISUALS ----------
                    st.subheader("🏦 HSBC Mode Summary")

                    if not summary_hsbc.empty:
                        top3 = summary_hsbc.sort_values("Total Talk Time", ascending=False).head(3)
                        cols = st.columns(len(top3))
                        for idx, (_, row) in enumerate(top3.iterrows()):
                            cols[idx].metric(
                                label=row["Collector"],
                                value=f"🕒 {row['Total Talk Time (HH:MM:SS)']}",
                                delta=f"{int(row['Total Calls'])} calls" if pd.notna(row["Total Calls"]) else "0 calls"
                            )

                    st.subheader("📞 Total Calls per Collector (HSBC Mode)")
                    st.plotly_chart(px.bar(summary_hsbc, x="Collector", y="Total Calls", text="Total Calls"), use_container_width=True)

                    st.subheader("🕒 Total Talk Time per Collector (HSBC Mode)")
                    st.plotly_chart(px.bar(summary_hsbc, x="Collector", y="Total Talk Time", text="Total Talk Time (HH:MM:SS)"), use_container_width=True)

                    st.subheader("📅 Average Daily Talk Time per Collector (HSBC Mode)")
                    st.plotly_chart(px.bar(summary_hsbc, x="Collector", y="Average Daily Talk Time", text="Average Daily Talk Time (HH:MM:SS)"), use_container_width=True)

                    # ---------- OVERALL KPIs ----------
                    overall_total_calls = df_hsbc_raw.shape[0]  # includes zero duration
                    overall_unique_accounts = df_hsbc_raw["Acct Number"].dropna().nunique()

                    # CONNECTED KPI (unique accounts only, no duration filtering)
                    if "System Disposition" in df_hsbc_raw.columns and "Acct Number" in df_hsbc_raw.columns:

                        connected_unique_accounts = (
                            df_hsbc_raw[df_hsbc_raw["System Disposition"].astype(str).str.upper() == "CONNECTED"]
                            ["Acct Number"]
                            .dropna()
                            .nunique()
                        )

                    else:
                        connected_unique_accounts = 0
                        st.warning("⚠️ Missing 'System Disposition' or 'Acct Number' column.")


                    # Safe sum of talk time
                    summary_hsbc["Total Talk Time"] = summary_hsbc["Total Talk Time"].fillna(pd.Timedelta(seconds=0))
                    overall_talk_seconds = summary_hsbc["Total Talk Time"].dt.total_seconds().sum()

                    def fmt_total(seconds):
                        seconds = int(seconds)
                        return f"{seconds//3600:02d}:{(seconds%3600)//60:02d}:{seconds%60:02d}"

                    overall_talk_formatted = fmt_total(overall_talk_seconds)

                    # Display KPIs
                    st.subheader("📊 Overall Performance KPIs (HSBC Mode)")
                    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

                    with kpi1:
                        st.metric("📞 Total Calls (All Collectors)", f"{overall_total_calls:,}")

                    with kpi2:
                        st.metric("🕒 Total Talk Time (All Collectors)", overall_talk_formatted)

                    with kpi3:
                        st.metric("💼 Total Work Accounts", f"{overall_unique_accounts:,}")

                    with kpi4:
                        st.metric("📡 CONNECTED Unique Accounts", f"{connected_unique_accounts:,}")



                    # ---------- AI Analysis ----------
                    if enable_ai:
                        st.divider()
                        st.subheader("🤖 AI-Powered Analysis")
                        with st.spinner("Generating intelligent insights..."):
                            ai_analysis = generate_ai_analysis(summary_hsbc, mode="hsbc")
                            st.markdown(ai_analysis)
                        st.download_button(
                            label="📥 Download AI Analysis Report",
                            data=ai_analysis,
                            file_name="hsbc_ai_analysis.txt",
                            mime="text/plain"
                        )

                    st.stop()

                # ---------------------------
                # --- NORMAL MODE LOGIC -----
                # ---------------------------
                df_talk = pd.read_excel(uploaded_file2, engine="openpyxl")
                
                st.write("🔍 Debug: Columns found in your file:", list(df_talk.columns))
                
                # Check for required columns to calculate talk time
                # We need: Collector (or User), Date, and Duration of the Call
                
                # Find Collector column
                collector_col = None
                for col in ["Collector", "User", "Agent", "Agent Name"]:
                    if col in df_talk.columns:
                        collector_col = col
                        break
                
                if not collector_col:
                    st.error("❌ Could not find collector column (tried: Collector, User, Agent, Agent Name)")
                    st.stop()
                
                # Find Date column
                date_col = None
                for col in ["Date", "Call Made Date", "Call Date", "Date of Call"]:
                    if col in df_talk.columns:
                        date_col = col
                        break
                
                if not date_col:
                    st.error("❌ Could not find date column (tried: Date, Call Made Date, Call Date, Date of Call)")
                    st.stop()
                
                # Find Duration column
                duration_col = None
                for col in ["Duration of the Call", "Duration", "Call Duration", "Talk Time"]:
                    if col in df_talk.columns:
                        duration_col = col
                        break
                
                if not duration_col:
                    st.error("❌ Could not find duration column (tried: Duration of the Call, Duration, Call Duration, Talk Time)")
                    st.stop()
                
                # Standardize column names
                df_talk = df_talk.rename(columns={
                    collector_col: "Collector",
                    date_col: "Call Made Date",
                    duration_col: "Duration of the Call"
                })
                
                # Clean and process data
                df_talk = df_talk.dropna(subset=["Collector", "Duration of the Call", "Call Made Date"])
                
                # Convert duration to timedelta
                def convert_duration(duration):
                    if pd.isna(duration):
                        return pd.Timedelta(seconds=0)
                    
                    # If Excel exported time as a float (fraction of a day)
                    if isinstance(duration, (int, float)):
                        return pd.to_timedelta(duration, unit="d")
                    
                    try:
                        parts = str(duration).split(':')
                        if len(parts) == 3:
                            h, m, s = map(int, parts)
                            return pd.Timedelta(hours=h, minutes=m, seconds=s)
                    except:
                        pass
                    
                    return pd.Timedelta(seconds=0)
                
                df_talk["Total Talk Time"] = df_talk["Duration of the Call"].apply(convert_duration)
                df_talk["Call Made Date"] = pd.to_datetime(df_talk["Call Made Date"], errors="coerce")
                
                # Remove invalid entries
                df_talk = df_talk.dropna(subset=["Total Talk Time", "Call Made Date"])
                df_talk = df_talk[df_talk["Total Talk Time"] > pd.Timedelta(seconds=0)]

                if len(df_talk) == 0:
                    st.warning("⚠️ No valid data found after filtering. Please check your data format.")
                    st.stop()

                # Calculate metrics
                total_calls = (
                    df_talk.groupby("Collector", as_index=False).size().rename(columns={"size": "Total Calls"})
                )
                total_talk = (
                    df_talk.groupby("Collector", as_index=False)["Total Talk Time"].sum()
                )
                df_daily = (
                    df_talk.groupby(["Collector", "Call Made Date"], as_index=False)["Total Talk Time"].sum()
                )
                avg_daily = (
                    df_daily.groupby("Collector", as_index=False)["Total Talk Time"]
                    .mean()
                    .rename(columns={"Total Talk Time": "Average Daily Talk Time"})
                )

                summary_normal = total_talk.merge(total_calls, on="Collector").merge(avg_daily, on="Collector")

                def fmt(td):
                    if pd.isna(td):
                        return "00:00:00"
                    s = int(td.total_seconds())
                    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

                summary_normal["Total Talk Time (HH:MM:SS)"] = summary_normal["Total Talk Time"].apply(fmt)
                summary_normal["Average Daily Talk Time (HH:MM:SS)"] = summary_normal["Average Daily Talk Time"].apply(fmt)

                # ---- Visualization ----
                st.subheader("📊 Normal Mode Summary")

                if not summary_normal.empty:
                    top3 = summary_normal.sort_values("Total Talk Time", ascending=False).head(3)
                    cols = st.columns(len(top3))
                    # FIX: Use enumerate to get proper index
                    for idx, (_, row) in enumerate(top3.iterrows()):
                        cols[idx].metric(
                            label=row["Collector"],
                            value=f"🕒 {row['Total Talk Time (HH:MM:SS)']}",
                            delta=f"{row['Total Calls']} calls"
                        )

                st.subheader("📞 Total Calls per Collector")
                st.plotly_chart(px.bar(summary_normal, x="Collector", y="Total Calls", text="Total Calls"), use_container_width=True)

                st.subheader("🕒 Total Talk Time per Collector")
                st.plotly_chart(px.bar(summary_normal, x="Collector", y="Total Talk Time", text="Total Talk Time (HH:MM:SS)"), use_container_width=True)

                st.subheader("📅 Average Daily Talk Time per Collector")
                st.plotly_chart(px.bar(summary_normal, x="Collector", y="Average Daily Talk Time", text="Average Daily Talk Time (HH:MM:SS)"), use_container_width=True)

                with st.expander("📋 Detailed Table", expanded=False):
                    st.dataframe(summary_normal[["Collector", "Total Calls", "Total Talk Time (HH:MM:SS)", "Average Daily Talk Time (HH:MM:SS)"]])
                
                # Download button for normal mode summary
                csv_summary_normal = summary_normal[["Collector", "Total Calls", "Total Talk Time (HH:MM:SS)", "Average Daily Talk Time (HH:MM:SS)"]].to_csv(index=False)
                st.download_button(
                    label="📥 Download Summary Report (CSV)",
                    data=csv_summary_normal,
                    file_name="call_center_summary_report.csv",
                    mime="text/csv"
                )
                
                # --- AI Analysis ---
                if enable_ai:
                    st.divider()
                    st.subheader("🤖 AI-Powered Analysis")
                    with st.spinner("Generating intelligent insights..."):
                        ai_analysis = generate_ai_analysis(summary_normal, mode="normal")
                        st.markdown(ai_analysis)
                    st.download_button(
                        label="📥 Download AI Analysis Report",
                        data=ai_analysis,
                        file_name="call_center_ai_analysis.txt",
                        mime="text/plain"
                    )

            except Exception as e:
                st.error(f"❌ Error processing Call Summarizer: {e}")
                st.info("Please make sure your Excel file contains the correct columns for the selected mode.")
                st.write("Debug info - Columns found in your file:", list(pd.read_excel(uploaded_file2, engine="openpyxl").columns))
        else:
            st.info("⚠️ Please upload an Excel file for Call Summarizer.")
    
    # --- Tab 10: Daily Performance Analyzer ---
    with tab10:
        st.title("📞 Tab 10: Daily Performance Analyzer")
        uploaded_file3 = st.file_uploader("📥 Upload Excel File for Daily Collector Performance Analyzer", type=["xlsx"], key="collector_perf")

        if uploaded_file3 is not None:
            try:
                df_perf = pd.read_excel(uploaded_file3, engine="openpyxl")

                required_cols_perf = [
                    "Collector", "Access", "Branch", "First Login Time", "Last Logout Time", "Total Login Time",
                    "No. Login", "No. Calls", "No. Acc.", "Before 8AM", "8AM - 9AM", "9AM - 10AM", "10AM - 11AM",
                    "11AM - 12PM", "12PM - 1PM", "1PM - 2PM", "2PM - 3PM", "3PM - 4PM", "4PM - 5PM", "5PM - 6PM",
                    "6PM - 7PM", "7PM - 8PM", "After 8PM", "Talk Time"
                ]
                missing = [c for c in required_cols_perf if c not in df_perf.columns]
                if missing:
                    st.error(f"❌ Missing required columns: {missing}")
                    st.stop()

                st.subheader("📈 Collector Performance Summary")
                st.dataframe(df_perf)

                # --- Key Metrics ---
                st.markdown("### Key Metrics")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Collectors", df_perf["Collector"].nunique())
                col2.metric("Total Calls", int(df_perf["No. Calls"].sum()))

                total_talk_time = pd.to_timedelta(df_perf["Talk Time"], errors="coerce").sum()

                def format_td(td):
                    if pd.isna(td):
                        return "00:00:00"
                    total_seconds = int(td.total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                col3.metric("Total Talk Time (HH:MM:SS)", format_td(total_talk_time))

                st.divider()

                # ==================== AI INSIGHTS SECTION ====================
                # Use your existing get_groq_client() function
                groq_client = get_groq_client()
                
                if groq_client is not None:
                    st.markdown("### 🤖 AI Performance Insights")
                    st.markdown("*Get intelligent analysis and recommendations from AI*")
                    
                    ai_tab1, ai_tab2, ai_tab3 = st.tabs(["💬 Ask AI", "⚡ Quick Insights", "📊 Analyze Charts"])
                    
                    # Tab 1: Custom Question
                    with ai_tab1:
                        st.markdown("**Ask any question about your collector performance data:**")
                        user_question = st.text_area(
                            "Your Question:",
                            placeholder="Examples:\n• Which collectors need more training?\n• What are the peak calling hours?\n• Who are my top 3 performers?\n• What's a realistic daily target?",
                            height=100,
                            key="ai_custom_question"
                        )
                        
                        if st.button("🚀 Get AI Answer", key="ask_ai_btn", type="primary"):
                            if user_question.strip():
                                with st.spinner("🤔 AI is analyzing your data..."):
                                    try:
                                        prompt = generate_tab10_prompt(df_perf, user_query=user_question)
                                        
                                        chat_completion = groq_client.chat.completions.create(
                                            messages=[
                                                {
                                                    "role": "system",
                                                    "content": "You are a helpful data analyst who provides clear, specific, and actionable insights. Always include numbers."
                                                },
                                                {
                                                    "role": "user",
                                                    "content": prompt
                                                }
                                            ],
                                            model="llama-3.3-70b-versatile",
                                            temperature=0.7,
                                            max_tokens=2000
                                        )
                                        
                                        st.markdown("#### 💡 AI Response:")
                                        st.info(chat_completion.choices[0].message.content)
                                        
                                    except Exception as e:
                                        st.error(f"Error: {str(e)}")
                            else:
                                st.warning("⚠️ Please enter a question first!")
                    
                    # Tab 2: Quick Insights
                    with ai_tab2:
                        st.markdown("**Get instant automatic analysis:**")
                        
                        col_btn1, col_btn2, col_btn3 = st.columns(3)
                        
                        with col_btn1:
                            auto_btn = st.button("📊 Auto Analysis", key="auto_btn", use_container_width=True)
                        with col_btn2:
                            top_btn = st.button("🏆 Top Performers", key="top_btn", use_container_width=True)
                        with col_btn3:
                            rec_btn = st.button("💡 Recommendations", key="rec_btn", use_container_width=True)
                        
                        if auto_btn:
                            with st.spinner("📈 Analyzing..."):
                                try:
                                    prompt = generate_tab10_prompt(df_perf)
                                    response = groq_client.chat.completions.create(
                                        messages=[
                                            {"role": "system", "content": "You are a helpful data analyst."},
                                            {"role": "user", "content": prompt}
                                        ],
                                        model="llama-3.3-70b-versatile",
                                        temperature=0.7,
                                        max_tokens=2000
                                    )
                                    st.markdown("#### 📈 Analysis:")
                                    st.success(response.choices[0].message.content)
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                        
                        if top_btn:
                            with st.spinner("🏆 Analyzing..."):
                                try:
                                    query = "Who are the top 5 performers and what makes them successful? Include specific metrics."
                                    prompt = generate_tab10_prompt(df_perf, user_query=query)
                                    response = groq_client.chat.completions.create(
                                        messages=[
                                            {"role": "system", "content": "You are a helpful data analyst."},
                                            {"role": "user", "content": prompt}
                                        ],
                                        model="llama-3.3-70b-versatile",
                                        temperature=0.7,
                                        max_tokens=2000
                                    )
                                    st.markdown("#### 🏆 Top Performers:")
                                    st.success(response.choices[0].message.content)
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                        
                        if rec_btn:
                            with st.spinner("💡 Generating..."):
                                try:
                                    query = "Provide specific, actionable recommendations to improve team performance. Include numbers and examples."
                                    prompt = generate_tab10_prompt(df_perf, user_query=query)
                                    response = groq_client.chat.completions.create(
                                        messages=[
                                            {"role": "system", "content": "You are a helpful data analyst."},
                                            {"role": "user", "content": prompt}
                                        ],
                                        model="llama-3.3-70b-versatile",
                                        temperature=0.7,
                                        max_tokens=2000
                                    )
                                    st.markdown("#### 💡 Recommendations:")
                                    st.info(response.choices[0].message.content)
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    
                    # Tab 3: Visualization Analysis
                    with ai_tab3:
                        st.markdown("**Analyze specific charts:**")
                        
                        viz_selection = st.selectbox(
                            "Choose visualization:",
                            options=[
                                ("📅 Daily Work Account Comparison", "daily_accounts"),
                                ("⏱️ Daily Talk Time Comparison", "daily_talk_time"),
                                ("📊 Calls per Collector", "calls_comparison"),
                                ("⏰ Talk Time per Collector", "talk_time_comparison"),
                                ("🕐 Calls by Time Frame", "time_frame_analysis"),
                                ("🧑‍💼 Aggregated Summary", "agent_summary")
                            ],
                            format_func=lambda x: x[0],
                            key="viz_sel"
                        )
                        
                        extra_q = st.text_input(
                            "Additional question (optional):",
                            placeholder="e.g., What's the ideal target?",
                            key="extra_q"
                        )
                        
                        if st.button("🔍 Analyze", key="analyze_btn", type="primary"):
                            with st.spinner(f"🔍 Analyzing {viz_selection[0]}..."):
                                try:
                                    prompt = generate_tab10_prompt(
                                        df_perf, 
                                        user_query=extra_q,
                                        visualization_type=viz_selection[1]
                                    )
                                    response = groq_client.chat.completions.create(
                                        messages=[
                                            {"role": "system", "content": "You are a helpful data analyst."},
                                            {"role": "user", "content": prompt}
                                        ],
                                        model="llama-3.3-70b-versatile",
                                        temperature=0.7,
                                        max_tokens=2000
                                    )
                                    st.markdown(f"#### 📊 {viz_selection[0]}:")
                                    st.info(response.choices[0].message.content)
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    
                    st.divider()
                else:
                    st.info("💡 **Enable AI:** Add GROQ_API_KEY to your secrets to unlock AI insights. Get free key at https://console.groq.com/")
                    st.divider()
                
                # ==================== END AI SECTION ====================

                # --- Extract Date ---
                df_perf["Date_dt"] = pd.to_datetime(df_perf["First Login Time"], errors="coerce", dayfirst=True)
                df_perf["Date"] = df_perf["Date_dt"].dt.strftime("%d-%m-%Y").fillna("Unknown")

                # --- Daily Performance Grid per Agent ---
                st.markdown("### 📅 Daily Work Account Comparison per Agent")

                daily_pivot = df_perf.pivot_table(
                    index="Collector",
                    columns="Date",
                    values="No. Acc.",
                    aggfunc="sum",
                    fill_value=0
                )

                daily_pivot["Total"] = daily_pivot.sum(axis=1)
                daily_pivot = daily_pivot.sort_values("Total", ascending=False)

                try:
                    date_cols = [c for c in daily_pivot.columns if c != "Total"]
                    ordered_dates = sorted(date_cols, key=lambda x: pd.to_datetime(x, format="%d-%m-%Y", errors="coerce"))
                    daily_pivot = daily_pivot[ordered_dates + ["Total"]]
                except:
                    pass

                total_row = daily_pivot.sum(axis=0)
                total_row.name = "TOTAL"
                daily_pivot_with_total = pd.concat([daily_pivot, total_row.to_frame().T])

                st.dataframe(daily_pivot_with_total, use_container_width=True)

                st.divider()

                # --- Daily Talk Time Grid ---
                st.markdown("### ⏱️ Daily Talk Time Comparison per Agent")

                df_perf["Talk Time Seconds"] = pd.to_timedelta(df_perf["Talk Time"], errors="coerce").dt.total_seconds()

                daily_talk_pivot = df_perf.pivot_table(
                    index="Collector",
                    columns="Date",
                    values="Talk Time Seconds",
                    aggfunc="sum",
                    fill_value=0
                )

                daily_talk_pivot["Total (seconds)"] = daily_talk_pivot.sum(axis=1)
                daily_talk_pivot = daily_talk_pivot.sort_values("Total (seconds)", ascending=False)

                total_talk_row = daily_talk_pivot.sum(axis=0)
                total_talk_row.name = "TOTAL"
                daily_talk_pivot_with_total = pd.concat([daily_talk_pivot, total_talk_row.to_frame().T])

                daily_talk_display = daily_talk_pivot_with_total.copy()
                for col in daily_talk_display.columns:
                    daily_talk_display[col] = daily_talk_display[col].apply(
                        lambda x: format_td(pd.to_timedelta(x, unit="s")) if x > 0 else "00:00:00"
                    )

                st.dataframe(daily_talk_display, use_container_width=True)

                output_talk_daily = BytesIO()
                with pd.ExcelWriter(output_talk_daily, engine="openpyxl") as writer:
                    daily_talk_display.to_excel(writer, sheet_name="Daily Talk Time")
                st.download_button(
                    label="📥 Download Daily Talk Time (Excel)",
                    data=output_talk_daily.getvalue(),
                    file_name="daily_talk_time_comparison.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.divider()

                # --- Aggregated Summary ---
                agg_cols = [
                    "No. Login", "No. Calls", "No. Acc.", "Before 8AM", "8AM - 9AM", "9AM - 10AM", "10AM - 11AM",
                    "11AM - 12PM", "12PM - 1PM", "1PM - 2PM", "2PM - 3PM", "3PM - 4PM", "4PM - 5PM",
                    "5PM - 6PM", "6PM - 7PM", "7PM - 8PM", "After 8PM"
                ]

                df_perf["Talk Time Sec"] = pd.to_timedelta(df_perf["Talk Time"], errors="coerce").dt.total_seconds()
                agg_cols_with_talk = agg_cols + ["Talk Time Sec"]

                agent_summary = df_perf.groupby("Collector", as_index=False)[agg_cols_with_talk].sum()

                agent_summary["Talk Time (HH:MM:SS)"] = agent_summary["Talk Time Sec"].apply(
                    lambda x: format_td(pd.to_timedelta(x, unit="s"))
                )

                display_cols = ["Collector"] + agg_cols + ["Talk Time (HH:MM:SS)"]

                st.markdown("### 🧑‍💼 Aggregated Summary per Agent")

                total_agent_row = agent_summary[agg_cols_with_talk].sum()
                total_agent_row["Collector"] = "TOTAL"
                total_agent_row["Talk Time (HH:MM:SS)"] = format_td(pd.to_timedelta(total_agent_row["Talk Time Sec"], unit="s"))

                agent_summary_with_total = pd.concat([agent_summary, pd.DataFrame([total_agent_row])], ignore_index=True)

                st.dataframe(agent_summary_with_total[display_cols], use_container_width=True)

                output = BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    agent_summary_with_total[display_cols].to_excel(writer, index=False, sheet_name="Agent Summary")
                st.download_button(
                    label="📥 Download Summary (Excel)",
                    data=output.getvalue(),
                    file_name="aggregated_agent_summary.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                # --- Bar Charts ---
                st.markdown("### 📊 Calls per Collector")
                calls_per_collector = df_perf.groupby("Collector", as_index=False)["No. Calls"].sum()
                calls_per_collector = calls_per_collector.sort_values("No. Calls", ascending=False)

                fig_calls = px.bar(
                    calls_per_collector,
                    x="Collector",
                    y="No. Calls",
                    title="Calls per Collector",
                    text="No. Calls"
                )
                fig_calls.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_calls, use_container_width=True)

                st.markdown("### ⏰ Talk Time per Collector")
                talk_time_per_collector = df_perf.groupby("Collector", as_index=False)["Talk Time Sec"].sum()
                talk_time_per_collector = talk_time_per_collector.sort_values("Talk Time Sec", ascending=False)

                fig_talk = px.bar(
                    talk_time_per_collector,
                    x="Collector",
                    y="Talk Time Sec",
                    title="Talk Time per Collector (seconds)",
                    text="Talk Time Sec"
                )
                fig_talk.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_talk, use_container_width=True)

                st.markdown("### 🕐 Calls per Collector by Time Frame")
                time_frames = [
                    "Before 8AM", "8AM - 9AM", "9AM - 10AM", "10AM - 11AM", "11AM - 12PM",
                    "12PM - 1PM", "1PM - 2PM", "2PM - 3PM", "3PM - 4PM", "4PM - 5PM",
                    "5PM - 6PM", "6PM - 7PM", "7PM - 8PM", "After 8PM"
                ]

                calls_melted = df_perf.melt(
                    id_vars=["Collector"],
                    value_vars=time_frames,
                    var_name="Time Frame",
                    value_name="Calls"
                )

                calls_grouped = calls_melted.groupby(["Collector", "Time Frame"], as_index=False)["Calls"].sum()

                fig_calls_time = px.bar(
                    calls_grouped,
                    x="Time Frame",
                    y="Calls",
                    color="Collector",
                    barmode="group",
                    title="Calls per Collector by Time Frame",
                    text="Calls"
                )
                st.plotly_chart(fig_calls_time, use_container_width=True)

            except Exception as e:
                st.error(f"❌ Error: {e}")
                st.info("Please check your Excel file format.")
        else:
            st.info("⚠️ Please upload an Excel file for Collector Performance Analyzer.")

    # --- Tab 11: VB Result Data Summarizer (merged + prioritized statuses) ---
    with tab11:
        st.title("📊 Tab 11: VB Result Data Summarizer")
        uploaded_files_vb = st.file_uploader(
            "📥 Upload Excel File(s) for VB Result Data Summarizer",
            type=["xls", "xlsx", "csv"],
            key="voice_broadcast",
            accept_multiple_files=True
        )

        if not uploaded_files_vb:
            st.info("⚠️ Please upload one or more Excel/CSV files to generate the VB Result Summary.")
        else:
            st.header("📊 Voice Broadcast Result Data Summarizer (Merged & De-duplicated)")

            all_dfs = []
            for file in uploaded_files_vb:
                try:
                    # read depending on extension (pandas will usually auto-detect, but be explicit for csv)
                    if file.name.lower().endswith(".csv"):
                        df_vb = pd.read_csv(file)
                    else:
                        # pandas.read_excel can handle xls/xlsx here; let it choose the engine
                        df_vb = pd.read_excel(file)

                    # normalize column names (strip whitespace)
                    df_vb.columns = df_vb.columns.str.strip()

                    # append
                    all_dfs.append(df_vb)
                except Exception as e:
                    st.error(f"❌ Error reading {file.name}: {e}")

            if not all_dfs:
                st.info("No valid files could be read. Please check file types and contents.")
            else:
                merged_df = pd.concat(all_dfs, ignore_index=True)

                # Normalize column names & required columns check
                merged_df.columns = merged_df.columns.str.strip()
                required_cols = ["Debtor", "Debtor ID", "Final Dial Status"]
                if not any(col in merged_df.columns for col in ["Debtor ID", "Debtor"]):
                    st.error("❌ Neither 'Debtor ID' nor 'Debtor' found in the uploaded files. One of them is required.")
                elif "Final Dial Status" not in merged_df.columns:
                    st.error("❌ 'Final Dial Status' column not found in the uploaded files.")
                else:
                    # choose id column (prefer Debtor ID)
                    id_col = "Debtor ID" if "Debtor ID" in merged_df.columns else "Debtor"

                    # Normalize the Final Dial Status values for robust matching
                    merged_df["Final Dial Status"] = merged_df["Final Dial Status"].astype(str).str.strip()
                    merged_df["_FDS_UPPER"] = merged_df["Final Dial Status"].str.upper()

                    # Define reachable / unreachable heuristics (case-insensitive)
                    reachable_patterns = [
                        "PLAYED MESSAGE",  # covers "Played Message(PM)"
                        "(PM)",            # explicit PM marker
                        " PM",             # loose match
                        " PU",             # PU marker or simply 'PU'
                        "^PM$",            # exact PM
                        "^PU$",
                    ]
                    unreachable_patterns = [
                        "ADC/DC",
                        "AUTO BUSY",
                        "NO ANSWER",
                        "(NA)",
                        "^NA$",
                        "BUSY",
                        "FAILED",
                    ]

                    # helper functions (robust substring / token check)
                    def is_reachable_status(s: str) -> bool:
                        if not isinstance(s, str):
                            return False
                        s = s.upper()
                        if "PLAYED MESSAGE" in s or "(PM)" in s or s.strip() == "PM" or s.strip() == "PU":
                            return True
                        # also allow short/loose matches like 'PM ' or 'PU '
                        if s.strip() in {"PM", "PU"}:
                            return True
                        return False

                    def is_unreachable_status(s: str) -> bool:
                        if not isinstance(s, str):
                            return False
                        s = s.upper()
                        if "ADC" in s or "AUTO BUSY" in s or "NO ANSWER" in s or "(NA)" in s or s.strip() == "NA":
                            return True
                        # some systems use 'AutoBusy', 'Busy', 'Failed' etc.
                        if "BUSY" in s or "FAILED" in s:
                            return True
                        return False

                    # For each unique account we want to look at all statuses and prioritize reachable
                    grouped = (
                        merged_df[[id_col, "_FDS_UPPER", "Final Dial Status"]]
                        .dropna(subset=[id_col])  # require id
                        .groupby(id_col, as_index=False)
                        .agg(statuses=("Final Dial Status", lambda s: list(s.dropna().astype(str).str.strip().unique())))
                    )

                    # classify each unique account based on its list of statuses (priority: reachable -> unreachable -> other)
                    def classify_status_list(status_list):
                        # status_list is a list of status strings
                        # First test reachable
                        for st_val in status_list:
                            if is_reachable_status(st_val):
                                return "Reachable"
                        # If none reachable, test unreachable
                        for st_val in status_list:
                            if is_unreachable_status(st_val):
                                return "Unreachable"
                        return "Other"

                    grouped["Classification"] = grouped["statuses"].apply(classify_status_list)

                    # Now compute counts
                    total_tickets = grouped[id_col].nunique()
                    reachable_count = int((grouped["Classification"] == "Reachable").sum())
                    unreachable_count = int((grouped["Classification"] == "Unreachable").sum())
                    other_count = int((grouped["Classification"] == "Other").sum())

                    # show summary
                    summary_df = pd.DataFrame([{
                        "Tickets": total_tickets,
                        "Reachable (active)": reachable_count,
                        "Unreachable (inactive)": unreachable_count,
                        "Other / Unknown": other_count
                    }])

                    st.subheader("📋 Combined VB Result Summary (unique accounts, prioritized statuses)")
                    st.dataframe(summary_df)

                    # Download merged unique dataset (optional)
                    if st.checkbox("Download merged unique accounts with classification", value=False):
                        out = BytesIO()
                        export_df = grouped.copy()
                        export_df.to_excel(out, index=False, sheet_name="VB_Unique_Class")
                        downloaded = st.download_button(
                            "📥 Download merged_unique_vb.xlsx",
                            data=out.getvalue(),
                            file_name="merged_unique_vb.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        if downloaded:
                            st.balloons()

                    # --- Download button for Reachable Debtor IDs ---
                    reachable_ids = grouped.loc[grouped["Classification"] == "Reachable", id_col]

                    if not reachable_ids.empty:
                        out_ids = BytesIO()
                        reachable_ids_df = pd.DataFrame({id_col: reachable_ids})
                        reachable_ids_df.to_excel(out_ids, index=False, sheet_name="Reachable_IDs")

                        downloaded_ids = st.download_button(
                            "📥 Download Reachable Debtor IDs",
                            data=out_ids.getvalue(),
                            file_name="reachable_debtor_ids.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        if downloaded_ids:
                                st.balloons()
   # --- Tab 12: DSU Report Generator ---
    with tab12:
        st.title("📋 Tab 12: DSU Report Generator")

        # Initialize session state for processed data and report date if not already set
        if 'processed_data_dsu' not in st.session_state:
            st.session_state.processed_data_dsu = None
        if 'report_date_dsu' not in st.session_state:
            st.session_state.report_date_dsu = None
        if 'agent_data_dsu' not in st.session_state:
            st.session_state.agent_data_dsu = None
        if 'cycle_data_dsu' not in st.session_state:
            st.session_state.cycle_data_dsu = None
        if 'raw_data_dsu' not in st.session_state:
            st.session_state.raw_data_dsu = None

        # Helper: Convert time string (HH:MM:SS) to total seconds
        def parse_time_to_seconds(time_string):
            if pd.isna(time_string) or str(time_string).strip() == '':
                return 0
            parts = str(time_string).strip().split(':')
            if len(parts) != 3:
                return 0
            try:
                h, m, s = map(int, parts)
                return h * 3600 + m * 60 + s
            except:
                return 0

        # Helper: Convert total seconds to time string (H:MM:SS)
        def format_seconds_to_time(total_seconds):
            try:
                total_seconds = int(total_seconds or 0)
            except:
                total_seconds = 0
            if total_seconds == 0:
                return '0:00:00'
            h = total_seconds // 3600
            m = (total_seconds % 3600) // 60
            s = total_seconds % 60
            return f"{h}:{m:02d}:{s:02d}"

        # Helper: Robust amount parsing (returns float >= 0)
        def parse_amount_to_float(val):
            """
            Parse a value that might be formatted like:
            12345, 12,345.67, ₱12,345.67, (12,345.67), '-', ''
            Returns float (0.0 when value is empty / non-parsable).
            """
            if pd.isna(val):
                return 0.0
            s = str(val).strip()
            if s == '' or s.lower() in ['nan', 'none', '-']:
                return 0.0
            # remove currency symbol and commas
            s = s.replace('₱', '').replace(',', '').replace(' ', '')
            # parentheses mean negative; convert "(123)" -> -123
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            try:
                num = float(s)
                if np.isnan(num):
                    return 0.0
                return float(num)
            except:
                # fallback to pandas robust coercion
                num = pd.to_numeric(s, errors='coerce')
                if pd.isna(num):
                    return 0.0
                return float(num)

        # New function: Process agent-level data
        def process_agent_data(df):
            """Process data to create agent-level summary"""
            df.columns = df.columns.str.strip()
            
            agent_summary = {}
            
            for _, row in df.iterrows():
                collector = row.get('Collector', '')
                if pd.isna(collector) or str(collector).strip() == '':
                    continue
                    
                collector = str(collector).strip()
                product_type = row.get('Product Type', 'Unknown')
                if pd.isna(product_type):
                    product_type = 'Unknown'
                product_type = str(product_type).strip()
                
                key = (collector, product_type)
                
                if key not in agent_summary:
                    agent_summary[key] = {
                        'ptp_count': 0,
                        'ptp_amount': 0.0,
                        'ptp_balance': 0.0,
                        'payment_count': 0,
                        'payment_amount': 0.0,
                        'payment_balance': 0.0,
                    }
                
                grp = agent_summary[key]
                
                # PTP processing
                ptp_raw = row.get('PTP Amount', None)
                ptp_num = parse_amount_to_float(ptp_raw)
                if ptp_num > 0:
                    grp['ptp_count'] += 1
                    grp['ptp_amount'] += ptp_num
                    balance_val = row.get('Balance', None)
                    if balance_val is not None:
                        grp['ptp_balance'] += parse_amount_to_float(balance_val)
                
                # Payment processing
                claim_raw = row.get('Claim Paid Amount', None)
                claim_num = parse_amount_to_float(claim_raw)
                if claim_num > 0:
                    grp['payment_count'] += 1
                    grp['payment_amount'] += claim_num
                    balance_val = row.get('Balance', None)
                    if balance_val is not None:
                        grp['payment_balance'] += parse_amount_to_float(balance_val)
            
            # Convert to DataFrame
            agent_data = []
            for (collector, product_type), grp in agent_summary.items():
                agent_data.append({
                    'Agent': collector,
                    'Product Type': product_type,
                    'PTP Count': grp['ptp_count'],
                    'PTP Amount': grp['ptp_amount'],
                    'PTP Balance': grp['ptp_balance'],
                    'Payment Count': grp['payment_count'],
                    'Payment Amount': grp['payment_amount'],
                    'Payment Balance': grp['payment_balance'],
                })
            
            return pd.DataFrame(agent_data)

        # New function: Process cycle-level data
        def process_cycle_data(df):
            """Process data to create cycle-level summary"""
            df.columns = df.columns.str.strip()
            
            cycle_summary = {}
            
            for _, row in df.iterrows():
                cycle = row.get('Cycle', '')
                if pd.isna(cycle) or str(cycle).strip() == '':
                    cycle = 'Unknown'
                else:
                    cycle = str(cycle).strip()
                    
                product_type = row.get('Product Type', 'Unknown')
                if pd.isna(product_type):
                    product_type = 'Unknown'
                product_type = str(product_type).strip()
                
                key = (cycle, product_type)
                
                if key not in cycle_summary:
                    cycle_summary[key] = {
                        'ptp_count': 0,
                        'ptp_amount': 0.0,
                        'ptp_balance': 0.0,
                        'payment_count': 0,
                        'payment_amount': 0.0,
                        'payment_balance': 0.0,
                    }
                
                grp = cycle_summary[key]
                
                # PTP processing
                ptp_raw = row.get('PTP Amount', None)
                ptp_num = parse_amount_to_float(ptp_raw)
                if ptp_num > 0:
                    grp['ptp_count'] += 1
                    grp['ptp_amount'] += ptp_num
                    balance_val = row.get('Balance', None)
                    if balance_val is not None:
                        grp['ptp_balance'] += parse_amount_to_float(balance_val)
                
                # Payment processing
                claim_raw = row.get('Claim Paid Amount', None)
                claim_num = parse_amount_to_float(claim_raw)
                if claim_num > 0:
                    grp['payment_count'] += 1
                    grp['payment_amount'] += claim_num
                    balance_val = row.get('Balance', None)
                    if balance_val is not None:
                        grp['payment_balance'] += parse_amount_to_float(balance_val)
            
            # Convert to DataFrame
            cycle_data = []
            for (cycle, product_type), grp in cycle_summary.items():
                cycle_data.append({
                    'Cycle': cycle,
                    'Product Type': product_type,
                    'PTP Count': grp['ptp_count'],
                    'PTP Amount': grp['ptp_amount'],
                    'PTP Balance': grp['ptp_balance'],
                    'Payment Count': grp['payment_count'],
                    'Payment Amount': grp['payment_amount'],
                    'Payment Balance': grp['payment_balance'],
                })
            
            return pd.DataFrame(cycle_data)

        # Main processing function for the uploaded DRR file
        def process_data(df, progress_bar, status_text):
            # Clean column names
            df.columns = df.columns.str.strip()
            status_text.text("🔄 Cleaning column names...")
            progress_bar.progress(5)

            # Rename columns to standard names
            column_mapping = {
                'Client': ['Client', 'client', 'Campaign'],
                'Date': ['Date', 'date'],
                'Account No.': ['Account No.', 'Account No', 'account_no'],
                'Status': ['Status', 'status'],
                'Remark Type': ['Remark Type', 'remark_type'],
                'Call Status': ['Call Status', 'call_status'],
                'PTP Amount': ['PTP Amount', 'ptp_amount'],
                'Claim Paid Amount': ['Claim Paid Amount', 'claim_paid_amount'],
                'Talk Time Duration': ['Talk Time Duration', 'talk_time_duration'],
                'Collector': ['Collector', 'collector', 'Agent', 'agent'],
                'Remark By': ['Remark By', 'remark_by'],
                'Balance': ['Balance', 'balance']
            }
            for standard, options in column_mapping.items():
                for col in df.columns:
                    if col in options:
                        df.rename(columns={col: standard}, inplace=True)
                        break
            status_text.text("📅 Extracting report date...")
            progress_bar.progress(20)

            # Extract report date from first non-null 'Date' value (if any)
            report_date = None
            if 'Date' in df.columns and len(df) > 0:
                first_dates = df['Date'].dropna().astype(str).tolist()
                if len(first_dates) > 0:
                    date_str = first_dates[0]
                    try:
                        if isinstance(date_str, str) and '/' in date_str:
                            d, m, y = map(int, date_str.split('/'))
                            report_date = datetime(y, m, d)
                        else:
                            report_date = pd.to_datetime(date_str, errors='coerce')
                    except:
                        report_date = None
            st.session_state.report_date_dsu = report_date
            status_text.text("📊 Grouping campaign data...")
            progress_bar.progress(35)

            # Group data by campaign/client and aggregate metrics
            campaign_groups = {}
            for _, row in df.iterrows():
                client = row.get('Client', '')
                if pd.isna(client) or str(client).strip() == '':
                    continue
                client = str(client)
                if client not in campaign_groups:
                    campaign_groups[client] = {
                        'records': [],
                        'unique_accounts': set(),
                        'unique_ptp_ids': set(),
                        'balance_with_ptp': 0.0,
                        'connected_count': 0,
                        'connected_unique_accounts': set(),
                        'dials_count': 0,
                        'skip_tracing_count': 0,
                        'predictive_talk_time': 0,
                        'non_predictive_talk_time': 0,
                        'non_predictive_disposition': 0,
                        'predictive_agents_with_talktime': set(),
                        'non_predictive_agents_with_talktime': set(),
                        'all_unique_agents': set(),
                        'ptp_amount': 0.0,
                        'ptp_count': 0,
                        'claim_paid_amount': 0.0,
                        'claim_paid_count': 0,
                    }

                grp = campaign_groups[client]
                grp['records'].append(row)

                # --- Unique Debtor ID for PTP sum ---
                debtor_id = row.get('Debtor ID', None)
                if debtor_id is None or str(debtor_id).strip() == '':
                    debtor_id = row.get('Account No.', None)
                debtor_id = str(debtor_id).strip() if debtor_id is not None else None

                ptp_raw = row.get('PTP Amount', None)
                ptp_num = parse_amount_to_float(ptp_raw)
                balance_val = row.get('Balance', None)
                # Only add if PTP > 0, Debtor ID is present, and not already counted
                if ptp_num > 0 and debtor_id and debtor_id not in grp['unique_ptp_ids']:
                    if balance_val is not None and str(balance_val).strip() != '':
                        try:
                            bal = float(str(balance_val).replace(',', '').replace('₱', '').replace(' ', ''))
                            if not np.isnan(bal):
                                grp['balance_with_ptp'] += bal
                                grp['unique_ptp_ids'].add(debtor_id)
                        except:
                            pass

                # Track unique accounts by Account No.
                acc = row.get('Account No.', '')
                if pd.notna(acc) and str(acc).strip() != '':
                    grp['unique_accounts'].add(str(acc).strip())

                # Skip tracing detection
                status = row.get('Status', '')
                if pd.notna(status) and 'skip' in str(status).lower():
                    grp['skip_tracing_count'] += 1

                # Count dials (exclude incoming/sms remark types)
                remark_type = row.get('Remark Type', '')
                excluded = ['incoming', 'sms']
                if pd.notna(acc) and str(acc).strip() != '':
                    if not (pd.notna(remark_type) and str(remark_type).lower().strip() in excluded):
                        grp['dials_count'] += 1

                # Connected detection (call status contains 'connected')
                call_status = row.get('Call Status', '')
                if pd.notna(call_status) and 'connected' in str(call_status).lower():
                    grp['connected_count'] += 1
                    if pd.notna(acc) and str(acc).strip() != '':
                        grp['connected_unique_accounts'].add(str(acc).strip())

                # ---- PTP / Claim Paid: parse robustly and sum as numeric floats ----
                ptp_raw = row.get('PTP Amount', None)
                ptp_num = parse_amount_to_float(ptp_raw)
                grp['ptp_amount'] += ptp_num
                if ptp_num > 0:
                    grp['ptp_count'] += 1

                claim_raw = row.get('Claim Paid Amount', None)
                claim_num = parse_amount_to_float(claim_raw)
                grp['claim_paid_amount'] += claim_num
                if claim_num > 0:
                    grp['claim_paid_count'] += 1

                # Talk time & agent tracking
                talk_sec = parse_time_to_seconds(row.get('Talk Time Duration', '0:00:00'))
                collector = row.get('Collector', '')
                if pd.notna(collector) and str(collector).strip() != '':
                    grp['all_unique_agents'].add(str(collector).strip())

                # Distinguish predictive vs manual talk time & dispositions
                if pd.notna(remark_type):
                    if str(remark_type).lower().strip() == 'predictive':
                        grp['predictive_talk_time'] += talk_sec
                        if talk_sec > 0 and pd.notna(collector) and str(collector).strip() != '':
                            grp['predictive_agents_with_talktime'].add(str(collector).strip())
                    else:
                        grp['non_predictive_talk_time'] += talk_sec
                        if talk_sec > 0 and pd.notna(collector) and str(collector).strip() != '':
                            grp['non_predictive_agents_with_talktime'].add(str(collector).strip())

                        remark_by = row.get('Remark By', '')
                        excluded_by = ['system','spmadrid','daduran','etaquino','jvagustinjr','pasantos','jvagustijr']
                        excluded_type = ['incoming','predictive']
                        rb = str(remark_by).lower().strip() if pd.notna(remark_by) else ''
                        rt = str(remark_type).lower().strip() if pd.notna(remark_type) else ''
                        if rb and (rb not in excluded_by) and rt and (rt not in excluded_type):
                            grp['non_predictive_disposition'] += 1

            status_text.text("⚙️ Finalizing calculations...")
            progress_bar.progress(70)

            # Build the final summary DataFrame for all campaigns
            processed = []
            for client, grp in campaign_groups.items():
                pred_agents = len(grp['predictive_agents_with_talktime']) or 1
                non_pred_agents = len(grp['non_predictive_agents_with_talktime']) or 1
                total_agents = len(grp['all_unique_agents']) or 1

                avg_pred = grp['predictive_talk_time'] / pred_agents
                avg_nonpred = grp['non_predictive_talk_time'] / non_pred_agents
                avg_manual = round(grp['non_predictive_disposition'] / total_agents)

                penetration = 'N/A'
                if grp['dials_count'] > 0 and len(grp['unique_accounts']) > 0:
                    penetration_value = (grp['dials_count'] / len(grp['unique_accounts'])) * 100
                    penetration = f"{penetration_value:.2f}%"

                connected_rate = 'N/A'
                if len(grp['unique_accounts']) > 0:
                    connected_rate_value = (len(grp['connected_unique_accounts']) / len(grp['unique_accounts'])) * 100
                    connected_rate = f"{connected_rate_value:.2f}%"

                ptp_amt_val = float(grp['ptp_amount'] or 0.0)
                claim_amt_val = float(grp['claim_paid_amount'] or 0.0)
                balance_with_ptp_val = float(grp['balance_with_ptp'] or 0.0)

                processed.append({
                    'Campaign': client,
                    'Total Worked On Tickets': f"{len(grp['unique_accounts']):,}",
                    'Dials': f"{grp['dials_count']:,}",
                    'Connected': f"{grp['connected_count']:,}",
                    'Connected Unique': f"{len(grp['connected_unique_accounts']):,}",
                    'Penetration': penetration,
                    'Connected Rate': connected_rate,
                    'Skip Tracing Effort': f"{grp['skip_tracing_count']:,}",
                    'Average Talktime Per Agent (PREDICTIVE)': format_seconds_to_time(avg_pred),
                    'Average Talktime Per Agent (MANUAL)': format_seconds_to_time(avg_nonpred),
                    'Average Manual Disposition': str(avg_manual),
                    'PTP Amount': f"₱{ptp_amt_val:,.2f}",
                    'PTP Count': f"{grp['ptp_count']:,}",
                    'Claim Paid Amount': f"₱{claim_amt_val:,.2f}",
                    'Claim Paid Count': f"{grp['claim_paid_count']:,}",
                    'Balance with PTP > 0 (Unique Debtor ID)': f"₱{balance_with_ptp_val:,.2f}",
                })

            status_text.text("✅ Report ready!")
            progress_bar.progress(100)
            # return DataFrame
            return pd.DataFrame(processed)

        # Helper: Create Excel report from processed DataFrame
        def create_excel_report(df, report_date):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_transposed = df.set_index('Campaign').T
                df_transposed.reset_index(inplace=True)
                df_transposed.rename(columns={'index': 'Campaigns'}, inplace=True)
                df_transposed.to_excel(writer, sheet_name='Daily Productivity Report', index=False)
            output.seek(0)
            return output

        # File uploader for DRR file (unique key)
        uploaded_file = st.file_uploader(
            "📥 Upload your DRR file",
            type=['csv','xlsx','xls','txt'],
            key="dsu_file_tab12"
        )

        # If file is uploaded, process on button click
        if uploaded_file is not None:
            if st.button("Generate Report"):
                try:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    # Read file based on extension
                    if uploaded_file.name.endswith(('.csv','.txt')):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)

                    # Store raw data for agent processing
                    st.session_state.raw_data_dsu = df.copy()
                    
                    processed_df = process_data(df, progress_bar, status_text)
                    st.session_state.processed_data_dsu = processed_df
                    
                    # Process agent data
                    agent_df = process_agent_data(st.session_state.raw_data_dsu)
                    st.session_state.agent_data_dsu = agent_df
                    
                    # Process cycle data
                    cycle_df = process_cycle_data(st.session_state.raw_data_dsu)
                    st.session_state.cycle_data_dsu = cycle_df
                    
                    st.success("✅ Report generated successfully!")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

        # Show preview and download if report is ready
        if st.session_state.processed_data_dsu is not None:
            st.subheader("📋 Daily Productivity Report Preview")
            if st.session_state.report_date_dsu:
                st.write(f"Report Date: {st.session_state.report_date_dsu.strftime('%m/%d/%Y')}")
            display_df = st.session_state.processed_data_dsu.set_index('Campaign').T
            st.dataframe(display_df)

            report_date = st.session_state.report_date_dsu or datetime.now()
            date_str = report_date.strftime('%m-%d-%Y')
            excel_data = create_excel_report(st.session_state.processed_data_dsu, report_date)

            if st.download_button(
                label="📥 Download Excel Report",
                data=excel_data,
                file_name=f"Daily_Productivity_Report_{date_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ):
                st.balloons()
        
        # Agent-level data grid section
        if st.session_state.agent_data_dsu is not None:
            st.markdown("---")
            st.subheader("👤 Agent Performance Summary")
            
            agent_df = st.session_state.agent_data_dsu
            
            # Get unique product types for filter
            product_types = sorted(agent_df['Product Type'].unique().tolist())
            product_types.insert(0, 'All Products')
            
            # Product type filter dropdown
            selected_product = st.selectbox(
                "Filter by Product Type:",
                options=product_types,
                key="product_filter_dsu"
            )
            
            # Filter data based on selection
            if selected_product == 'All Products':
                filtered_df = agent_df.copy()
            else:
                filtered_df = agent_df[agent_df['Product Type'] == selected_product].copy()
            
            # Format numeric columns for display
            display_agent_df = filtered_df.copy()
            display_agent_df['PTP Amount'] = display_agent_df['PTP Amount'].apply(lambda x: f"₱{x:,.2f}")
            display_agent_df['PTP Balance'] = display_agent_df['PTP Balance'].apply(lambda x: f"₱{x:,.2f}")
            display_agent_df['Payment Amount'] = display_agent_df['Payment Amount'].apply(lambda x: f"₱{x:,.2f}")
            display_agent_df['Payment Balance'] = display_agent_df['Payment Balance'].apply(lambda x: f"₱{x:,.2f}")
            display_agent_df['PTP Count'] = display_agent_df['PTP Count'].apply(lambda x: f"{x:,}")
            display_agent_df['Payment Count'] = display_agent_df['Payment Count'].apply(lambda x: f"{x:,}")
            
            # Display the agent data grid
            st.dataframe(
                display_agent_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Summary statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                total_ptp = filtered_df['PTP Amount'].sum()
                st.metric("Total PTP Amount", f"₱{total_ptp:,.2f}")
            with col2:
                total_ptp_count = filtered_df['PTP Count'].sum()
                st.metric("Total PTP Count", f"{total_ptp_count:,}")
            with col3:
                total_ptp_balance = filtered_df['PTP Balance'].sum()
                st.metric("Total PTP Balance", f"₱{total_ptp_balance:,.2f}")
            
            col4, col5, col6 = st.columns(3)
            with col4:
                total_payment = filtered_df['Payment Amount'].sum()
                st.metric("Total Payment Amount", f"₱{total_payment:,.2f}")
            with col5:
                total_payment_count = filtered_df['Payment Count'].sum()
                st.metric("Total Payment Count", f"{total_payment_count:,}")
            with col6:
                total_payment_balance = filtered_df['Payment Balance'].sum()
                st.metric("Total Payment Balance", f"₱{total_payment_balance:,.2f}")
        
        # Cycle-level data grid section
        if st.session_state.cycle_data_dsu is not None:
            st.markdown("---")
            st.subheader("🔄 Cycle Performance Summary")
            
            cycle_df = st.session_state.cycle_data_dsu
            
            # Get unique product types for filter
            cycle_product_types = sorted(cycle_df['Product Type'].unique().tolist())
            cycle_product_types.insert(0, 'All Products')
            
            # Product type filter dropdown
            selected_cycle_product = st.selectbox(
                "Filter by Product Type:",
                options=cycle_product_types,
                key="cycle_product_filter_dsu"
            )
            
            # Filter data based on selection
            if selected_cycle_product == 'All Products':
                filtered_cycle_df = cycle_df.copy()
            else:
                filtered_cycle_df = cycle_df[cycle_df['Product Type'] == selected_cycle_product].copy()
            
            # Format numeric columns for display
            display_cycle_df = filtered_cycle_df.copy()
            display_cycle_df['PTP Amount'] = display_cycle_df['PTP Amount'].apply(lambda x: f"₱{x:,.2f}")
            display_cycle_df['PTP Balance'] = display_cycle_df['PTP Balance'].apply(lambda x: f"₱{x:,.2f}")
            display_cycle_df['Payment Amount'] = display_cycle_df['Payment Amount'].apply(lambda x: f"₱{x:,.2f}")
            display_cycle_df['Payment Balance'] = display_cycle_df['Payment Balance'].apply(lambda x: f"₱{x:,.2f}")
            display_cycle_df['PTP Count'] = display_cycle_df['PTP Count'].apply(lambda x: f"{x:,}")
            display_cycle_df['Payment Count'] = display_cycle_df['Payment Count'].apply(lambda x: f"{x:,}")
            
            # Display the cycle data grid
            st.dataframe(
                display_cycle_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Summary statistics for cycles
            col1, col2, col3 = st.columns(3)
            with col1:
                total_cycle_ptp = filtered_cycle_df['PTP Amount'].sum()
                st.metric("Total PTP Amount", f"₱{total_cycle_ptp:,.2f}")
            with col2:
                total_cycle_ptp_count = filtered_cycle_df['PTP Count'].sum()
                st.metric("Total PTP Count", f"{total_cycle_ptp_count:,}")
            with col3:
                total_cycle_ptp_balance = filtered_cycle_df['PTP Balance'].sum()
                st.metric("Total PTP Balance", f"₱{total_cycle_ptp_balance:,.2f}")
            
            col4, col5, col6 = st.columns(3)
            with col4:
                total_cycle_payment = filtered_cycle_df['Payment Amount'].sum()
                st.metric("Total Payment Amount", f"₱{total_cycle_payment:,.2f}")
            with col5:
                total_cycle_payment_count = filtered_cycle_df['Payment Count'].sum()
                st.metric("Total Payment Count", f"{total_cycle_payment_count:,}")
            with col6:
                total_cycle_payment_balance = filtered_cycle_df['Payment Balance'].sum()
                st.metric("Total Payment Balance", f"₱{total_cycle_payment_balance:,.2f}")
        
        # NEW SECTION: Individual Account Details per Cycle and Agent
        if st.session_state.raw_data_dsu is not None:
            st.markdown("---")
            st.subheader("🔍 Individual Account Details")
            
            raw_df = st.session_state.raw_data_dsu.copy()
            raw_df.columns = raw_df.columns.str.strip()
            
            # Create two columns for filters
            filter_col1, filter_col2 = st.columns(2)
            
            with filter_col1:
                # Get unique cycles
                cycles = raw_df.get('Cycle', pd.Series()).dropna().astype(str).str.strip()
                unique_cycles = sorted([c for c in cycles.unique() if c != ''])
                unique_cycles.insert(0, 'All Cycles')
                
                selected_cycle = st.selectbox(
                    "Filter by Cycle:",
                    options=unique_cycles,
                    key="account_cycle_filter_dsu"
                )
            
            with filter_col2:
                # Get unique agents/collectors
                agents = raw_df.get('Collector', pd.Series()).dropna().astype(str).str.strip()
                unique_agents = sorted([a for a in agents.unique() if a != ''])
                unique_agents.insert(0, 'All Agents')
                
                selected_agent = st.selectbox(
                    "Filter by Agent:",
                    options=unique_agents,
                    key="account_agent_filter_dsu"
                )
            
            # --- SEPARATE DATA GRID FOR PTPs ---
            st.markdown("### 💰 Accounts with PTPs")
            
            ptp_df = raw_df.copy()
            
            # Filter for PTP Amount > 0
            ptp_mask = ptp_df.get('PTP Amount', pd.Series()).apply(parse_amount_to_float) > 0
            ptp_df = ptp_df[ptp_mask]
            
            # Apply cycle filter
            if selected_cycle != 'All Cycles':
                cycle_col = ptp_df.get('Cycle', pd.Series())
                ptp_df = ptp_df[cycle_col.astype(str).str.strip() == selected_cycle]
            
            # Apply agent filter
            if selected_agent != 'All Agents':
                collector_col = ptp_df.get('Collector', pd.Series())
                ptp_df = ptp_df[collector_col.astype(str).str.strip() == selected_agent]
            
            # Select relevant columns to display
            display_columns_ptp = []
            available_cols_ptp = ptp_df.columns.tolist()
            
            priority_cols_ptp = [
                'Account No.', 'Cycle', 'Collector', 'Product Type', 
                'Status', 'Call Status', 'Remark Type',
                'PTP Amount', 'Balance', 'Date', 'Client', 'Talk Time Duration'
            ]
            
            for col in priority_cols_ptp:
                if col in available_cols_ptp:
                    display_columns_ptp.append(col)
            
            for col in available_cols_ptp:
                if col not in display_columns_ptp:
                    display_columns_ptp.append(col)
            
            ptp_display_df = ptp_df[display_columns_ptp].copy()
            
            # Format currency columns
            if 'PTP Amount' in ptp_display_df.columns:
                ptp_display_df['PTP Amount'] = ptp_display_df['PTP Amount'].apply(
                    lambda x: f"₱{parse_amount_to_float(x):,.2f}" if parse_amount_to_float(x) > 0 else ''
                )
            
            if 'Balance' in ptp_display_df.columns:
                ptp_display_df['Balance'] = ptp_display_df['Balance'].apply(
                    lambda x: f"₱{parse_amount_to_float(x):,.2f}" if pd.notna(x) else ''
                )
            
            st.write(f"**Total PTP Records:** {len(ptp_display_df):,}")
            
            st.dataframe(
                ptp_display_df,
                use_container_width=True,
                hide_index=True,
                height=400
            )
            
            # PTP Summary metrics
            st.markdown("**PTP Summary:**")
            ptp_sum_col1, ptp_sum_col2, ptp_sum_col3 = st.columns(3)
            
            with ptp_sum_col1:
                unique_ptp_accounts = ptp_df.get('Account No.', pd.Series()).dropna().nunique()
                st.metric("Unique PTP Accounts", f"{unique_ptp_accounts:,}")
            
            with ptp_sum_col2:
                total_ptp = sum(
                    parse_amount_to_float(val) 
                    for val in ptp_df.get('PTP Amount', pd.Series())
                )
                st.metric("Total PTP Amount", f"₱{total_ptp:,.2f}")
            
            with ptp_sum_col3:
                total_ptp_balance = sum(
                    parse_amount_to_float(val) 
                    for val in ptp_df.get('Balance', pd.Series())
                )
                st.metric("Total PTP Balance", f"₱{total_ptp_balance:,.2f}")
            
            # --- SEPARATE DATA GRID FOR CLAIM PAIDS ---
            st.markdown("---")
            st.markdown("### 💳 Accounts with Claim Paids")
            
            claim_df = raw_df.copy()
            
            # Filter for Claim Paid Amount > 0
            claim_mask = claim_df.get('Claim Paid Amount', pd.Series()).apply(parse_amount_to_float) > 0
            claim_df = claim_df[claim_mask]
            
            # Apply cycle filter
            if selected_cycle != 'All Cycles':
                cycle_col = claim_df.get('Cycle', pd.Series())
                claim_df = claim_df[cycle_col.astype(str).str.strip() == selected_cycle]
            
            # Apply agent filter
            if selected_agent != 'All Agents':
                collector_col = claim_df.get('Collector', pd.Series())
                claim_df = claim_df[collector_col.astype(str).str.strip() == selected_agent]
            
            # Select relevant columns to display
            display_columns_claim = []
            available_cols_claim = claim_df.columns.tolist()
            
            priority_cols_claim = [
                'Account No.', 'Cycle', 'Collector', 'Product Type', 
                'Status', 'Call Status', 'Remark Type',
                'Claim Paid Amount', 'Balance', 'Date', 'Client', 'Talk Time Duration'
            ]
            
            for col in priority_cols_claim:
                if col in available_cols_claim:
                    display_columns_claim.append(col)
            
            for col in available_cols_claim:
                if col not in display_columns_claim:
                    display_columns_claim.append(col)
            
            claim_display_df = claim_df[display_columns_claim].copy()
            
            # Format currency columns
            if 'Claim Paid Amount' in claim_display_df.columns:
                claim_display_df['Claim Paid Amount'] = claim_display_df['Claim Paid Amount'].apply(
                    lambda x: f"₱{parse_amount_to_float(x):,.2f}" if parse_amount_to_float(x) > 0 else ''
                )
            
            if 'Balance' in claim_display_df.columns:
                claim_display_df['Balance'] = claim_display_df['Balance'].apply(
                    lambda x: f"₱{parse_amount_to_float(x):,.2f}" if pd.notna(x) else ''
                )
            
            st.write(f"**Total Claim Paid Records:** {len(claim_display_df):,}")
            
            st.dataframe(
                claim_display_df,
                use_container_width=True,
                hide_index=True,
                height=400
            )
            
            # Claim Paid Summary metrics
            st.markdown("**Claim Paid Summary:**")
            claim_sum_col1, claim_sum_col2, claim_sum_col3 = st.columns(3)
            
            with claim_sum_col1:
                unique_claim_accounts = claim_df.get('Account No.', pd.Series()).dropna().nunique()
                st.metric("Unique Claim Paid Accounts", f"{unique_claim_accounts:,}")
            
            with claim_sum_col2:
                total_claim = sum(
                    parse_amount_to_float(val) 
                    for val in claim_df.get('Claim Paid Amount', pd.Series())
                )
                st.metric("Total Claim Paid Amount", f"₱{total_claim:,.2f}")
            
            with claim_sum_col3:
                total_claim_balance = sum(
                    parse_amount_to_float(val) 
                    for val in claim_df.get('Balance', pd.Series())
                )
                st.metric("Total Claim Paid Balance", f"₱{total_claim_balance:,.2f}") 
    # --- Tab 13: Claim Paid Report ---
    with tab13:
        st.title("💰 Tab 13: Claim Paid Analyzer")

        uploaded_files = st.file_uploader(  
            "📥 Upload Claim Paid Report file(s)",
            type=['csv', 'xlsx', 'xls'],
            accept_multiple_files=True,
            key="claim_paid_files"
        )

        if uploaded_files:
            df_list = []
            for file in uploaded_files:
                if file.name.endswith('.csv'):
                    temp_df = pd.read_csv(file)
                else:
                    temp_df = pd.read_excel(file)
                df_list.append(temp_df)

            # Combine all files
            df = pd.concat(df_list, ignore_index=True)

            # Clean column names
            df.columns = df.columns.str.strip()

            # Standardize important columns
            column_mapping = {
                'Account No.': ['Account No.', 'Account No', 'account_no'],
                'Client': ['Client', 'client'],
                'Claim Paid Date': ['Claim Paid Date', 'claim_paid_date'],
                'Claim Paid Amount': ['Claim Paid Amount', 'claim_paid_amount'],
                'Collector Name': ['Collector Name', 'collector name', 'Collector', 'Agent'],
                'Balance': ['Balance', 'balance', 'Outstanding Balance', 'Total Balance']
            }
            for standard, options in column_mapping.items():
                for col in df.columns:
                    if col in options:
                        df.rename(columns={col: standard}, inplace=True)
                        break

            # Convert claim paid amount to numeric
            df['Claim Paid Amount'] = pd.to_numeric(
                df['Claim Paid Amount'].astype(str).str.replace('[₱,]', '', regex=True),
                errors='coerce'
            ).fillna(0)

            # Convert balance to numeric
            if 'Balance' in df.columns:
                df['Balance'] = pd.to_numeric(
                    df['Balance'].astype(str).str.replace('[₱,]', '', regex=True),
                    errors='coerce'
                ).fillna(0)

            # Convert claim paid date to date only (strip time)
            if 'Claim Paid Date' in df.columns:
                df['Claim Paid Date'] = pd.to_datetime(
                    df['Claim Paid Date'],
                    errors='coerce',
                    dayfirst=True
                ).dt.date

            # ====================================================================
            # Section 1: Overall KPIs
            # ====================================================================
            st.subheader("📊 Overall Performance Metrics")
            
            total_claim_paid = df['Claim Paid Amount'].sum()
            unique_accounts = df['Account No.'].nunique()
            avg_claim_paid = df['Claim Paid Amount'].mean()
            
            # Calculate total balance if available
            if 'Balance' in df.columns:
                total_balance = df['Balance'].sum()
                overall_conversion = (total_claim_paid / total_balance * 100) if total_balance > 0 else 0
                
                kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                kpi1.metric("💵 Total Claim Paid", f"₱{total_claim_paid:,.2f}")
                kpi2.metric("📊 Total Balance", f"₱{total_balance:,.2f}")
                kpi3.metric("👥 Unique Accounts Paid", f"{unique_accounts:,}")
                kpi4.metric("🎯 Overall Conversion Rate", f"{overall_conversion:.2f}%")
            else:
                kpi1, kpi2, kpi3 = st.columns(3)
                kpi1.metric("💵 Total Claim Paid", f"₱{total_claim_paid:,.2f}")
                kpi2.metric("👥 Unique Accounts Paid", f"{unique_accounts:,}")
                kpi3.metric("📊 Average per Account", f"₱{avg_claim_paid:,.2f}")

            st.divider()

            # ====================================================================
            # Section 2: Collector Performance Analysis (NEW SECTION)
            # ====================================================================
            if 'Collector Name' in df.columns:
                st.subheader("👥 Collector Performance & Conversion Analysis")
                
                # Aggregate by collector
                if 'Balance' in df.columns:
                    collector_perf = df.groupby('Collector Name').agg({
                        'Claim Paid Amount': 'sum',
                        'Balance': 'sum',
                        'Account No.': 'nunique'
                    }).reset_index()
                    
                    collector_perf.columns = ['Collector Name', 'Total Claim Paid', 'Total Balance', 'Unique Accounts']
                    
                    # Calculate conversion rate
                    collector_perf['Conversion Rate (%)'] = (
                        collector_perf['Total Claim Paid'] / collector_perf['Total Balance'] * 100
                    ).fillna(0)
                    
                    # Sort by conversion rate
                    collector_perf = collector_perf.sort_values('Conversion Rate (%)', ascending=False)
                    
                    # Format for display
                    collector_perf_display = collector_perf.copy()
                    collector_perf_display['Total Claim Paid'] = collector_perf_display['Total Claim Paid'].apply(
                        lambda x: f"₱{x:,.2f}"
                    )
                    collector_perf_display['Total Balance'] = collector_perf_display['Total Balance'].apply(
                        lambda x: f"₱{x:,.2f}"
                    )
                    collector_perf_display['Conversion Rate (%)'] = collector_perf_display['Conversion Rate (%)'].apply(
                        lambda x: f"{x:.2f}%"
                    )
                    
                    # Display table
                    st.dataframe(
                        collector_perf_display,
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    st.caption("💡 Conversion Rate = (Total Claim Paid ÷ Total Balance) × 100")
                    
                    # ============================================================
                    # Subsection 2.1: Balance vs Claim Paid by Collector
                    # ============================================================
                    st.markdown("### 📊 Balance vs Claim Paid by Collector")
                    
                    # Create grouped bar chart
                    fig_balance_vs_claim = px.bar(
                        collector_perf,
                        x='Collector Name',
                        y=['Total Balance', 'Total Claim Paid'],
                        barmode='group',
                        title='Balance vs Claim Paid Comparison by Collector',
                        labels={'value': 'Amount (₱)', 'variable': 'Metric'},
                        color_discrete_map={
                            'Total Balance': '#FF6B6B',
                            'Total Claim Paid': '#4ECDC4'
                        }
                    )
                    fig_balance_vs_claim.update_layout(
                        xaxis_tickangle=-45,
                        yaxis_tickprefix='₱',
                        yaxis_separatethousands=True,
                        legend_title_text='Metric'
                    )
                    fig_balance_vs_claim.update_traces(
                        hovertemplate='<b>%{x}</b><br>%{fullData.name}: ₱%{y:,.2f}<extra></extra>'
                    )
                    st.plotly_chart(fig_balance_vs_claim, use_container_width=True)
                    
                    # ============================================================
                    # Subsection 2.2: Conversion Rate by Collector
                    # ============================================================
                    st.markdown("### 🎯 Conversion Rate by Collector")
                    
                    # Create color-coded bar chart for conversion rates
                    fig_conversion = px.bar(
                        collector_perf,
                        x='Collector Name',
                        y='Conversion Rate (%)',
                        title='Claim Paid Conversion Rate by Collector',
                        color='Conversion Rate (%)',
                        color_continuous_scale='RdYlGn',
                        text='Conversion Rate (%)'
                    )
                    fig_conversion.update_traces(
                        texttemplate='%{text:.2f}%',
                        textposition='outside',
                        hovertemplate='<b>%{x}</b><br>Conversion Rate: %{y:.2f}%<extra></extra>'
                    )
                    fig_conversion.update_layout(
                        xaxis_tickangle=-45,
                        yaxis_title='Conversion Rate (%)',
                        showlegend=False
                    )
                    st.plotly_chart(fig_conversion, use_container_width=True)
                    
                    # ============================================================
                    # Subsection 2.3: Top Performers Highlight
                    # ============================================================
                    st.markdown("### 🏆 Top Performers")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    # Top by Claim Paid Amount
                    top_claim_paid = collector_perf.nlargest(1, 'Total Claim Paid').iloc[0]
                    with col1:
                        st.metric(
                            "💰 Highest Claim Paid",
                            top_claim_paid['Collector Name'],
                            f"₱{top_claim_paid['Total Claim Paid']:,.2f}"
                        )
                    
                    # Top by Conversion Rate
                    top_conversion = collector_perf.nlargest(1, 'Conversion Rate (%)').iloc[0]
                    with col2:
                        st.metric(
                            "🎯 Best Conversion Rate",
                            top_conversion['Collector Name'],
                            f"{top_conversion['Conversion Rate (%)']:.2f}%"
                        )
                    
                    # Most Accounts
                    most_accounts = collector_perf.nlargest(1, 'Unique Accounts').iloc[0]
                    with col3:
                        st.metric(
                            "📊 Most Accounts Collected",
                            most_accounts['Collector Name'],
                            f"{most_accounts['Unique Accounts']:,} accounts"
                        )
                    
                    # ============================================================
                    # Subsection 2.4: Scatter Plot - Balance vs Conversion
                    # ============================================================
                    st.markdown("### 📈 Balance vs Conversion Rate Analysis")
                    
                    fig_scatter = px.scatter(
                        collector_perf,
                        x='Total Balance',
                        y='Conversion Rate (%)',
                        size='Total Claim Paid',
                        color='Conversion Rate (%)',
                        hover_name='Collector Name',
                        hover_data={
                            'Total Balance': ':,.2f',
                            'Total Claim Paid': ':,.2f',
                            'Conversion Rate (%)': ':.2f',
                            'Unique Accounts': True
                        },
                        title='Balance Portfolio vs Conversion Efficiency',
                        color_continuous_scale='Viridis',
                        size_max=60
                    )
                    fig_scatter.update_layout(
                        xaxis_title='Total Balance (₱)',
                        yaxis_title='Conversion Rate (%)',
                        xaxis_tickprefix='₱',
                        xaxis_separatethousands=True
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
                    st.caption("💡 Bubble size represents Total Claim Paid amount. Ideal: Top-right quadrant (high balance + high conversion)")
                    
                else:
                    # If Balance column not available, show basic collector stats
                    st.warning("⚠️ 'Balance' column not found. Showing basic collector statistics only.")
                    
                    collector_basic = df.groupby('Collector Name').agg({
                        'Claim Paid Amount': 'sum',
                        'Account No.': 'nunique'
                    }).reset_index()
                    
                    collector_basic.columns = ['Collector Name', 'Total Claim Paid', 'Unique Accounts']
                    collector_basic = collector_basic.sort_values('Total Claim Paid', ascending=False)
                    
                    st.dataframe(collector_basic, use_container_width=True, hide_index=True)

            st.divider()

            # ====================================================================
            # Section 3: Client Analysis
            # ====================================================================
            st.subheader("🏢 Client Analysis")
            
            claim_by_client = df.groupby("Client")['Claim Paid Amount'].sum().sort_values(ascending=False)

            col_a, col_b = st.columns(2)
            
            with col_a:
                st.markdown("#### 📊 Claim Paid by Client (Bar Chart)")
                st.bar_chart(claim_by_client)
            
            with col_b:
                st.markdown("#### 🏦 Claim Paid Distribution by Client (Pie Chart)")
                fig, ax = plt.subplots(figsize=(8, 8))
                ax.pie(
                    claim_by_client,
                    labels=claim_by_client.index,
                    autopct='%1.1f%%',
                    startangle=90
                )
                ax.axis('equal')
                st.pyplot(fig)

            st.divider()

            # ====================================================================
            # Section 4: Time-based Analysis
            # ====================================================================
            if 'Claim Paid Date' in df.columns:
                st.subheader("📅 Time-based Analysis")
                
                claim_by_day = df.groupby('Claim Paid Date')['Claim Paid Amount'].sum().reset_index()
                claim_by_day.rename(columns={'Claim Paid Date': 'Date'}, inplace=True)
                
                st.markdown("#### 📈 Daily Trend of Claim Paid Amounts")
                
                fig_daily_trend = px.line(
                    claim_by_day,
                    x='Date',
                    y='Claim Paid Amount',
                    markers=True,
                    title='Daily Claim Paid Trend'
                )
                fig_daily_trend.update_traces(
                    hovertemplate='<b>Date: %{x}</b><br>Amount: ₱%{y:,.2f}<extra></extra>'
                )
                fig_daily_trend.update_layout(
                    yaxis_tickprefix='₱',
                    yaxis_separatethousands=True,
                    yaxis_title='Claim Paid Amount (₱)'
                )
                st.plotly_chart(fig_daily_trend, use_container_width=True)

            st.divider()

            # ====================================================================
            # Section 5: Download Options
            # ====================================================================
            st.subheader("📥 Download Reports")
            
            col_d1, col_d2 = st.columns(2)
            
            with col_d1:
                # Download collector performance report
                if 'Collector Name' in df.columns and 'Balance' in df.columns:
                    output_collector = BytesIO()
                    with pd.ExcelWriter(output_collector, engine='openpyxl') as writer:
                        collector_perf_display.to_excel(writer, index=False, sheet_name='Collector Performance')
                    
                    st.download_button(
                        label="📊 Download Collector Performance Report",
                        data=output_collector.getvalue(),
                        file_name="collector_performance_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
            with col_d2:
                # Download full data
                output_full = BytesIO()
                with pd.ExcelWriter(output_full, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Claim Paid Data')
                
                st.download_button(
                    label="📋 Download Full Claim Paid Data",
                    data=output_full.getvalue(),
                    file_name="claim_paid_full_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            st.divider()

            # ====================================================================
            # Section 6: Data Preview
            # ====================================================================
            st.subheader("📋 Data Preview")
            st.dataframe(df.head(50), use_container_width=True)
    with tab14:
        st.title("🎯 Tab 14: PREDEL Personal Loan Target (Exclusive)")

        st.markdown("""
        **PREDEL Personal Loan Target (Exclusive)**
        - **Fixed Targets:**  
            - Attempts: 300%  
            - Contact Rate: 4%  
            - PTP Rate: 26%  
            - Collection Rate: 26%  
            - Target Amount: ₱4,750,754.47  
        - **October 2025 Monthly Target:**  
            - Attempt Count: 18,400  
            - Contact Rate Count: 756  
            - PTP Rate Count: 191  
            - Collection Rate: ₱4,750,754.47  
        """)

        # --- Customization for monthly targets ---
        st.subheader("Customize Monthly Targets")
        col1, col2, col3, col4 = st.columns(4)
        monthly_attempts = col1.number_input("Monthly Attempt Target", value=18400)
        monthly_contacts = col2.number_input("Monthly Contact Target", value=756)
        monthly_ptp = col3.number_input("Monthly PTP Target", value=191)
        monthly_collection = col4.number_input("Monthly Collection Target (₱)", value=4750754.47, format="%.2f")

        st.divider()
        st.subheader("Upload PREDEL Personal Loan File(s)")
        uploaded_files_predel = st.file_uploader(
            "Upload one or more Excel files (PREDEL Personal Loan)",
            type=["xlsx"],
            accept_multiple_files=True,
            key="predel_files"
        )

        if uploaded_files_predel:
            # Combine all files into one DataFrame
            dfs = []
            for file in uploaded_files_predel:
                try:
                    df = pd.read_excel(file)
                    dfs.append(df)
                except Exception as e:
                    st.error(f"Error reading {file.name}: {e}")
            if not dfs:
                st.warning("No valid files uploaded.")
                st.stop()
            df_predel = pd.concat(dfs, ignore_index=True)
            df_predel.columns = df_predel.columns.str.strip()

            # --- Attempt Count ---
            attempt_count = len(df_predel)

            # --- Contact Rate & PTP Rate ---
            contact_keywords = ["PTP", "POSITIVE CONTACT", "POSITIVE", "FOLLOW UP", "PAYMENT"]
            ptp_keyword = "PTP"
            if "STATUS CODE" in df_predel.columns:
                status_upper = df_predel["STATUS CODE"].astype(str).str.upper()
                # Contact Rate: any row containing any of the keywords
                contact_count = status_upper.str.contains("|".join(contact_keywords), na=False).sum()
                # PTP Rate: any row containing "PTP"
                ptp_count = status_upper.str.contains(ptp_keyword, na=False).sum()
            else:
                contact_count = 0
                ptp_count = 0

            # --- Collection Rate ---
            if "PAYMENT AMOUNT" in df_predel.columns:
                df_predel["PAYMENT AMOUNT"] = pd.to_numeric(df_predel["PAYMENT AMOUNT"], errors="coerce").fillna(0)
                collection_sum = df_predel["PAYMENT AMOUNT"].sum()
            else:
                collection_sum = 0.0

            # --- KPIs ---
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Attempts", f"{attempt_count:,}", f"Target: {monthly_attempts:,}")
            k2.metric("Contact Rate", f"{contact_count:,}", f"Target: {monthly_contacts:,}")
            k3.metric("PTP Rate", f"{ptp_count:,}", f"Target: {monthly_ptp:,}")
            k4.metric("Collection Rate", f"₱{collection_sum:,.2f}", f"Target: ₱{monthly_collection:,.2f}")

            # --- Progress Bars ---
            st.progress(min(attempt_count / monthly_attempts, 1.0), text="Attempts Progress")
            st.progress(min(contact_count / monthly_contacts, 1.0), text="Contact Rate Progress")
            st.progress(min(ptp_count / monthly_ptp, 1.0), text="PTP Rate Progress")
            st.progress(min(collection_sum / monthly_collection, 1.0), text="Collection Rate Progress")

            # --- Data Preview ---
            with st.expander("🔎 Preview Uploaded Data"):
                st.dataframe(df_predel.head(50))

            # --- Downloadable Summary ---
            summary_df = pd.DataFrame({
                "Metric": ["Attempts", "Contact Rate", "PTP Rate", "Collection Rate"],
                "Actual": [attempt_count, contact_count, ptp_count, collection_sum],
                "Target": [monthly_attempts, monthly_contacts, monthly_ptp, monthly_collection]
            })
            out = BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as writer:
                summary_df.to_excel(writer, index=False, sheet_name="PREDEL_Target_Summary")
                df_predel.to_excel(writer, index=False, sheet_name="Raw_Data")
            st.download_button(
                "📥 Download PREDEL Target Summary (Excel)",
                data=out.getvalue(),
                file_name="PREDEL_Target_Summary.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("Upload at least one PREDEL Personal Loan Excel file to see the summary.")
    
    # --- Tab 15: Final DROPPED Status Checker ---
    with tab15:
        st.title("📌 Tab 15: Final DROPPED Status Checker")
        st.write("Upload file(s). For each Debtor, we take the latest Date row and check whether its Status == 'DROPPED'.")
        files = st.file_uploader(
            "📥 Upload Excel/CSV file(s) containing Debtor, Date, Status",
            type=["csv", "xls", "xlsx"],
            accept_multiple_files=True,
            key="dropped_files"
        )

        if not files:
            st.info("⚠️ Please upload one or more files to check final DROPPED statuses.")
        else:
            try:
                dfs = []
                for f in files:
                    if f.name.lower().endswith(".csv"):
                        dfs.append(pd.read_csv(f))
                    else:
                        dfs.append(pd.read_excel(f, engine="openpyxl"))
                df_all = pd.concat(dfs, ignore_index=True)
                # normalize column names
                df_all.columns = df_all.columns.str.strip()

                # find debtor / id column
                debtor_candidates = ["Debtor", "Debtor ID", "Account No.", "Account", "DebtorID"]
                debtor_col = next((c for c in debtor_candidates if c in df_all.columns), None)
                if debtor_col is None:
                    st.error("❌ Could not find a Debtor/Account ID column. Expected one of: " + ", ".join(debtor_candidates))
                    st.stop()

                # find date column
                date_candidates = ["Date", "Call Made Date", "Call Date", "Timestamp"]
                date_col = next((c for c in date_candidates if c in df_all.columns), None)
                if date_col is None:
                    st.error("❌ Could not find a Date column. Expected one of: " + ", ".join(date_candidates))
                    st.stop()

                # find status column
                status_candidates = ["Status", "Final Dial Status", "Final Status"]
                status_col = next((c for c in status_candidates if c in df_all.columns), None)
                if status_col is None:
                    st.error("❌ Could not find a Status column. Expected one of: " + ", ".join(status_candidates))
                    st.stop()

                # parse dates
                df_all[date_col] = pd.to_datetime(df_all[date_col], errors="coerce")
                # drop rows without debtor or date
                df_all = df_all[df_all[debtor_col].notna()]
                if df_all.empty:
                    st.warning("No valid rows after filtering out missing Debtor/ID.")
                    st.stop()

                # get latest row per debtor
                idx = df_all.groupby(df_all[debtor_col].astype(str))[date_col].idxmax()
                latest = df_all.loc[idx].copy()
                latest[status_col] = latest[status_col].astype(str).str.upper().str.strip()

                # count DROPPED
                dropped_mask = latest[status_col] == "DROPPED"
                dropped_count = int(dropped_mask.sum())
                total_debtors = int(latest.shape[0])

                st.subheader("✅ Summary")
                st.metric("Total unique debtors (latest row)", f"{total_debtors:,}")
                st.metric("Debtors with final status DROPPED", f"{dropped_count:,}")

                if dropped_count > 0:
                    dropped_df = latest.loc[dropped_mask, [debtor_col, date_col, status_col]].sort_values(date_col, ascending=False)
                    st.subheader("📋 Debtors with final DROPPED status (preview)")
                    st.dataframe(dropped_df)

                    # download
                    out = BytesIO()
                    with pd.ExcelWriter(out, engine="openpyxl") as writer:
                        dropped_df.to_excel(writer, index=False, sheet_name="Dropped_Final")
                    st.download_button(
                        "📥 Download DROPPED list (Excel)",
                        data=out.getvalue(),
                        file_name="final_dropped_debtors.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.info("No debtors have final status DROPPED in the uploaded data (based on latest Date per Debtor).")

            except Exception as e:
                st.error(f"❌ Error processing files: {e}")
    # --- Tab 16: Intensity Report Analyzer (Calls + SMS) ---
    with tab16:
        st.title("📊 Intensity Report Analyzer")

        st.write("""
        Upload your **raw Intensity Report** Excel files below.  
        The system will automatically process both calls and SMS — creating attempt counts,
        total dials, contact rates, delivery rates, and other disposition summaries.
        """)

        # --- Two file uploaders ---
        col_upload1, col_upload2 = st.columns(2)
        
        with col_upload1:
            uploaded_call_file = st.file_uploader("📞 Upload Call Intensity Report (.xlsx or .csv)", type=["xlsx", "csv"], key="call_file")
        
        with col_upload2:
            uploaded_sms_file = st.file_uploader("📱 Upload SMS Report (.xlsx or .csv)", type=["xlsx", "csv"], key="sms_file")

        # Initialize dataframes
        df_call = None
        df_sms = None
        summary_call = None
        summary_sms = None

        # --- Process Call Data ---
        if uploaded_call_file:
            # --- Load and Clean ---
            if uploaded_call_file.name.endswith('.csv'):
                df_call = pd.read_csv(uploaded_call_file)
            else:
                df_call = pd.read_excel(uploaded_call_file)
            
            df_call.columns = df_call.columns.str.strip()

            required_cols = ["Account No.", "Call Date", "Status"]
            if not all(col in df_call.columns for col in required_cols):
                st.error(f"❌ Call file missing required columns. Found: {list(df_call.columns)}")
                df_call = None
            else:
                # --- Process Data ---
                df_call["Call Date"] = pd.to_datetime(df_call["Call Date"], format='%d/%m/%Y', errors="coerce")
                df_call = df_call.sort_values(["Account No.", "Call Date", "Call Time"])
                df_call["Attempt"] = df_call.groupby("Account No.").cumcount() + 1

                def classify_status(status):
                    s = str(status).upper()
                    # Positive contacts
                    if any(k in s for k in ["POSITIVE CONTACT", "PTP", "BP -", "FOLLOW UP"]):
                        return "Contact"
                    # Positive but not contact
                    elif "POSITIVE" in s:
                        return "Positive-Other"
                    # Voicemail
                    elif "VM" in s or "VOICEMAIL" in s:
                        return "Voicemail"
                    # No answer / Ringing
                    elif "RINGING" in s or "RNA" in s:
                        return "No Answer"
                    # Busy
                    elif "BUSY" in s:
                        return "Busy"
                    # Negative dispositions
                    elif "NEGATIVE" in s:
                        return "Negative"
                    # Other categories
                    elif any(k in s for k in ["JUNK", "DO NOT CALL", "BULK SMS"]):
                        return "Other"
                    else:
                        return "Other"

                df_call["Category"] = df_call["Status"].apply(classify_status)

                summary_call = (
                    df_call.groupby("Attempt")
                    .agg(
                        Dials=("Account No.", "count"),
                        Contacts=("Category", lambda x: (x == "Contact").sum()),
                        Positive_Other=("Category", lambda x: (x == "Positive-Other").sum()),
                        Voicemail=("Category", lambda x: (x == "Voicemail").sum()),
                        No_Answer=("Category", lambda x: (x == "No Answer").sum()),
                        Busy=("Category", lambda x: (x == "Busy").sum()),
                        Negative=("Category", lambda x: (x == "Negative").sum()),
                        Other=("Category", lambda x: (x == "Other").sum()),
                    )
                    .reset_index()
                )
                summary_call["Contact Rate (%)"] = (summary_call["Contacts"] / summary_call["Dials"] * 100).round(2)

        # --- Process SMS Data ---
        if uploaded_sms_file:
            # --- Load and Clean ---
            if uploaded_sms_file.name.endswith('.csv'):
                df_sms = pd.read_csv(uploaded_sms_file)
            else:
                df_sms = pd.read_excel(uploaded_sms_file)
            
            df_sms.columns = df_sms.columns.str.strip()

            required_cols_sms = ["Account No.", "Date Sent", "Status"]
            if not all(col in df_sms.columns for col in required_cols_sms):
                st.error(f"❌ SMS file missing required columns. Found: {list(df_sms.columns)}")
                df_sms = None
            else:
                # --- Process Data ---
                df_sms["Date Sent"] = pd.to_datetime(df_sms["Date Sent"], format='%d/%m/%Y', errors="coerce")
                df_sms = df_sms.sort_values(["Account No.", "Date Sent", "Time Sent"])
                df_sms["Attempt"] = df_sms.groupby("Account No.").cumcount() + 1

                def classify_sms_status(status):
                    s = str(status).lower()
                    if "delivered" in s:
                        return "Delivered"
                    elif "failed" in s:
                        return "Failed"
                    elif "pending" in s:
                        return "Pending"
                    else:
                        return "Other"

                df_sms["Category"] = df_sms["Status"].apply(classify_sms_status)

                summary_sms = (
                    df_sms.groupby("Attempt")
                    .agg(
                        Total_SMS=("Account No.", "count"),
                        Delivered=("Category", lambda x: (x == "Delivered").sum()),
                        Failed=("Category", lambda x: (x == "Failed").sum()),
                        Pending=("Category", lambda x: (x == "Pending").sum()),
                        Other=("Category", lambda x: (x == "Other").sum()),
                    )
                    .reset_index()
                )
                summary_sms["Delivery Rate (%)"] = (summary_sms["Delivered"] / summary_sms["Total_SMS"] * 100).round(2)

        # --- Combined KPI Section ---
        if df_call is not None or df_sms is not None:
            st.subheader("📈 Metrics Overview")
            
            metric_cols = st.columns(4)
            
            if df_call is not None and not summary_call.empty:
                most_call_attempt = summary_call.loc[summary_call["Dials"].idxmax()]
                least_call_attempt = summary_call.loc[summary_call["Dials"].idxmin()]
                
                with metric_cols[0]:
                    st.metric(
                        label="📞 Most Call Attempts",
                        value=f"Attempt {int(most_call_attempt['Attempt'])}",
                        delta=f"{int(most_call_attempt['Dials'])} Dials"
                    )
                with metric_cols[1]:
                    st.metric(
                        label="💤 Least Call Attempts",
                        value=f"Attempt {int(least_call_attempt['Attempt'])}",
                        delta=f"{int(least_call_attempt['Dials'])} Dials"
                    )
            
            if df_sms is not None and not summary_sms.empty:
                most_sms_attempt = summary_sms.loc[summary_sms["Total_SMS"].idxmax()]
                overall_delivery = (df_sms["Category"] == "Delivered").sum() / len(df_sms) * 100
                
                with metric_cols[2]:
                    st.metric(
                        label="📱 Most SMS Attempts",
                        value=f"Attempt {int(most_sms_attempt['Attempt'])}",
                        delta=f"{int(most_sms_attempt['Total_SMS'])} SMS"
                    )
                with metric_cols[3]:
                    st.metric(
                        label="✅ Overall SMS Delivery",
                        value=f"{overall_delivery:.2f}%"
                    )

        # --- HIGH ATTEMPT ACCOUNTS SECTION ---
        if df_call is not None or df_sms is not None:
            st.subheader("🔥 Accounts with Most Attempts")
            
            # Input for threshold
            top_n = st.number_input("Show Top N Accounts", min_value=5, max_value=100, value=10, step=5,
                                   help="Number of accounts to display with highest attempts")
            
            high_attempt_accounts = []
            
            # Get accounts with high call attempts
            if df_call is not None:
                call_attempts_per_account = df_call.groupby("Account No.")["Attempt"].max().reset_index()
                call_attempts_per_account.columns = ["Account No.", "Total_Call_Attempts"]
                high_attempt_accounts.append(call_attempts_per_account)
            
            # Get accounts with high SMS attempts
            if df_sms is not None:
                sms_attempts_per_account = df_sms.groupby("Account No.")["Attempt"].max().reset_index()
                sms_attempts_per_account.columns = ["Account No.", "Total_SMS_Attempts"]
                high_attempt_accounts.append(sms_attempts_per_account)
            
            # Merge the dataframes
            if len(high_attempt_accounts) == 2:
                # Both call and SMS data available
                high_attempts_df = pd.merge(
                    high_attempt_accounts[0], 
                    high_attempt_accounts[1], 
                    on="Account No.", 
                    how="outer"
                )
                high_attempts_df = high_attempts_df.fillna(0)
                high_attempts_df["Total_Call_Attempts"] = high_attempts_df["Total_Call_Attempts"].astype(int)
                high_attempts_df["Total_SMS_Attempts"] = high_attempts_df["Total_SMS_Attempts"].astype(int)
                
                # Calculate combined attempts
                high_attempts_df["Total_Combined_Attempts"] = (
                    high_attempts_df["Total_Call_Attempts"] + high_attempts_df["Total_SMS_Attempts"]
                )
                
            elif len(high_attempt_accounts) == 1:
                # Only one type of data available
                high_attempts_df = high_attempt_accounts[0].copy()
                if df_call is not None and df_sms is None:
                    high_attempts_df["Total_SMS_Attempts"] = 0
                    high_attempts_df["Total_Combined_Attempts"] = high_attempts_df["Total_Call_Attempts"]
                else:
                    high_attempts_df["Total_Call_Attempts"] = 0
                    high_attempts_df["Total_Combined_Attempts"] = high_attempts_df["Total_SMS_Attempts"]
            else:
                high_attempts_df = pd.DataFrame()
            
            if not high_attempts_df.empty:
                # Sort by combined attempts and get top N
                high_attempts_df = high_attempts_df.sort_values(
                    by="Total_Combined_Attempts", 
                    ascending=False
                ).head(top_n)
                
                st.write(f"**Top {len(high_attempts_df)} accounts with most attempts:**")
                st.dataframe(high_attempts_df, use_container_width=True)
                
                # Download button
                buffer_high_attempts = BytesIO()
                high_attempts_df.to_excel(buffer_high_attempts, index=False)
                st.download_button(
                    label="📥 Download High Attempt Accounts",
                    data=buffer_high_attempts.getvalue(),
                    file_name="High_Attempt_Accounts.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                
                # Visual breakdown
                col_viz1, col_viz2, col_viz3 = st.columns(3)
                with col_viz1:
                    avg_calls = high_attempts_df["Total_Call_Attempts"].mean()
                    st.metric("📞 Avg Call Attempts", f"{avg_calls:.1f}")
                with col_viz2:
                    avg_sms = high_attempts_df["Total_SMS_Attempts"].mean()
                    st.metric("📱 Avg SMS Attempts", f"{avg_sms:.1f}")
                with col_viz3:
                    avg_combined = high_attempts_df["Total_Combined_Attempts"].mean()
                    st.metric("🔥 Avg Combined Attempts", f"{avg_combined:.1f}")
            else:
                st.info("No data available to show high attempt accounts.")

        # --- LOW ATTEMPT ACCOUNTS SECTION ---
        if df_call is not None or df_sms is not None:
            st.subheader("⚠️ Accounts with Low Attempts")
            
            # Input for threshold
            threshold_col1, threshold_col2 = st.columns(2)
            with threshold_col1:
                call_threshold = st.number_input("Call Attempt Threshold (≤)", min_value=0, value=2, step=1, 
                                                help="Show accounts with attempts less than or equal to this number")
            with threshold_col2:
                sms_threshold = st.number_input("SMS Attempt Threshold (≤)", min_value=0, value=2, step=1,
                                               help="Show accounts with attempts less than or equal to this number")
            
            low_attempt_accounts = []
            
            # Get accounts with low call attempts
            if df_call is not None:
                call_attempts_per_account = df_call.groupby("Account No.")["Attempt"].max().reset_index()
                call_attempts_per_account.columns = ["Account No.", "Total_Call_Attempts"]
                low_call_accounts = call_attempts_per_account[call_attempts_per_account["Total_Call_Attempts"] <= call_threshold]
                low_attempt_accounts.append(low_call_accounts)
            
            # Get accounts with low SMS attempts
            if df_sms is not None:
                sms_attempts_per_account = df_sms.groupby("Account No.")["Attempt"].max().reset_index()
                sms_attempts_per_account.columns = ["Account No.", "Total_SMS_Attempts"]
                low_sms_accounts = sms_attempts_per_account[sms_attempts_per_account["Total_SMS_Attempts"] <= sms_threshold]
                low_attempt_accounts.append(low_sms_accounts)
            
            # Merge the dataframes
            if len(low_attempt_accounts) == 2:
                # Both call and SMS data available
                low_attempts_df = pd.merge(
                    low_attempt_accounts[0], 
                    low_attempt_accounts[1], 
                    on="Account No.", 
                    how="outer"
                )
                low_attempts_df = low_attempts_df.fillna(0)
                low_attempts_df["Total_Call_Attempts"] = low_attempts_df["Total_Call_Attempts"].astype(int)
                low_attempts_df["Total_SMS_Attempts"] = low_attempts_df["Total_SMS_Attempts"].astype(int)
                
                # Add a combined flag
                low_attempts_df["Low_in_Both"] = (
                    (low_attempts_df["Total_Call_Attempts"] <= call_threshold) & 
                    (low_attempts_df["Total_SMS_Attempts"] <= sms_threshold)
                )
                
            elif len(low_attempt_accounts) == 1:
                # Only one type of data available
                low_attempts_df = low_attempt_accounts[0].copy()
                if df_call is not None and df_sms is None:
                    low_attempts_df["Total_SMS_Attempts"] = 0
                    low_attempts_df["Low_in_Both"] = False
                else:
                    low_attempts_df["Total_Call_Attempts"] = 0
                    low_attempts_df["Low_in_Both"] = False
            else:
                low_attempts_df = pd.DataFrame()
            
            if not low_attempts_df.empty:
                # Sort by accounts with low attempts in both
                low_attempts_df = low_attempts_df.sort_values(
                    by=["Low_in_Both", "Total_Call_Attempts", "Total_SMS_Attempts"], 
                    ascending=[False, True, True]
                )
                
                st.write(f"**Found {len(low_attempts_df)} accounts with low attempts:**")
                st.dataframe(low_attempts_df, use_container_width=True)
                
                # Download button
                buffer_low_attempts = BytesIO()
                low_attempts_df.to_excel(buffer_low_attempts, index=False)
                st.download_button(
                    label="📥 Download Low Attempt Accounts",
                    data=buffer_low_attempts.getvalue(),
                    file_name="Low_Attempt_Accounts.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                
                # Visual breakdown
                col_viz1, col_viz2 = st.columns(2)
                with col_viz1:
                    low_both_count = low_attempts_df["Low_in_Both"].sum()
                    st.metric("🔴 Low in BOTH Calls & SMS", low_both_count)
                with col_viz2:
                    low_either_count = len(low_attempts_df) - low_both_count
                    st.metric("🟡 Low in Either Calls OR SMS", low_either_count)
            else:
                st.info("✅ No accounts found with low attempts based on current thresholds.")

        # --- Unified Account Search ---
        if df_call is not None or df_sms is not None:
            st.subheader("🔍 Account Search (Calls + SMS)")
            account_search = st.text_input("Enter Account Number to search:")

            if account_search:
                search_results = []
                
                # Search in calls
                if df_call is not None:
                    call_filtered = df_call[df_call["Account No."].astype(str).str.contains(account_search, case=False, na=False)]
                    if not call_filtered.empty:
                        total_call_attempts = call_filtered["Attempt"].max()
                        search_results.append(f"📞 **Calls: {total_call_attempts} attempts**")
                
                # Search in SMS
                if df_sms is not None:
                    sms_filtered = df_sms[df_sms["Account No."].astype(str).str.contains(account_search, case=False, na=False)]
                    if not sms_filtered.empty:
                        total_sms_attempts = sms_filtered["Attempt"].max()
                        search_results.append(f"📱 **SMS: {total_sms_attempts} attempts**")
                
                if search_results:
                    st.success(f"✅ Account **{account_search}** found:")
                    for result in search_results:
                        st.write(result)
                    
                    # Display detailed data in tabs
                    detail_tab1, detail_tab2 = st.tabs(["📞 Call History", "📱 SMS History"])
                    
                    with detail_tab1:
                        if df_call is not None and not call_filtered.empty:
                            st.dataframe(
                                call_filtered[["Account No.", "Call Date", "Call Time", "Type of Call", "Status", "Category", "Attempt"]],
                                use_container_width=True
                            )
                        else:
                            st.info("No call records found for this account.")
                    
                    with detail_tab2:
                        if df_sms is not None and not sms_filtered.empty:
                            st.dataframe(
                                sms_filtered[["Account No.", "Date Sent", "Time Sent", "SMS Template Name", "Status", "Category", "Attempt"]],
                                use_container_width=True
                            )
                        else:
                            st.info("No SMS records found for this account.")
                else:
                    st.warning(f"No matching records found for account: {account_search}")

        # --- Display Summary Tables and Charts ---
        if df_call is not None or df_sms is not None:
            
            summary_tab1, summary_tab2 = st.tabs(["📞 Call Summary", "📱 SMS Summary"])
            
            # --- Call Summary Tab ---
            with summary_tab1:
                if df_call is not None and summary_call is not None:
                    st.subheader("📊 Call Summary by Attempt")
                    st.dataframe(summary_call, use_container_width=True)

                    buffer_call = BytesIO()
                    summary_call.to_excel(buffer_call, index=False)
                    st.download_button(
                        label="💾 Download Call Report",
                        data=buffer_call.getvalue(),
                        file_name="Processed_Call_Report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                    st.subheader("📈 Contact Rate Trend by Attempt")
                    fig_call = px.line(
                        summary_call,
                        x="Attempt",
                        y="Contact Rate (%)",
                        markers=True,
                        title="Contact Rate Trend per Attempt",
                    )
                    st.plotly_chart(fig_call, use_container_width=True)

                    st.subheader("📞 Call Disposition Breakdown")
                    disp_cols = ["Contacts", "Positive_Other", "Voicemail", "No_Answer", "Busy", "Negative", "Other"]
                    disp_df = summary_call.melt(
                        id_vars=["Attempt"], value_vars=disp_cols, var_name="Disposition", value_name="Count"
                    )
                    fig_call2 = px.bar(
                        disp_df,
                        x="Attempt",
                        y="Count",
                        color="Disposition",
                        barmode="stack",
                        title="Disposition Distribution by Attempt",
                    )
                    st.plotly_chart(fig_call2, use_container_width=True)
                else:
                    st.info("📂 Upload a call report to view call analytics.")

            # --- SMS Summary Tab ---
            with summary_tab2:
                if df_sms is not None and summary_sms is not None:
                    st.subheader("📊 SMS Summary by Attempt")
                    st.dataframe(summary_sms, use_container_width=True)

                    buffer_sms = BytesIO()
                    summary_sms.to_excel(buffer_sms, index=False)
                    st.download_button(
                        label="💾 Download SMS Report",
                        data=buffer_sms.getvalue(),
                        file_name="Processed_SMS_Report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                    st.subheader("📈 Delivery Rate Trend by Attempt")
                    fig_sms = px.line(
                        summary_sms,
                        x="Attempt",
                        y="Delivery Rate (%)",
                        markers=True,
                        title="SMS Delivery Rate Trend per Attempt",
                    )
                    st.plotly_chart(fig_sms, use_container_width=True)

                    st.subheader("📱 SMS Status Breakdown")
                    status_cols = ["Delivered", "Failed", "Pending", "Other"]
                    status_df = summary_sms.melt(
                        id_vars=["Attempt"], value_vars=status_cols, var_name="Status", value_name="Count"
                    )
                    fig_sms2 = px.bar(
                        status_df,
                        x="Attempt",
                        y="Count",
                        color="Status",
                        barmode="stack",
                        title="SMS Status Distribution by Attempt",
                    )
                    st.plotly_chart(fig_sms2, use_container_width=True)

                    # SMS Template Analysis
                    if "SMS Template Name" in df_sms.columns:
                        st.subheader("📋 SMS Template Performance")
                        template_summary = (
                            df_sms.groupby("SMS Template Name")
                            .agg(
                                Total=("Account No.", "count"),
                                Delivered=("Category", lambda x: (x == "Delivered").sum()),
                                Failed=("Category", lambda x: (x == "Failed").sum()),
                            )
                            .reset_index()
                        )
                        template_summary["Delivery Rate (%)"] = (template_summary["Delivered"] / template_summary["Total"] * 100).round(2)
                        template_summary = template_summary.sort_values("Total", ascending=False)
                        
                        st.dataframe(template_summary, use_container_width=True)
                else:
                    st.info("📂 Upload an SMS report to view SMS analytics.")

            # --- Column Guide ---
            st.markdown("""
            ### 🧩 Column Guide:
            
            **Call Metrics:**
            - **Attempt** → The number of call rounds made to the same customers.  
            - **Dials** → Total calls made in that attempt.  
            - **Contacts** → Successful contacts (PTP, BP, Follow-ups, Positive Contacts).  
            - **Positive_Other** → Other positive dispositions (Hiding, Leave Message, etc.).  
            - **Voicemail** → Calls that reached voicemail (VM).  
            - **No_Answer** → Calls that kept ringing or RNA.  
            - **Busy** → Lines that were busy.  
            - **Negative** → Negative call dispositions (Not Active, Wrong Number, etc.).  
            - **Other** → Junk, Do Not Call, Bulk SMS, etc.  
            - **Contact Rate (%)** → Percentage of successful contacts from total dials.  
            
            **SMS Metrics:**
            - **Attempt** → The number of SMS rounds sent to the same customers.  
            - **Total_SMS** → Total SMS messages sent in that attempt.  
            - **Delivered** → SMS successfully delivered to recipient.  
            - **Failed** → SMS that failed to deliver.  
            - **Pending** → SMS still in pending status.  
            - **Delivery Rate (%)** → Percentage of successful deliveries from total SMS sent.  
            
            **Low Attempt Accounts:**
            - **Total_Call_Attempts** → Maximum number of call attempts for this account.
            - **Total_SMS_Attempts** → Maximum number of SMS attempts for this account.
            - **Low_in_Both** → True if account has low attempts in both calls AND SMS.
            """)
        else:
            st.info("📂 Please upload your Call and/or SMS Report files to start.")
    # --- Tab 17: Playwright Experiment ---
    with tab17:
        st.title("🧪 Tab 17: Playwright Experiment")

        # Initialize session states
        if 'custom_clients' not in st.session_state:
            st.session_state.custom_clients = {
                "BANKARD": ["AAA", "SPO L2", "SPO L1"],
                "EWB": ["SP MADRID L1", "SP MADRID L4", "SPM GEO L1", "SPM GEO L4", "SPM GEO L2", "PL SPM L1"],
                "EWB PRE CHARGE OFF": ["EWB 150"],
                "RCBC": ["T", "PREDEL", "PL", "SL", "PN", "LUZ", "VIS", "MIN", "MAN", "W"]
            }
        
        if 'custom_dispositions' not in st.session_state:
            st.session_state.custom_dispositions = {
                "NEGATIVE": ["RNA", "BUSY", "NEGATIVE - CALL JOB_NO LONGER CONNECTED", "NEGATIVE - NO SOCIAL MEDIA ACCOUNTS", 
                            "NEGATIVE - CALL MOBILE_WRONG NUMBER", "NEGATIVE - CALL MOBILE_KEEPS ON RINGING", "NEGATIVE - CALL HOME_WRONG NUMBER",
                            "NEGATIVE - CALL JOB_WRONG NUMBER", "NEGATIVE - CALL JOB_NOT ACTIVE", "NEGATIVE - CALL JOB_KEEPS ON RINGING", 
                            "NEGATIVE - CALL MOBILE_NOT ACTIVE", "NEGATIVE - CALL HOME_ NOT ACTIVE", "NEGATIVE - CALL HOME_KEEPS ON RINGING",
                            "NEGATIVE - CALL_BUSY", "NEGATIVE - CALL_KEEPS ON RINGING", "NEGATIVE - SMEDIA NEGATIVE", "NEGATIVE - CALL_NIS",
                            "NEGATIVE - CLIENT_UNKNOWN", "NEGATIVE - CALL DROPPED_CALL DROPPED W/O PID", "NEGATIVE - HIDING", 
                            "NEGATIVE - SENT EMAIL TEMPLATE", "NEGATIVE - SENT SMS TEMPLATE", "NEGATIVE - CALL_CLIENT NO LONGER CONNECTED",
                            "NEGATIVE - MOVED OUT_CH NO LONGER RESIDING", "NEGATIVE - KOR", "NEGATIVE - NIS/BUSY/NOT RINGING", 
                            "NEGATIVE - DROPPED CALL", "NEGATIVE - RECYCLED MOBILE NUMBER", "NEGATIVE - WRONG NUMBER"],
                "POSITIVE": ["POSITIVE - HIDING", "POSITIVE CONTACT - OTHER PRIORITY", "POSITIVE - LEAVE MESSAGE WITH THIRD PARTY", 
                            "POSITIVE CONTACT - NO INTENTION OF PAYING", "POSITIVE - SOCIAL MEDIA POSITIVE", "POSITIVE - EMAIL RESPONSIVE",
                            "POSITIVE - NO PERSONAL CALL", "POSITIVE - CALLBACK", "POSITIVE - FIELD EMPLOYER POSITIVE", 
                            "POSITIVE - FIELD HOME POSITIVE", "POSITIVE CONTACT - NIOP", "POSITIVE CONTACT - TFIP", "POSITIVE - EMPLOYER POSITIVE", 
                            "POSITIVE CONTACT - EMAIL RESPONSIVE", "POSITIVE CONTACT - UNDER MEDICATION", "POSITIVE - LEAVE MESSAGE",
                            "POSITIVE CONTACT - FINANCIAL DIFFICULTY", "POSITIVE CONTACT - SMS RESPONSIVE", "POSITIVE - POSITIVE EMPLOYER", 
                            "POSITIVE - CALL EMPLOYER POSITIVE", "POSITIVE - LEFT MESSAGE"],
                "BP": ["BP"],
                "FIELD REQUEST": ["FIELD REQUEST DL 1 - POSITIVE", "FIELD RESULT - AUTO FIELD", "FIELD RESULT - FIELD_NIOP"],
                "OTHER": ["BULK SMS SENT", "NEW", "REACTIVE", "PM", "PU", "EMAIL BLAST - SENT NOTIFICATIONS", 
                        "EMAIL BLAST - SENT PAYMENT OPTIONS", "UNTC", "DROPPED", "UNLOCKED"]
            }
        
        if 'custom_agents' not in st.session_state:
            st.session_state.custom_agents = {
                "AAA": ["MSAGBUYA", "JVPANIT", "JELALANGAN", "CLAGUILAR", "JVDEVERA", "WGGARCIA", "JMLEGASPI"],
                "SPO L1": ["MSAGBUYA", "JVPANIT", "JELALANGAN", "CLAGUILAR", "JVDEVERA", "WGGARCIA", "JMLEGASPI"],
                "SPO L2": ["MSAGBUYA", "JVPANIT", "JELALANGAN", "CLAGUILAR", "JVDEVERA", "WGGARCIA", "JMLEGASPI"],
                "T": ["LVROSARIO", "PLMENDOZA"],
                "PL": ["STMATEO", "MBLOSA", "MSVILLANUEVA", "KALEGNO", "JANTICAMARA", "CMANLAPUZ"],
                "SL": ["STMATEO", "MBLOSA", "MSVILLANUEVA", "KALEGNO", "JANTICAMARA", "CMANLAPUZ", "MNICOLAS", "GJOCOMEN"],
                "PN": ["STMATEO", "MBLOSA", "MSVILLANUEVA", "KALEGNO", "JANTICAMARA", "CMANLAPUZ", "MNICOLAS", "GJOCOMEN"],
                "W": ["CTBALGUA", "GJOCOMEN", "MNICOLAS", "MBLOSA"],
                "LUZ": ["CTBALGUA", "GJOCOMEN", "MNICOLAS", "MBLOSA"],
                "VIS": ["CTBALGUA", "GJOCOMEN", "MNICOLAS", "MBLOSA"],
                "MIN": ["CTBALGUA", "GJOCOMEN", "MNICOLAS", "MBLOSA"],
                "MAN": ["CTBALGUA", "GJOCOMEN", "MNICOLAS", "MBLOSA"],
                "PREDEL": ["VIDEGUZMAN", "JCADAME", "NTCONSTANTINO"],
                "SP MADRID L1": ["JRVITERO", "MSRICO", "DALEGASPI", "IEFORTADES"],
                "SP MADRID L4": ["JBPALISOC"],
                "SPM GEO L1": ["RICARANTO", "JPABALOS"],
                "SPM GEO L2": ["CBTODIO"],
                "PL SPM L1": ["BNBAUTISTA"],
                "SPM GEO L4": ["FADONATO"],
                "EWB 150": ["MSVILLANUEVA", "JVCABAMALAN", "KEARCENO", "KALEGNO"]
            }
        
        for key in ['selected_dispositions', 'previous_statuses', 'selected_contacts']:
            if key not in st.session_state:
                st.session_state[key] = []

        # Settings in expander (collapsed by default)
        with st.expander("⚙️ Settings", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.subheader("👥 Clients")
                client_edit = st.selectbox("Edit:", ["+ Add New"] + list(st.session_state.custom_clients.keys()), key="ce")
                if client_edit == "+ Add New":
                    new_c = st.text_input("Name:", key="nc")
                    if st.button("Create", key="cc") and new_c and new_c not in st.session_state.custom_clients:
                        st.session_state.custom_clients[new_c] = []
                        st.rerun()
                else:
                    p_str = st.text_area("Placements:", "\n".join(st.session_state.custom_clients[client_edit]), height=100, key=f"ep{client_edit}")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("💾", key=f"sc{client_edit}"):
                            st.session_state.custom_clients[client_edit] = [p.strip() for p in p_str.split("\n") if p.strip()]
                            st.rerun()
                    with c2:
                        if st.button("🗑️", key=f"dc{client_edit}"):
                            del st.session_state.custom_clients[client_edit]
                            st.rerun()
            
            with col2:
                st.subheader("📝 Dispositions")
                cat_edit = st.selectbox("Edit:", ["+ Add New"] + list(st.session_state.custom_dispositions.keys()), key="cate")
                if cat_edit == "+ Add New":
                    new_cat = st.text_input("Name:", key="ncat")
                    if st.button("Create", key="ccat") and new_cat and new_cat not in st.session_state.custom_dispositions:
                        st.session_state.custom_dispositions[new_cat] = []
                        st.rerun()
                else:
                    d_str = st.text_area("Items:", "\n".join(st.session_state.custom_dispositions[cat_edit]), height=100, key=f"ed{cat_edit}")
                    c3, c4 = st.columns(2)
                    with c3:
                        if st.button("💾", key=f"scat{cat_edit}"):
                            st.session_state.custom_dispositions[cat_edit] = [d.strip() for d in d_str.split("\n") if d.strip()]
                            st.rerun()
                    with c4:
                        if st.button("🗑️", key=f"dcat{cat_edit}"):
                            del st.session_state.custom_dispositions[cat_edit]
                            st.rerun()
            
            with col3:
                st.subheader("👤 Agents")
                agent_edit = st.selectbox("Edit Placement:", ["+ Add New"] + list(st.session_state.custom_agents.keys()), key="ae")
                if agent_edit == "+ Add New":
                    new_p = st.text_input("Placement:", key="npl")
                    if st.button("Create", key="cpl") and new_p and new_p not in st.session_state.custom_agents:
                        st.session_state.custom_agents[new_p] = []
                        st.rerun()
                else:
                    a_str = st.text_area("Agents:", "\n".join(st.session_state.custom_agents[agent_edit]), height=100, key=f"ea{agent_edit}")
                    c5, c6 = st.columns(2)
                    with c5:
                        if st.button("💾", key=f"sag{agent_edit}"):
                            st.session_state.custom_agents[agent_edit] = [a.strip() for a in a_str.split("\n") if a.strip()]
                            st.rerun()
                    with c6:
                        if st.button("🗑️", key=f"dag{agent_edit}"):
                            del st.session_state.custom_agents[agent_edit]
                            st.rerun()
            
            if st.button("🔄 Reset All", key="reset"):
                for k in ['custom_clients', 'custom_dispositions', 'custom_agents']:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()

        # Main form in 2-column layout
        main_col1, main_col2 = st.columns([1, 1])
        
        with main_col1:
            st.subheader("📋 Selection Criteria")
            
            placement_options = st.session_state.custom_clients
            all_dispositions = st.session_state.custom_dispositions
            all_agents = st.session_state.custom_agents
            
            # Client and Placement
            client = st.selectbox("Client:", list(placement_options.keys()), key="cs")
            placements = st.multiselect("Placement(s):", placement_options[client], key="ps")
            
            # Agents - Auto-populate based on selected placements
            available_agents = []
            for placement in placements:
                if placement in all_agents:
                    available_agents.extend(all_agents[placement])
            available_agents = sorted(list(set(available_agents)))  # Remove duplicates and sort
            
            selected_agents = st.multiselect("Agent(s):", available_agents if available_agents else ["No agents available"], 
                                            default=None, key="ags",
                                            disabled=not available_agents)
            
            # Status and Disposition
            all_disp_list = [d for cat in all_dispositions.values() for d in cat]
            selected_statuses = st.multiselect("Status(es):", 
                ["OVERALL", "NEGATIVE", "POSITIVE", "POTENTIAL ACCOUNTS", "NON-POTENTIAL ACCOUNTS", "BP's", "BROADCAST"], key="ss")
            
            # Auto-select dispositions based on status
            if selected_statuses != st.session_state.previous_statuses:
                st.session_state.previous_statuses = selected_statuses
                auto_disp = []
                for status in selected_statuses:
                    if status == "OVERALL":
                        auto_disp = all_disp_list
                        break
                    elif status == "NEGATIVE" and "NEGATIVE" in all_dispositions:
                        auto_disp.extend(all_dispositions["NEGATIVE"])
                    elif status == "POSITIVE" and "POSITIVE" in all_dispositions:
                        auto_disp.extend(all_dispositions["POSITIVE"])
                    elif status == "POTENTIAL ACCOUNTS":
                        if "POSITIVE" in all_dispositions:
                            auto_disp.extend(all_dispositions["POSITIVE"])
                        if "FIELD REQUEST" in all_dispositions:
                            auto_disp.extend(all_dispositions["FIELD REQUEST"])
                        auto_disp.extend([d for d in ["NEW", "REACTIVE"] if d in all_disp_list])
                    elif status == "NON-POTENTIAL ACCOUNTS" and "NEGATIVE" in all_dispositions:
                        auto_disp.extend(all_dispositions["NEGATIVE"])
                    elif status == "BP's" and "BP" in all_dispositions:
                        auto_disp.extend(all_dispositions["BP"])
                    elif status == "BROADCAST":
                        auto_disp.extend([d for d in ["PU", "PM"] if d in all_disp_list])
                st.session_state.selected_dispositions = list(set(auto_disp))
            
            selected_dispositions = st.multiselect("Disposition(s):", sorted(all_disp_list), 
                default=st.session_state.selected_dispositions, key="ds")
            st.session_state.selected_dispositions = selected_dispositions
            
            # Contact Types
            st.write("**Contact Types:**")
            contact_types = ["Employer", "FCO", "Guarantor", "House", "Mobile", "Office", "Other", "Reference", "Relative"]
            
            select_all = st.checkbox("✓ Select All", key="select_all")
            
            # Handle select/deselect all
            if select_all and set(st.session_state.selected_contacts) != set(contact_types):
                st.session_state.selected_contacts = contact_types.copy()
                st.rerun()
            elif not select_all and set(st.session_state.selected_contacts) == set(contact_types):
                st.session_state.selected_contacts = []
                st.rerun()
            
            # Compact 3-column checkbox layout
            ct_col1, ct_col2, ct_col3 = st.columns(3)
            selected_contacts = []
            for idx, contact in enumerate(contact_types):
                with [ct_col1, ct_col2, ct_col3][idx % 3]:
                    if st.checkbox(contact, value=contact in st.session_state.selected_contacts, key=f"ct_{contact}"):
                        selected_contacts.append(contact)
            
            st.session_state.selected_contacts = selected_contacts
            
            # Summary
            st.info(f"✓ {len(placements)} Placement(s) | {len(selected_agents)} Agent(s) | {len(selected_dispositions)} Disposition(s) | {len(selected_contacts)} Contact(s)")
            
            # Launch Button
            if st.button("🚀 Launch Playwright Browser", type="primary", use_container_width=True):
                errors = []
                if not placements: errors.append("placement")
                if not selected_agents: errors.append("agent")
                if not selected_dispositions: errors.append("disposition")
                if not selected_contacts: errors.append("contact type")
                
                if errors:
                    st.error(f"Please select at least one {', '.join(errors)}.")
                else:
                    st.success("✓ Playwright script started!")
                    st.write(f"**Client:** {client}")
                    st.write(f"**Placements:** {', '.join(placements)}")
                    st.write(f"**Agents:** {', '.join(selected_agents)}")
                    st.write(f"**Contacts:** {', '.join(selected_contacts)}")
                    subprocess.Popen([sys.executable, "test.py", "--client", client, "--placements", ",".join(placements),
                                    "--agents", ",".join(selected_agents), "--statuses", ",".join(selected_statuses), 
                                    "--dispositions", ",".join(selected_dispositions), "--contacts", ",".join(selected_contacts)])
        
        with main_col2:
            st.subheader("📊 Selected Dispositions")
            
            # Display selected dispositions table
            if selected_dispositions:
                import pandas as pd
                disp_data = [{"Category": cat, "Disposition": d} for cat, disps in all_dispositions.items() 
                            for d in disps if d in selected_dispositions]
                st.dataframe(pd.DataFrame(disp_data), use_container_width=True, height=600)
                st.caption(f"Total: {len(selected_dispositions)} disposition(s)")
            else:
                st.info("👆 Select a status or disposition to see them here")
                st.caption("Tip: Use the Status dropdown for quick selection")
        # --- Tab 18: Leads Counter ---
    with tab18:
        st.title("📊 Leads Counter")
        st.caption("Analyze your campaign leads data - Upload multiple Excel files to get insights per file")
        
        # Multiple file uploader
        uploaded_files = st.file_uploader(
            "Upload Excel Files", 
            type=['xlsx', 'xls'],
            help="Upload one or more Excel files containing Debtor ID and Phone Number columns",
            accept_multiple_files=True,
            key="leads_counter_upload"
        )
        
        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} file(s) uploaded successfully")
            
            # Option to view all files or individual files
            view_option = st.radio(
                "View Options:",
                ["📊 Summary of All Files", "📁 Individual File Analysis"],
                horizontal=True
            )
            
            if view_option == "📊 Summary of All Files":
                # Aggregate summary across all files
                st.subheader("📊 Aggregate Summary")
                
                total_unique_accounts = 0
                total_unique_contacts = 0
                total_dials_all = 0
                all_debtor_ids = set()
                all_phone_numbers = set()
                total_positive_all = 0
                total_ptp_all = 0
                
                summary_list = []
                
                for uploaded_file in uploaded_files:
                    try:
                        df = pd.read_excel(uploaded_file)
                        
                        if 'Debtor ID' in df.columns and 'Phone Number' in df.columns:
                            unique_accounts = df['Debtor ID'].nunique()
                            total_contacts = df['Phone Number'].nunique()
                            total_dials = len(df)
                            
                            # Add to aggregate sets
                            all_debtor_ids.update(df['Debtor ID'].dropna().unique())
                            all_phone_numbers.update(df['Phone Number'].dropna().unique())
                            total_dials_all += total_dials
                            
                            # Calculate penetration rate per file
                            if total_contacts > 0:
                                penetration_rate = (total_dials / total_contacts) * 100
                            else:
                                penetration_rate = 0
                            
                            # Try to get agent count from Call History sheet
                            agents_joined = 0
                            try:
                                call_history_df = pd.read_excel(uploaded_file, sheet_name='Call History')
                                if 'Collector Name' in call_history_df.columns:
                                    agents_joined = call_history_df['Collector Name'].nunique()
                            except Exception as call_error:
                                agents_joined = 0
                            
                            # Try to read Volare Status Summary sheet for Positive and PTP counts
                            positive_count = 0
                            ptp_count = 0
                            try:
                                volare_df = pd.read_excel(
                                    uploaded_file,
                                    sheet_name='Volare Status Summary'
                                )

                                # Normalize text
                                volare_df['Volare Status'] = (
                                    volare_df['Volare Status']
                                    .astype(str)
                                    .str.upper()
                                    .str.strip()
                                )

                                # Ensure Count is numeric
                                volare_df['Count'] = pd.to_numeric(
                                    volare_df['Count'],
                                    errors='coerce'
                                ).fillna(0)

                                # POSITIVE = any status that starts with or contains POSITIVE
                                positive_count = volare_df.loc[
                                    volare_df['Volare Status'].str.contains('POS CLIENT|POSITIVE CLIENT|RPC|POS 3RD PARTY', na=False),
                                    'Count'
                                ].sum()

                                # PTP = any status that starts with or contains PTP or PROMISE TO PAY
                                ptp_count = volare_df.loc[
                                    volare_df['Volare Status'].str.contains('PTP|PROMISE TO PAY', na=False),
                                    'Count'
                                ].sum()

                                total_positive_all += positive_count
                                total_ptp_all += ptp_count
                                
                            except Exception as sheet_error:
                                # Sheet not found or error reading it
                                positive_count = 0
                                ptp_count = 0
                            
                            summary_list.append({
                                'File Name': uploaded_file.name,
                                'Unique Accounts': unique_accounts,
                                'Total Contacts': total_contacts,
                                'Total Dials': total_dials,
                                'Agents Joined': agents_joined,
                                'Penetration Rate': f"{int(np.ceil(penetration_rate))}%",
                                'PTP Count': ptp_count,
                                'Positive Count': positive_count
                            })
                    except Exception as e:
                        st.warning(f"⚠️ Could not process {uploaded_file.name}: {str(e)}")
                
                # Calculate aggregate metrics
                total_unique_accounts = len(all_debtor_ids)
                total_unique_contacts = len(all_phone_numbers)
                
                if total_unique_contacts > 0:
                    overall_penetration = (total_dials_all / total_unique_contacts) * 100
                else:
                    overall_penetration = 0
                
                # Calculate total agents from summary list
                total_agents = sum(item['Agents Joined'] for item in summary_list)
                
                # Display aggregate metrics
                col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
                
                with col1:
                    st.metric(
                        label="👥 Total Unique Accounts",
                        value=f"{total_unique_accounts:,}",
                        help="Unique Debtor IDs across all files"
                    )
                
                with col2:
                    st.metric(
                        label="📞 Total Unique Contacts",
                        value=f"{total_unique_contacts:,}",
                        help="Unique Phone Numbers across all files"
                    )
                
                with col3:
                    st.metric(
                        label="📊 Total Dials",
                        value=f"{total_dials_all:,}",
                        help="Total records across all files"
                    )
                
                with col4:
                    st.metric(
                        label="👤 Total Agents",
                        value=f"{total_agents:,}",
                        help="Total unique agents who joined across all files"
                    )
                
                with col5:
                    st.metric(
                        label="🤝 Total PTP",
                        value=f"{total_ptp_all:,}",
                        help="Total Promise To Pay across all files"
                    )
                
                with col6:
                    st.metric(
                        label="✅ Total Positive",
                        value=f"{total_positive_all:,}",
                        help="Total Positive statuses across all files"
                    )
                
                with col7:
                    st.metric(
                        label="📈 Overall Penetration",
                        value=f"{int(np.ceil(overall_penetration))}%",
                        help="Aggregate penetration rate across all files"
                    )
                
                st.divider()
                
                # Display summary table
                if summary_list:
                    st.subheader("📋 Per-File Summary")
                    summary_df = pd.DataFrame(summary_list)
                    
                    # Add totals row
                    totals_row = pd.DataFrame([{
                        'File Name': '📊 TOTAL',
                        'Unique Accounts': summary_df['Unique Accounts'].sum(),
                        'Total Contacts': summary_df['Total Contacts'].sum(),
                        'Total Dials': summary_df['Total Dials'].sum(),
                        'Agents Joined': summary_df['Agents Joined'].sum(),
                        'Penetration Rate': f"{int(np.ceil(overall_penetration))}%",
                        'PTP Count': summary_df['PTP Count'].sum(),
                        'Positive Count': summary_df['Positive Count'].sum()
                    }])
                    
                    summary_df = pd.concat([summary_df, totals_row], ignore_index=True)
                    
                    st.dataframe(
                        summary_df,
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Export aggregate summary
                    with st.expander("📥 Export Aggregate Summary"):
                        csv = summary_df.to_csv(index=False)
                        st.download_button(
                            label="📊 Download Summary as CSV",
                            data=csv,
                            file_name=f"leads_aggregate_summary_{len(uploaded_files)}_files.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
            
            else:  # Individual File Analysis
                st.subheader("📁 Individual File Analysis")
                
                # File selector
                selected_file_name = st.selectbox(
                    "Select a file to analyze:",
                    [f.name for f in uploaded_files],
                    key="file_selector"
                )
                
                # Find the selected file
                selected_file = next((f for f in uploaded_files if f.name == selected_file_name), None)
                
                if selected_file:
                    try:
                        # Read the Excel file
                        df = pd.read_excel(selected_file)
                        
                        # Check for required columns
                        required_columns = ['Debtor ID', 'Phone Number']
                        missing_columns = [col for col in required_columns if col not in df.columns]
                        
                        if missing_columns:
                            st.error(f"❌ Missing required columns in {selected_file_name}: {', '.join(missing_columns)}")
                            st.info("**Available columns in your file:**")
                            st.write(df.columns.tolist())
                        else:
                            # Calculate metrics
                            unique_accounts = df['Debtor ID'].nunique()
                            total_contacts = df['Phone Number'].nunique()
                            total_dials = len(df)
                            
                            # Calculate penetration rate (dials/contacts as percentage)
                            if total_contacts > 0:
                                penetration_rate = (total_dials / total_contacts) * 100
                            else:
                                penetration_rate = 0
                            
                            # Try to get agent count from Call History sheet
                            agents_joined = 0
                            call_history_found = False
                            
                            try:
                                call_history_df = pd.read_excel(selected_file, sheet_name='Call History')
                                call_history_found = True
                                if 'Collector Name' in call_history_df.columns:
                                    agents_joined = call_history_df['Collector Name'].nunique()
                            except Exception as call_error:
                                # Sheet not found or error reading it
                                agents_joined = 0
                            
                            # Try to read Volare Status Summary sheet for Positive and PTP counts
                            positive_count = 0
                            ptp_count = 0
                            volare_sheet_found = False
                            
                            try:
                                volare_df = pd.read_excel(selected_file, sheet_name='Volare Status Summary')
                                volare_sheet_found = True
                                
                                # Check all columns for status information
                                for col in volare_df.columns:
                                    col_data = volare_df[col].astype(str).str.upper()
                                    
                                    # Count rows containing "POSITIVE"
                                    positive_count += col_data.str.contains('POSITIVE', na=False).sum()
                                    
                                    # Count rows containing "PTP" or "PROMISE TO PAY"
                                    ptp_count += col_data.str.contains('PTP|PROMISE TO PAY', na=False, regex=True).sum()
                                
                            except Exception as sheet_error:
                                # Sheet not found or error reading it
                                st.warning(f"⚠️ 'Volare Status Summary' sheet not found in {selected_file_name}. Positive and PTP counts will be 0.")
                            
                            if not call_history_found:
                                st.warning(f"⚠️ 'Call History' sheet not found in {selected_file_name}. Agents Joined count will be 0.")
                            
                            st.divider()
                            
                            # Display metrics in columns
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric(
                                    label="👥 Unique Accounts",
                                    value=f"{unique_accounts:,}",
                                    help="Total unique Debtor IDs in the file"
                                )
                                st.metric(
                                    label="📞 Total Contacts",
                                    value=f"{total_contacts:,}",
                                    help="Total unique Phone Numbers in the file"
                                )
                            
                            with col2:
                                st.metric(
                                    label="📈 Penetration Rate",
                                    value=f"{int(np.ceil(penetration_rate))}%",
                                    help="Calculated as: (Total Dials / Total Contacts) × 100"
                                )
                            
                            with col3:
                                st.metric(
                                    label="📊 Total Dials",
                                    value=f"{total_dials:,}",
                                    help="Total number of rows/records in the file"
                                )
                                st.metric(
                                    label="👤 Agents Joined",
                                    value=f"{agents_joined:,}",
                                    help="Number of unique agents from Call History sheet"
                                )
                            
                            with col4:
                                st.metric(
                                    label="🤝 PTP Count",
                                    value=f"{ptp_count:,}",
                                    help="Count of 'PTP' or 'Promise To Pay' from Volare Status Summary sheet"
                                )
                                st.metric(
                                    label="✅ Positive Count",
                                    value=f"{positive_count:,}",
                                    help="Count of statuses containing 'POSITIVE' from Volare Status Summary sheet"
                                )
                            
                            st.divider()
                            
                            # Additional insights section
                            st.subheader("📋 Additional Insights")
                            
                            insight_col1, insight_col2 = st.columns(2)
                            
                            with insight_col1:
                                st.info(f"**Average Dials per Account:** {total_dials / unique_accounts:.2f}" if unique_accounts > 0 else "N/A")
                                st.info(f"**Average Dials per Contact:** {total_dials / total_contacts:.2f}" if total_contacts > 0 else "N/A")
                                st.info(f"**Average Contacts per Account:** {total_contacts / unique_accounts:.2f}" if unique_accounts > 0 else "N/A")
                            
                            with insight_col2:
                                st.info(f"**Total Records:** {len(df):,}")
                                if volare_sheet_found:
                                    ptp_rate = (ptp_count / total_dials * 100) if total_dials > 0 else 0
                                    positive_rate = (positive_count / total_dials * 100) if total_dials > 0 else 0
                                    st.info(f"**PTP Rate:** {ptp_rate:.2f}%")
                                    st.info(f"**Positive Rate:** {positive_rate:.2f}%")
                            
                            st.divider()
                            
                            # Data preview section
                            with st.expander("👀 Preview Main Data", expanded=False):
                                st.dataframe(
                                    df.head(100),
                                    use_container_width=True,
                                    height=400
                                )
                                st.caption(f"Showing first 100 rows of {len(df):,} total records")
                            
                            # Volare Status Summary preview
                            if volare_sheet_found:
                                with st.expander("👀 Preview Volare Status Summary", expanded=False):
                                    st.dataframe(
                                        volare_df.head(100),
                                        use_container_width=True,
                                        height=400
                                    )
                                    st.caption(f"Showing first 100 rows of {len(volare_df):,} total records")
                            
                            # Export summary
                            with st.expander("📥 Export Summary", expanded=False):
                                summary_data = {
                                    'Metric': [
                                        'File Name',
                                        'Unique Accounts',
                                        'Total Contacts',
                                        'Total Dials',
                                        'Positive Count',
                                        'PTP Count',
                                        'Penetration Rate',
                                        'Avg Dials per Account',
                                        'Avg Dials per Contact',
                                        'Avg Contacts per Account',
                                        'Positive Rate',
                                        'PTP Rate',
                                        'Total Records'
                                    ],
                                    'Value': [
                                        selected_file_name,
                                        unique_accounts,
                                        total_contacts,
                                        total_dials,
                                        positive_count,
                                        ptp_count,
                                        f"{int(np.ceil(penetration_rate))}%",
                                        round(total_dials / unique_accounts, 2) if unique_accounts > 0 else 0,
                                        round(total_dials / total_contacts, 2) if total_contacts > 0 else 0,
                                        round(total_contacts / unique_accounts, 2) if unique_accounts > 0 else 0,
                                        f"{round((positive_count / total_dials * 100), 2)}%" if total_dials > 0 else "0%",
                                        f"{round((ptp_count / total_dials * 100), 2)}%" if total_dials > 0 else "0%",
                                        len(df)
                                    ]
                                }
                                
                                summary_df = pd.DataFrame(summary_data)
                                
                                # Convert to CSV for download
                                csv = summary_df.to_csv(index=False)
                                
                                st.download_button(
                                    label="📊 Download Summary as CSV",
                                    data=csv,
                                    file_name=f"leads_summary_{selected_file_name.split('.')[0]}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                                
                                st.dataframe(summary_df, use_container_width=True)
                            
                            # Visualizations
                            st.divider()
                            st.subheader("📊 Visualizations")
                            
                            viz_col1, viz_col2 = st.columns(2)
                            
                            with viz_col1:
                                # Pie chart for metrics distribution
                                fig_pie = plt.figure(figsize=(8, 6))
                                metrics_values = [unique_accounts, total_contacts, total_dials]
                                metrics_labels = ['Unique Accounts', 'Total Contacts', 'Total Dials']
                                colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
                                
                                plt.pie(metrics_values, labels=metrics_labels, autopct='%1.1f%%', 
                                       colors=colors, startangle=90)
                                plt.title(f'Distribution - {selected_file_name}')
                                st.pyplot(fig_pie)
                                plt.close()
                            
                            with viz_col2:
                                # Bar chart for metrics comparison
                                fig_bar = plt.figure(figsize=(8, 6))
                                plt.bar(metrics_labels, metrics_values, color=colors)
                                plt.title(f'Metrics - {selected_file_name}')
                                plt.ylabel('Count')
                                plt.xticks(rotation=15, ha='right')
                                plt.tight_layout()
                                st.pyplot(fig_bar)
                                plt.close()
                            
                            # Additional visualization for Positive/PTP if available
                            if volare_sheet_found and (positive_count > 0 or ptp_count > 0):
                                st.divider()
                                viz_col3, viz_col4 = st.columns(2)
                                
                                with viz_col3:
                                    # PTP vs Positive pie chart
                                    fig_status = plt.figure(figsize=(8, 6))
                                    status_values = [ptp_count, positive_count]
                                    status_labels = ['PTP', 'Positive']
                                    status_colors = ['#F39C12', '#2ECC71']
                                    
                                    plt.pie(status_values, labels=status_labels, autopct='%1.1f%%',
                                           colors=status_colors, startangle=90)
                                    plt.title('PTP vs Positive Distribution')
                                    st.pyplot(fig_status)
                                    plt.close()
                                
                                with viz_col4:
                                    # Status rates bar chart
                                    fig_rates = plt.figure(figsize=(8, 6))
                                    rate_labels = ['PTP Rate', 'Positive Rate']
                                    rate_values = [
                                        (ptp_count / total_dials * 100) if total_dials > 0 else 0,
                                        (positive_count / total_dials * 100) if total_dials > 0 else 0
                                    ]
                                    plt.bar(rate_labels, rate_values, color=status_colors)
                                    plt.title('Success Rates (%)')
                                    plt.ylabel('Percentage')
                                    plt.tight_layout()
                                    st.pyplot(fig_rates)
                                    plt.close()
                    
                    except Exception as e:
                        st.error(f"❌ Error processing file: {str(e)}")
                        st.info("Please ensure your Excel file contains 'Debtor ID' and 'Phone Number' columns.")
        
        else:
            # Instructions when no file is uploaded
            st.info("👆 Please upload one or more Excel files to begin analysis")
            
            st.markdown("""
            ### 📖 Instructions:
            
            1. **Upload your Excel files** (.xlsx or .xls format) - you can select multiple files
            2. Ensure your files contain these columns:
               - **Main Sheet**: `Debtor ID` and `Phone Number` columns (required)
               - **Call History Sheet**: `Collector Name` column for agent count (optional)
               - **Volare Status Summary Sheet**: Contains status information for Positive and PTP counts (optional)
            3. Choose between:
               - **Summary of All Files**: View aggregate metrics across all uploaded files
               - **Individual File Analysis**: Analyze each file separately with detailed insights
            4. The tool will automatically calculate:
               - **Unique Accounts**: Count of unique Debtor IDs
               - **Total Contacts**: Count of unique Phone Numbers
               - **Total Dials**: Total number of rows in the file
               - **Agents Joined**: Number of unique agents from Call History sheet
               - **Positive Count**: Number of statuses containing "POSITIVE" in Volare Status Summary
               - **PTP Count**: Number of statuses containing "PTP" or "Promise To Pay" in Volare Status Summary
               - **Penetration Rate**: (Total Dials ÷ Total Contacts) × 100 (rounded up)
               - **Positive Rate**: (Positive Count ÷ Total Dials) × 100
               - **PTP Rate**: (PTP Count ÷ Total Dials) × 100
            
            ### 💡 What is Penetration Rate?
            
            The penetration rate indicates how many times, on average, each contact number 
            has been dialed. A higher percentage means more intensive dialing activity.
            
            **Formula:** `Penetration Rate = (Total Dials / Total Contacts) × 100`
            
            ### ✅ Positive & PTP Counts
            
            The system searches for any status containing "POSITIVE" or "PTP"/"Promise To Pay" 
            in the **Volare Status Summary** sheet, capturing all variations automatically.
            
            ### 👤 Agents Joined
            
            The system counts unique agents from the **Call History** sheet's `Collector Name` column,
            showing how many agents participated in the predictive dialer campaign.
            """)
            
            # Sample data format
            with st.expander("📄 Sample Data Format"):
                st.write("**Main Sheet (Sheet1):**")
                sample_data = pd.DataFrame({
                    'Debtor ID': ['ACC001', 'ACC001', 'ACC002', 'ACC002', 'ACC003'],
                    'Phone Number': ['09171234567', '09281234567', '09171234567', '09391234567', '09171234567'],
                    'Other Column 1': ['Data', 'Data', 'Data', 'Data', 'Data'],
                    'Other Column 2': ['Data', 'Data', 'Data', 'Data', 'Data']
                })
                st.dataframe(sample_data, use_container_width=True)
                
                st.write("**Call History Sheet:**")
                sample_call_history = pd.DataFrame({
                    'Collector Name': ['AGENT001', 'AGENT002', 'AGENT001', 'AGENT003'],
                    'Call Time': ['10:00 AM', '10:15 AM', '10:30 AM', '10:45 AM'],
                    'Duration': ['120s', '90s', '150s', '60s']
                })
                st.dataframe(sample_call_history, use_container_width=True)
                
                st.write("**Volare Status Summary Sheet:**")
                sample_volare = pd.DataFrame({
                    'Account': ['ACC001', 'ACC002', 'ACC003'],
                    'Status': ['POSITIVE CONTACT', 'PTP - PROMISE TO PAY', 'POSITIVE - CALLBACK'],
                    'Date': ['2024-01-15', '2024-01-16', '2024-01-17']
                })
                st.dataframe(sample_volare, use_container_width=True)
                st.caption("The Call History and Volare Status Summary sheets are optional but recommended for complete tracking")
