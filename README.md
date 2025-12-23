# IntelliStock

**AI-Driven Inventory Health & Stock-Out Alert System**

Built for Snowflake AI for Good Hackathon

---

## 📋 Overview

IntelliStock is a **decision-support system** designed to help organizations managing essential goods (hospitals, NGOs, public distribution systems) **know what to do first** when stock-outs threaten service delivery.

**Unlike traditional dashboards** that show "what's happening," IntelliStock answers:

- **What matters most?** → Action priority scoring ranks items by urgency
- **What should I act on first?** → Today's Action Panel shows top 3 priorities
- **Why is that action urgent?** → Clear, rule-based explanations

### Problem Statement

Organizations managing essential supplies face:

- **Delayed detection** of stock-outs leading to service disruptions
- **Information overload** with dozens of alerts requiring manual prioritization
- **Reactive procurement** decisions instead of proactive, prioritized action
- **Cognitive burden** on time-pressed staff to interpret analytics

### Solution

IntelliStock provides **decision-first intelligence**:

- **Early risk detection** using rule-based analytics
- **Visual dashboards** showing inventory health across locations
- **Actionable recommendations** with calculated reorder quantities
- **Deterministic explanations** for every alert

---

## ✨ Features

### 1. Overview Metrics

- Total organizations tracked
- Total items monitored
- Count of HIGH-risk alerts

### 2. Inventory Heatmap

- Visual representation of closing stock by item and location
- Color-coded intensity for quick identification of low stock

### 3. Stock-Out Alerts

- Filtered view of HIGH-risk items (days left ≤ lead time)
- Detailed metrics: avg daily usage, days left, lead time
- Rule-based explanations for each alert

### 4. Reorder Recommendations

- Calculated reorder quantities for items at risk
- Urgency levels (CRITICAL, HIGH, MEDIUM)
- Summary analytics by urgency

---

## 🎯 How IntelliStock Helps Decide What to Do First

### Traditional Dashboards Show Data

❌ Display 45 high-risk alerts  
❌ User must manually interpret and prioritize  
❌ Cognitive overload for time-pressed staff  
❌ Risk of overlooking critical items

### IntelliStock Provides Decisions

✅ **Action Priority Scoring** → Deterministic ranking using lead time, usage patterns, and criticality  
✅ **Today's Action Panel** → Immediately shows top 3 most urgent actions  
✅ **Clear Explanations** → Rule-based reasoning for every recommendation  
✅ **Zero Manual Prioritization** → Decision ready in 30 seconds

**Example:**  
Instead of: _"45 items flagged as high-risk - analyze spreadsheet to decide priorities"_  
IntelliStock shows: _"Reorder Insulin at City Hospital – Emergency Unit (Priority: 42.3)"_

**Result:** Hospital staff can act immediately without analyzing dashboards or comparing metrics.

---

## 🎯 Impact: AI for Good

IntelliStock supports organizations serving communities by:

- **Preventing stock-outs** of essential medicines and supplies
- **Optimizing procurement** to reduce waste and costs
- **Improving service delivery** for healthcare and aid organizations
- **Enabling data-driven decisions** without requiring ML expertise

---

## 🏗️ Architecture

```
┌─────────────────┐
│  Streamlit App  │  (Local execution)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Snowflake DB  │  (Cloud data warehouse)
│                 │
│  • INVENTORY    │  (Table)
│  • Analytics    │  (SQL queries)
└─────────────────┘
```

**Key Design Principles:**

- SQL is the source of truth for all analytics logic
- No ML models or probabilistic forecasts
- Deterministic, rule-based calculations
- Filters slice data without altering logic

---

## 📊 Analytics Logic

All calculations are performed in SQL:

### Average Daily Usage

```sql
AVG(issued) OVER (PARTITION BY organization, location, item)
```

### Days of Stock Left

```sql
closing_stock / avg_daily_usage
```

### Risk Status

```sql
CASE WHEN days_left <= lead_time_days
     THEN 'HIGH'
     ELSE 'NORMAL'
END
```

### Reorder Quantity

```sql
GREATEST(0, (lead_time_days * avg_daily_usage) - closing_stock)
```

---

## 🚀 Setup Instructions

### Prerequisites

- Python 3.8 or higher
- Snowflake account with browser-based SSO access
- Git (optional, for version control)

### 1. Clone Repository

```bash
git clone https://github.com/udaykumar0515/intellistock-ai-for-good.git
cd intellistock-ai-for-good
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Snowflake Connection

Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=AQZZSNT-TF92378
SNOWFLAKE_USER=UDAYKUMARH
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=INTELLISTOCK_DB
SNOWFLAKE_SCHEMA=PUBLIC
```

**Important:** The `.env` file is excluded from Git (.gitignore) to protect credentials.

### 4. Run the Application

```bash
streamlit run app.py
```

The application will open in your browser at `http://localhost:8501`

### 5. Initialize Database (First Run)

In the Streamlit app sidebar:

1. Click **"Test Snowflake Connection"** to verify connectivity
2. Click **"Initialize Database"** to create tables (uses idempotent DDL)
3. Click **"Load Sample Data"** to load the synthetic inventory dataset

---

## 📁 Project Structure

```
intellistock-ai-for-good/
├── app.py                      # Main Streamlit application
├── snowflake_connector.py      # Snowflake connection management
├── .env.example                # Environment variable template
├── .env                        # Your credentials (not in Git)
├── .gitignore                  # Excludes .env and sensitive files
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── sql/
│   ├── create_tables.sql       # DDL for database setup
│   └── analytics.sql           # Analytics queries (source of truth)
├── data/
│   └── inventory_sample.csv    # Synthetic inventory data
└── utils/
    └── calculations.py         # Validation functions (not used in app)
```

---

## 📝 Data Schema

### INVENTORY Table

| Column         | Type    | Description               |
| -------------- | ------- | ------------------------- |
| date           | DATE    | Transaction date          |
| organization   | STRING  | Organization name         |
| location       | STRING  | Storage location          |
| item           | STRING  | Item name                 |
| opening_stock  | INTEGER | Stock at start of day     |
| received       | INTEGER | Quantity received         |
| issued         | INTEGER | Quantity issued/consumed  |
| closing_stock  | INTEGER | Stock at end of day       |
| lead_time_days | INTEGER | Supplier lead time (days) |

### Sample Data

The included dataset contains:

- **3 organizations**: City Hospital, Regional NGO, Community Clinic
- **3 locations**: Main Warehouse, Emergency Unit, Outreach Center
- **7 items**: Paracetamol, Insulin, Rice, Masks, Gloves, Bandages, Syringes
- **8 days** of records (2025-12-08 to 2025-12-15)
- **76 total rows** with intentional HIGH-risk scenarios

---

## 🔍 Verification

### Success Criteria

The application is working correctly if:
✅ Metrics in Streamlit exactly match SQL query outputs  
✅ All analytics logic is traceable to SQL (not Python)  
✅ Filters slice data without altering calculations

### Manual Testing

1. **Test Snowflake connection** using sidebar button
2. **Run SQL queries directly** in Snowflake console (from `sql/analytics.sql`)
3. **Compare results** with Streamlit dashboard
4. **Apply filters** and verify calculations remain consistent
5. **Check explanations** are deterministic and rule-based

---

## 🚫 Non-Scope (Hackathon Prototype)

This hackathon prototype **does NOT** include:

- User authentication or role management
- CRUD operations or transaction handling
- POS/billing systems
- Email/SMS notifications
- External API integrations
- ML models or predictive forecasting
- Snowflake Marketplace deployment
- Advanced error handling or monitoring

These features are intentionally excluded as this is a hackathon prototype demonstrating core analytics capabilities.

---

## 🛠️ Troubleshooting

### "Missing required environment variables"

**Solution:** Create a `.env` file based on `.env.example` with your Snowflake credentials.

### "Failed to connect to Snowflake"

**Solution:**

- Verify your Snowflake account and credentials
- Ensure browser-based SSO (`externalbrowser`) is configured
- Check network connectivity

### "No data available"

**Solution:** Click "Load Sample Data" in the sidebar to import `data/inventory_sample.csv`.

### "Query execution failed"

**Solution:**

- Ensure database and tables are initialized
- Verify your Snowflake role has necessary permissions
- Check SQL syntax in error details

---

## 📚 Additional Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Project Repository](https://github.com/udaykumar0515/intellistock-ai-for-good)

---

## 📄 License

This project was created for the Snowflake AI for Good Hackathon.

---

## 🙏 Acknowledgments

- **Snowflake** for providing the cloud data platform
- **AI for Good Hackathon** organizers for the opportunity
- Organizations serving communities with essential goods

---

**Built with ❤️ for social impact**
