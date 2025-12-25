# IntelliStock v2.1 - AI-Driven Inventory Health & Stock-Out Alert System

> **Hackathon Submission:** Snowflake AI for Good Hackathon 2024  
> **Version:** 2.1 (Multi-Page Architecture)  
> **Status:** Production Ready ✅

## 🎯 Overview

IntelliStock is an intelligent inventory management system designed for essential goods organizations (hospitals, NGOs, government agencies). It provides real-time stock-out predictions, priority-based action recommendations, and data-driven decision support.

### Key Features

- **📊 Multi-Page Dashboard:** Clean, professional UI with dedicated pages for different functions
- **🎯 Today's Action Panel:** Top 3 priority items requiring immediate attention
- **📈 7-Day Trend Visualization:** Sparkline charts showing stock movement patterns
- **🧮 What-If Calculator:** Project inventory coverage based on order quantities
- **📄 PDF Export:** Download action items for offline sharing
- **📁 CSV Upload:** Custom data upload with comprehensive validation
- **⚙️ Configurable Scoring:** Customize criticality rules via JSON or UI
- **📦 Mark as Ordered:** Track items you've already ordered

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Snowflake account with credentials
- Git (for cloning)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/intellistock-ai-for-good.git
   cd intellistock-ai-for-good
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   ```

3. **Activate virtual environment:**
   - Windows:
     ```bash
     venv\Scripts\activate
     ```
   - macOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure Snowflake credentials:**
   - Copy `.env.example` to `.env`
   - Fill in your Snowflake credentials:
     ```
     SNOWFLAKE_ACCOUNT=your_account
     SNOWFLAKE_USER=your_username
     SNOWFLAKE_PASSWORD=your_password
     SNOWFLAKE_DATABASE=your_database
     SNOWFLAKE_SCHEMA=your_schema
     SNOWFLAKE_WAREHOUSE=your_warehouse
     ```

### Running the Application

**Option 1: Using the batch file (Windows):**
```bash
run.bat
```

**Option 2: Manual command:**
```bash
venv\Scripts\python.exe -m streamlit run Home.py
```

**Option 3: macOS/Linux:**
```bash
source venv/bin/activate
python -m streamlit run Home.py
```

The app will open automatically in your browser at `http://localhost:8503`

---

## 📁 Project Structure

```
intellistock-ai-for-good/
├── Home.py                      # Landing page with navigation
├── pages/
│   ├── 1__Dashboard.py          # Analytics & decision support
│   ├── 2__Data_Management.py    # CSV upload & validation
│   └── 3__Configuration.py      # Criticality scoring editor
├── utils/
│   ├── calculations.py          # Business logic helpers
│   └── csv_validator.py         # Data validation logic
├── data/
│   └── inventory_sample.csv     # Sample dataset
├── sql/
│   ├── create_tables.sql        # Database schema
│   └── analytics_queries.sql    # Core analytical queries
├── snowflake_connector.py       # Database connection handler
├── criticality_config.json      # Scoring rules configuration
├── requirements.txt             # Python dependencies
├── run.bat                      # Windows startup script
├── .env.example                 # Environment template
└── README.md                    # This file
```

---

## 🗂️ Application Pages

### 🏠 Home
Landing page with navigation to all sections and feature overview.

### 📊 Dashboard
Real-time analytics and decision support:
- **Today's Action Panel:** Top 3 priority reorder recommendations
- **Overview Metrics:** Total organizations, items, high-risk alerts
- **Inventory Heatmap:** Visual representation of stock levels
- **What-If Calculator:** Projection tool for order planning
- **Stock-Out Alerts:** Full list with trends and details
- **Reorder Recommendations:** Suggested order quantities

### 📁 Data Management
Upload and manage inventory data:
- **CSV Upload:** Drag-and-drop or browse to upload
- **Schema Validation:** Automatic checks for data quality
- **Data Preview:** View first 20 rows before loading
- **Database Tools:** Test connection, initialize tables, load sample data
- **Organization Profile:** Capture metadata about your organization

### ⚙️ Configuration
Customize priority scoring rules:
- **Location Rules:** Assign criticality scores to locations
- **Item Rules:** Define critical item categories
- **Default Score:** Set baseline criticality
- **Save/Reset:** Persist changes or restore defaults

---

## 📊 Data Schema

### Required CSV Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `date` | DATE | Transaction date | 2024-01-15 |
| `organization` | STRING | Organization name | City Hospital |
| `location` | STRING | Warehouse/clinic | Emergency Unit |
| `item` | STRING | Product name | Paracetamol |
| `opening_stock` | INTEGER | Stock at start | 100 |
| `received` | INTEGER | Units received | 50 |
| `issued` | INTEGER | Units distributed | 30 |
| `closing_stock` | INTEGER | Stock at end | 120 |
| `lead_time_days` | INTEGER | Supplier lead time | 7 |

### Validation Rules

- ✅ All 9 columns required (case-insensitive)
- ✅ Dates must be YYYY-MM-DD format
- ✅ Stock values must be non-negative integers
- ✅ No empty organization/location/item names
- ⚠️ Formula check: closing = opening + received - issued (warning if mismatch)

---

## 🧮 Priority Scoring Formula

```
Priority Score = (Lead Time × 2) + (Daily Usage × 1.5) + Criticality - (Current Stock × 0.5)
```

**Components:**
- **Lead Time (×2):** Longer supplier delivery times increase urgency
- **Daily Usage (×1.5):** Higher consumption rates increase urgency
- **Criticality:** Location/item importance (configurable 1-15)
- **Current Stock (×0.5):** Lower stock increases urgency

**Example:**
```
Item: Paracetamol @ Emergency Unit
Lead Time: 10 days
Daily Usage: 20 units/day
Criticality: 10 (Emergency Unit) + 7 (Critical Medicine) = 10 (max)
Current Stock: 15 units

Priority = (10 × 2) + (20 × 1.5) + 10 - (15 × 0.5)
         = 20 + 30 + 10 - 7.5
         = 52.5 (HIGH PRIORITY)
```

---

## 🔧 Configuration

### Criticality Scoring (`criticality_config.json`)

Customize how locations and items are prioritized:

```json
{
  "location_rules": [
    {
      "pattern": "Emergency Unit",
      "score": 10,
      "description": "Critical emergency care location"
    }
  ],
  "item_rules": [
    {
      "items": ["Paracetamol", "Insulin", "Syringes"],
      "score": 7,
      "description": "Critical medical supplies"
    }
  ],
  "default_score": 3
}
```

**Editing:**
- Via UI: Configuration page → Edit scores → Save
- Via JSON: Edit `criticality_config.json` → Restart app

---

## 📦 Dependencies

```
streamlit>=1.28.0
snowflake-connector-python>=3.0.0
pandas>=2.0.0
plotly>=5.14.0
python-dotenv>=1.0.0
reportlab>=4.0.0
```

Install all with: `pip install -r requirements.txt`

---

## 🧪 Testing

### Quick Test Workflow

1. **Start the app:** `run.bat` or `python -m streamlit run Home.py`
2. **Go to Data Management**
3. **Click "Initialize Database"** (first time only)
4. **Click "Load Sample Data"**
5. **Go to Dashboard** to see analytics
6. **Test features:**
   - Mark an item as ordered
   - Use What-If Calculator
   - Export PDF
   - Adjust criticality scores in Configuration

### Sample Data

The `data/inventory_sample.csv` contains:
- **3 organizations:** City Hospital, Rural Health Center, Community Clinic
- **7 items:** Paracetamol, Bandages, Syringes, Masks, Gloves, Insulin, Rice
- **Multiple locations:** Emergency Unit, Main Warehouse, Outpatient, etc.
- **30+ days** of transaction history

---

## 🚀 Deployment

### Streamlit Cloud

1. Push code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Deploy from repository
4. Add Snowflake secrets in Streamlit settings

### Snowflake Native App (Future)

This app can be packaged as a Snowflake Native App for:
- Seamless installation via Snowflake Marketplace
- Built-in data sharing across organizations
- Native Snowflake authentication

---

## 🔒 Security Notes

- **Credentials:** Never commit `.env` file (already in `.gitignore`)
- **SQL Injection:** Current implementation uses string escaping; parameterized queries recommended for production
- **Access Control:** Implement Snowflake RBAC for multi-user deployments
- **Data Privacy:** Ensure compliance with healthcare data regulations (HIPAA, GDPR)

---

## 📝 Version History

### v2.1 (Current) - Multi-Page Architecture
- ✅ Restructured into multi-page Streamlit app
- ✅ Added CSV upload with validation
- ✅ Added organization metadata capture
- ✅ Created dedicated Data Management page
- ✅ Created Configuration page for scoring rules
- ✅ Improved UX with clean, organized pages

### v2.0 - Decision Support Features
- ✅ Mark as Ordered tracking
- ✅ 7-day historical sparklines
- ✅ What-If order calculator
- ✅ PDF export for action panel
- ✅ Configurable criticality scoring

### v1.0 - Core Analytics
- ✅ Snowflake integration
- ✅ Stock-out risk prediction
- ✅ Priority scoring algorithm
- ✅ Today's Action Panel
- ✅ Inventory heatmap

---

## 🤝 Contributing

This is a hackathon project, but suggestions are welcome!

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## 📄 License

This project is open source and available for use by humanitarian organizations.

---

## 👥 Team

**Hackathon Project:** Snowflake AI for Good 2024

Built with ❤️ for essential goods organizations worldwide.

---

## 🆘 Troubleshooting

### Common Issues

**"Module not found" errors:**
```bash
# Ensure venv is activated
venv\Scripts\activate

# Reinstall dependencies
pip install -r requirements.txt
```

**Snowflake connection fails:**
- Check `.env` credentials
- Verify Snowflake account is active
- Test with: Data Management → Test Connection

**Pages don't load:**
- Ensure running with venv Python: `venv\Scripts\python.exe -m streamlit run Home.py`
- Check console for import errors
- Restart Streamlit server

**Data doesn't refresh:**
- Click browser refresh (F5)
- Verify data loaded: Data Management → Load Sample Data
- Check Snowflake query logs

---

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**🎉 Thank you for using IntelliStock!**

*Making inventory management intelligent, one stock-out alert at a time.* 📦✨
