# 📊 TallyInsightEngine – AI-Powered Financial Data Extraction & Query Tool

**TallyInsightEngine** is a smart and efficient platform that bridges traditional accounting systems with modern AI-powered analytics. It automates the extraction of financial data from **Tally ERP** and stores it in a **PostgreSQL** database, enabling powerful **natural language querying** through both a **desktop GUI** and a **web-based interface**. Designed for accountants, analysts, and businesses, this tool provides seamless access to insights from your accounting system—intuitively and intelligently.

---

## 🚀 Features

- 🔄 **Automatic Data Extraction** from Tally ERP into PostgreSQL  
- 🧠 **AI-Based Natural Language Querying** using local LLMs (like Mistral, LLaMA, Dolphin)  
- 💻 **Desktop GUI** (Tkinter) for offline access and analysis  
- 🌐 **Web Interface** (Flask) for remote querying and dashboards  
- 📂 **Supports Multiple Data Sources**: Tally ERP, Excel, CSV, PostgreSQL, MySQL  
- 🔁 **Real-Time / Scheduled Updates** for syncing latest accounting data  
- 🔒 **Secure Web Deployment** with SSL (optional)  

---

## 🛠️ Tech Stack

| Component       | Technology                        |
|----------------|------------------------------------|
| Language        | Python 3.8+                        |
| GUI             | Tkinter                            |
| Backend         | Flask (REST API)                   |
| Database        | PostgreSQL                         |
| AI Layer        | Ollama + LLMs (Mistral, LLaMA3.2) |
| NLP             | FastText (Language Detection)      |
| Data Handling   | Pandas, psycopg2                   |

---

## 📦 Installation

```bash
git clone https://github.com/yourusername/TallyInsightEngine.git
cd TallyInsightEngine
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
