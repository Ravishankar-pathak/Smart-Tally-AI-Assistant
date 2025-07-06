import pandas as pd
import psycopg2
import ollama
from flask import Flask, request, render_template_string, session
import logging
import re
from datetime import datetime
import secrets

# Generate a secure secret key
secret_key = secrets.token_hex(16)

# Set up logging to record queries and outputs
logging.basicConfig(filename='query_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(message)s')

# Connect to PostgreSQL and load data
try:
    conn = psycopg2.connect(
        host="localhost",
        database="database_name",
        user="username",
        password="yourpassword"
    )
    df = pd.read_sql_query("SELECT * FROM ledger_data", conn)
    print("✅ Data loaded from PostgreSQL successfully!\n")
    
    # Clean and preprocess data
    df.columns = df.columns.str.strip()
    df.loc[:, df.select_dtypes(include='object').columns] = df.select_dtypes(include='object').fillna("")
    df['altered_on'] = pd.to_datetime(df['altered_on'], errors='coerce')
    df['closing_balance'] = pd.to_numeric(df['closing_balance'], errors='coerce')
    df['opening_balance'] = pd.to_numeric(df['opening_balance'], errors='coerce')
    df['year'] = df['altered_on'].dt.year
except Exception as e:
    logging.error(f"Database error: {e}")
    print(f"Database error: {e}")
    exit()

# Helper Functions
def is_tax_ledger(ledger_name):
    """Check if ledger is a tax-related account"""
    tax_keywords = ['gst', 'igst', 'cgst', 'sgst', 'tax', 'tds', 'vat', 'cess']
    return any(keyword in str(ledger_name).lower() for keyword in tax_keywords)

def extract_ledger_name_improved(query):
    """Improved ledger name extraction with better pattern matching"""
    query = query.lower().strip()
    patterns = [
        r'ledger\s+name\s*[=:]\s*["\']([^"\']+)["\']',
        r'ledger\s+name\s*[=:]\s*([^,\s]+(?:\s+[^,\s]+)*)',
        r'ledger\s*[=:]\s*["\']([^"\']+)["\']',
        r'ledger\s*[=:]\s*([^,\s]+(?:\s+[^,\s]+)*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    of_match = re.search(r'\bof\s+(.+?)(?:\s+of\s+\d{4}|\s+\d{4}|$)', query, re.IGNORECASE)
    if of_match:
        return of_match.group(1).strip()
    words_to_remove = ['show', 'all', 'rows', 'ledger', 'name', 'data', 'of', 'for', 'with', 'balance', 'closing', 'opening']
    words = query.split()
    words = [word for word in words if not re.match(r'\d{4}', word)]
    filtered_words = [word for word in words if word not in words_to_remove and not re.match(r'^[=:]$', word)]
    if filtered_words:
        return ' '.join(filtered_words).strip()
    return None

def smart_ledger_search(ledger_query, df_data):
    """Smart ledger search - EXACT MATCH ONLY"""
    if not ledger_query:
        return pd.DataFrame()
    ledger_query = ledger_query.strip()
    exact_mask = df_data["ledger_name"].str.lower() == ledger_query.lower()
    results = df_data[exact_mask].copy()
    if not results.empty:
        return results
    normalized_query = ' '.join(ledger_query.split())
    normalized_mask = df_data["ledger_name"].str.lower().str.replace(r'\s+', ' ', regex=True) == normalized_query.lower()
    results = df_data[normalized_mask].copy()
    if not results.empty:
        return results
    return pd.DataFrame()

def extract_date(query):
    """Extract date from query in YYYY-MM-DD format"""
    match = re.search(r'\d{4}-\d{2}-\d{2}', query)
    return match.group(0) if match else None

def extract_year(query):
    """Extract year from query"""
    match = re.search(r'\b(20\d{2})\b', query)
    return int(match.group(1)) if match else None

# Query Processing Function
def process_query_logic(user_q):
    try:
        year_value = extract_year(user_q)
        
        if "add all" in user_q.lower() or "sum of" in user_q.lower():
            if "closing balance" in user_q.lower():
                col = "closing_balance"
                col_name = "closing balance"
            elif "opening balance" in user_q.lower():
                col = "opening_balance"
                col_name = "opening balance"
            else:
                col = "closing_balance"
                col_name = "closing balance"
            if year_value:
                result = df[df['year'] == year_value][col].sum()
                return f"📊 Total {col_name} for {year_value}: {result:,.2f}"
            else:
                result = df[col].sum()
                return f"📊 Total {col_name}: {result:,.2f}"

        if "show all ledger name" in user_q.lower() and ("closing balance" in user_q.lower() or "opening balance" in user_q.lower()):
            if "closing balance" in user_q.lower():
                col = "closing_balance"
                col_name = "closing balance"
            else:
                col = "opening_balance"
                col_name = "opening balance"
            if year_value:
                results = df[df['year'] == year_value][['ledger_name', col]]
                return f"📊 Ledger names with {col_name} for {year_value}:<br>{results.to_html(index=False, classes='styled-table')}"
            else:
                results = df[['ledger_name', col]]
                return f"📊 Ledger names with {col_name}:<br>{results.to_html(index=False, classes='styled-table')}"

        if "ledger name" in user_q.lower() or "ledger" in user_q.lower():
            ledger_query = extract_ledger_name_improved(user_q)
            if ledger_query:
                results = smart_ledger_search(ledger_query, df)
                if not results.empty:
                    if year_value:
                        results = results[results['year'] == year_value]
                    if results.empty:
                        return f"❌ No results found for '{ledger_query}' in year {year_value}"
                    if "closing balance" in user_q.lower():
                        output = results[["ledger_name", "closing_balance"]].to_html(index=False, classes='styled-table')
                    elif "opening balance" in user_q.lower():
                        output = results[["ledger_name", "opening_balance"]].to_html(index=False, classes='styled-table')
                    else:
                        output = results.to_html(index=False, classes='styled-table')
                    year_info = f" for {year_value}" if year_value else ""
                    return f"📊 {len(results)} results found for '{ledger_query}'{year_info}:<br>{output}"
                else:
                    partial_mask = df["ledger_name"].str.lower().str.contains(ledger_query.lower(), na=False)
                    suggestions = df[partial_mask]["ledger_name"].unique()
                    if len(suggestions) > 0:
                        suggestions_list = "<br>".join([f"• {s}" for s in suggestions[:5]])
                        return f"❌ No exact match found for '{ledger_query}'<br>💡 Did you mean one of these?<br>{suggestions_list}"
                    else:
                        return f"❌ No results found for '{ledger_query}'"

        date_str = extract_date(user_q)
        if date_str:
            try:
                query_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                results = df[df['altered_on'].dt.date == query_date]
                if not results.empty:
                    return f"📊 Results for date {date_str}:<br>{results.to_html(index=False, classes='styled-table')}"
                else:
                    return f"❌ No entries found for date {date_str}"
            except ValueError:
                pass

        if year_value and ("show all rows" in user_q.lower() or "show all" in user_q.lower()):
            results = df[df['year'] == year_value]
            if not results.empty:
                return f"📊 Results for year {year_value}:<br>{results.to_html(index=False, classes='styled-table')}"
            else:
                return f"❌ No entries found for year {year_value}"

        if "largest" in user_q.lower() or "highest" in user_q.lower() or "max" in user_q.lower():
            non_tax_df = df[~df['ledger_name'].apply(is_tax_ledger)].copy()
            if year_value:
                non_tax_df = non_tax_df[non_tax_df['year'] == year_value]
            if "closing" in user_q.lower():
                col = "closing_balance"
                col_name = "closing balance"
            elif "opening" in user_q.lower():
                col = "opening_balance"
                col_name = "opening balance"
            else:
                col = "closing_balance"
                col_name = "closing balance"
            max_value = non_tax_df[col].max()
            results = non_tax_df[non_tax_df[col] == max_value]
            if not results.empty:
                return f"📊 Ledger with highest {col_name} (excluding tax accounts):<br>{results.to_html(index=False, classes='styled-table')}"

        if "smallest" in user_q.lower() or "lowest" in user_q.lower() or "min" in user_q.lower():
            non_tax_df = df[~df['ledger_name'].apply(is_tax_ledger)].copy()
            if year_value:
                non_tax_df = non_tax_df[non_tax_df['year'] == year_value]
            if "closing" in user_q.lower():
                col = "closing_balance"
                col_name = "closing balance"
            elif "opening" in user_q.lower():
                col = "opening_balance"
                col_name = "opening balance"
            else:
                col = "closing_balance"
                col_name = "closing balance"
            min_value = non_tax_df[col].min()
            results = non_tax_df[non_tax_df[col] == min_value]
            if not results.empty:
                return f"📊 Ledger with lowest {col_name} (excluding tax accounts):<br>{results.to_html(index=False, classes='styled-table')}"

        # Fallback to AI
        columns_str = ", ".join(df.columns)
        prompt = f"""
        You are a smart Python assistant working with a Pandas dataframe named 'df'.
        Dataframe columns: {columns_str}

        User question: "{user_q}"

        Generate ONLY ONE LINE of valid Pandas code to answer the question.
        Important rules:
        1. For date comparisons, use: df[df['altered_on'] == pd.Timestamp('YYYY-MM-DD')]
        2. For numeric comparisons, ensure values are numeric: pd.to_numeric(df['column'], errors='coerce')
        3. For ledger name searches, use: df[df['ledger_name'].str.contains('name', case=False, na=False)]
        4. Return ONLY the Python code without explanations or markdown.
        """
        response = ollama.chat(
            model="llama3.2",
            messages=[{"role": "user", "content": prompt}]
        )
        code = response['message']['content'].strip().replace("`", "").replace("python", "")
        try:
            result = eval(code)
            if isinstance(result, pd.DataFrame):
                return result.to_html(index=False, classes='styled-table')
            return str(result)
        except Exception as e:
            return f"❌ Error running code: {e}"

    except Exception as e:
        return f"❌ Error: {e}"

# Flask App Setup
app = Flask(__name__)
app.secret_key = secret_key

# Main HTML Template with modern styling
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DATA(AI) - Smart Database Assistant</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        body {
            background: linear-gradient(135deg, #1a2a6c, #b21f1f, #1a2a6c);
            background-size: 400% 400%;
            animation: gradientBG 15s ease infinite;
            min-height: 100vh;
            padding: 20px;
            display: flex;
            justify-content: center;
            align-items: center;
            color: #333;
        }
        
        @keyframes gradientBG {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        .container {
            width: 90%;
            max-width: 1200px;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            box-shadow: 0 15px 35px rgba(0, 0, 0, 0.3);
            overflow: hidden;
            backdrop-filter: blur(10px);
        }
        
        header {
            background: linear-gradient(90deg, #1a2980, #26d0ce);
            color: white;
            padding: 25px 30px;
            text-align: center;
            position: relative;
        }
        
        header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
        }
        
        header p {
            font-size: 1.1rem;
            opacity: 0.9;
            max-width: 800px;
            margin: 0 auto;
        }
        
        .content {
            padding: 30px;
        }
        
        .chat-container {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 25px;
            box-shadow: inset 0 2px 5px rgba(0,0,0,0.05);
            margin-bottom: 25px;
            max-height: 60vh;
            overflow-y: auto;
            min-height: 300px;
        }
        
        .chat-bubble {
            padding: 15px 20px;
            border-radius: 18px;
            margin-bottom: 15px;
            max-width: 80%;
            position: relative;
            animation: fadeIn 0.3s ease;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .user-bubble {
            background: #e3f2fd;
            margin-left: auto;
            border-bottom-right-radius: 5px;
        }
        
        .ai-bubble {
            background: #ffffff;
            border: 1px solid #e0e0e0;
            margin-right: auto;
            border-bottom-left-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        
        .ai-bubble table {
            width: 100%;
        }
        
        .query-form {
            display: flex;
            gap: 15px;
            margin-top: 20px;
        }
        
        #query-input {
            flex: 1;
            padding: 18px 20px;
            border: 2px solid #26d0ce;
            border-radius: 50px;
            font-size: 1.1rem;
            outline: none;
            transition: all 0.3s;
            box-shadow: 0 4px 10px rgba(0,0,0,0.1);
        }
        
        #query-input:focus {
            border-color: #1a2980;
            box-shadow: 0 4px 15px rgba(26, 41, 128, 0.2);
        }
        
        .button-group {
            display: flex;
            gap: 10px;
        }
        
        #submit-btn, #clear-btn {
            color: white;
            border: none;
            border-radius: 50px;
            padding: 0 30px;
            font-size: 1.1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            box-shadow: 0 4px 10px rgba(0,0,0,0.2);
        }
        
        #submit-btn {
            background: linear-gradient(90deg, #1a2980, #26d0ce);
        }
        
        #clear-btn {
            background: linear-gradient(90deg, #ff416c, #ff4b2b);
        }
        
        #submit-btn:hover, #clear-btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 6px 15px rgba(0,0,0,0.3);
        }
        
        #submit-btn:hover {
            background: linear-gradient(90deg, #1a2980, #1a2980);
        }
        
        #clear-btn:hover {
            background: linear-gradient(90deg, #ff416c, #ff416c);
        }
        
        .styled-table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 0.95em;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.05);
        }
        
        .styled-table thead tr {
            background: linear-gradient(90deg, #1a2980, #26d0ce);
            color: #ffffff;
            text-align: left;
            font-weight: bold;
        }
        
        .styled-table th,
        .styled-table td {
            padding: 15px 18px;
        }
        
        .styled-table tbody tr {
            border-bottom: 1px solid #e0e0e0;
        }
        
        .styled-table tbody tr:nth-of-type(even) {
            background-color: #f8f9fa;
        }
        
        .styled-table tbody tr:last-of-type {
            border-bottom: 2px solid #1a2980;
        }
        
        .styled-table tbody tr:hover {
            background-color: #e3f2fd;
            cursor: pointer;
        }
        
        .emoji-response {
            font-size: 1.2rem;
            margin-bottom: 10px;
        }
        
        .timestamp {
            font-size: 0.8rem;
            color: #777;
            text-align: right;
            margin-top: 5px;
        }
        
        footer {
            text-align: center;
            padding: 20px;
            color: #777;
            font-size: 0.9rem;
            border-top: 1px solid #e0e0e0;
            background: #f8f9fa;
        }
        
        .empty-chat {
            text-align: center;
            color: #999;
            padding: 50px 20px;
            font-style: italic;
        }
        
        @media (max-width: 768px) {
            .container {
                width: 95%;
            }
            
            header h1 {
                font-size: 2rem;
            }
            
            .query-form {
                flex-direction: column;
            }
            
            .button-group {
                width: 100%;
            }
            
            #submit-btn, #clear-btn {
                padding: 15px;
                width: 100%;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Eigen's DATA(AI)</h1>
            <p>Your Smart Database Assistant - Ask anything about your data</p>
        </header>
        
        <div class="content">
            <div class="chat-container" id="chat-container">
                {% if not session.chat_history %}
                    <div class="empty-chat">
                        <p>Start chatting with your database assistant</p>
                        <p>Ask questions like:</p>
                        <p>"Show all rows of 2023"</p>
                        <p>"What is the closing balance for ABC Suppliers?"</p>
                        <p>"Total closing balance for 2024"</p>
                    </div>
                {% else %}
                    {% for message in session.chat_history %}
                        <div class="chat-bubble {{ message.type }}-bubble">
                            <div class="message-content">
                                {{ message.content | safe }}
                            </div>
                            <div class="timestamp">
                                {{ message.timestamp }}
                            </div>
                        </div>
                    {% endfor %}
                {% endif %}
            </div>
            
            <form method="POST" class="query-form">
                <input type="text" name="query" id="query-input" placeholder="Ask your question about ledger data..." autocomplete="off" autofocus>
                <div class="button-group">
                    <input type="submit" id="submit-btn" value="Ask" formaction="/query">
                    <input type="submit" id="clear-btn" value="Clear Chat" formaction="/clear">
                </div>
            </form>
        </div>
        
        <footer>
            <p>© 2025 Eigen Technologies • Secure Database AI Assistant</p>
        </footer>
    </div>
    
    <script>
        // Scroll to bottom of chat container
        const chatContainer = document.getElementById('chat-container');
        chatContainer.scrollTop = chatContainer.scrollHeight;
        
        // Focus on input field
        document.getElementById('query-input').focus();
    </script>
</body>
</html>
'''

@app.route('/', methods=['GET'])
def home():
    # Initialize chat history if it doesn't exist
    if 'chat_history' not in session:
        session['chat_history'] = []
    return render_template_string(HTML_TEMPLATE)

@app.route('/query', methods=['POST'])
def process_query_web():
    user_q = request.form['query'].strip()
    if not user_q:
        return render_template_string(HTML_TEMPLATE)
    
    # Get current timestamp
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    # Add user query to chat history
    session['chat_history'].append({
        'type': 'user',
        'content': user_q,
        'timestamp': timestamp
    })
    
    # Process query
    result = process_query_logic(user_q)
    
    # Add AI response to chat history
    session['chat_history'].append({
        'type': 'ai',
        'content': result,
        'timestamp': datetime.now().strftime("%H:%M:%S")
    })
    
    # Save session
    session.modified = True
    
    # Log the query and result
    logging.info(f"Query: {user_q}\nResult: {result}")
    
    return render_template_string(HTML_TEMPLATE)

@app.route('/clear', methods=['POST'])
def clear_chat():
    # Clear chat history
    session['chat_history'] = []
    session.modified = True
    return render_template_string(HTML_TEMPLATE)

if __name__ == '__main__':
    cert_path = '/home/ravi/ssl/cert.pem'
    key_path = '/home/ravi/ssl/key.pem'
    print("App will be accessible at https://192.168.1.100:5000")
    app.run(host='0.0.0.0', port=5000, ssl_context=(cert_path, key_path), debug=True)
