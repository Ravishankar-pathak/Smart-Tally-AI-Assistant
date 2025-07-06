import pandas as pd
import sqlalchemy
from transformers import pipeline
import os
import requests
import re
from pathlib import Path
import warnings
from collections import OrderedDict
import fasttext
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import threading
import time
from lxml import etree
from datetime import datetime
import psycopg2

warnings.filterwarnings('ignore')

class AIQueryEngine:
    def __init__(self, data_source_config):
        self.data_source = data_source_config
        self.available_tables = []
        self.table_columns = {}
        
        os.environ['OMP_NUM_THREADS'] = str(os.cpu_count() or 4)
        os.environ['TOKENIZERS_PARALLELISM'] = 'false'
        
        self._setup_models()
        
        if self.data_source['type'] in ['postgresql', 'mysql']:
            self._analyze_database_schema()
        
        print("✅ AI Query Engine initialized successfully!")

    def _setup_models(self):
        try:
            self.translator = pipeline(
                'translation', 
                model='Helsinki-NLP/opus-mt-hi-en',
                device=-1,
                torch_dtype='auto'
            )
        except Exception:
            self.translator = None
        
        try:
            self.lang_detector = fasttext.load_model('lid.176.ftz')
        except:
            self.lang_detector = None

    def _analyze_database_schema(self):
        try:
            engine = sqlalchemy.create_engine(self.data_source['conn_str'])
            with engine.connect() as conn:
                if self.data_source['type'] == 'postgresql':
                    tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                else:
                    tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
                
                tables = conn.execute(sqlalchemy.text(tables_query)).fetchall()
                self.available_tables = [table[0] for table in tables]
                
                for table_name in self.available_tables:
                    if self.data_source['type'] == 'postgresql':
                        columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
                    else:
                        columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = DATABASE()"
                    
                    columns = conn.execute(sqlalchemy.text(columns_query)).fetchall()
                    self.table_columns[table_name] = {
                        'all': [col[0] for col in columns],
                        'numeric': [col[0] for col in columns if col[1] in ['integer', 'numeric', 'float', 'double precision', 'decimal', 'bigint', 'smallint', 'int', 'double', 'float']],
                        'jsonb': [col[0] for col in columns if col[1] == 'jsonb']
                    }
        except Exception:
            pass

    def _detect_language(self, text):
        if self.lang_detector:
            try:
                prediction = self.lang_detector.predict(text)[0][0]
                return prediction.split('__')[-1]
            except:
                pass
        hindi_chars = re.findall(r'[\u0900-\u097F]', text)
        return 'hi' if len(hindi_chars) > len(text) * 0.3 else 'en'
        
    def _translate_hindi_to_english(self, text):
        if not self.translator:
            return text
        try:
            return self.translator(text)[0]['translation_text']
        except Exception:
            return text

    def _find_best_table(self, query):
        query_lower = query.lower()
        for table in self.available_tables:
            if table.lower() in query_lower:
                return table
        table_patterns = {
            'invoice': ['invoice', 'bill', 'receipt'],
            'customer': ['customer', 'client', 'buyer'],
            'product': ['product', 'item', 'goods'],
            'sales': ['sales', 'sell', 'revenue'],
            'transaction': ['transaction', 'payment', 'txn']
        }
        for table in self.available_tables:
            table_lower = table.lower()
            for pattern, keywords in table_patterns.items():
                if pattern in table_lower:
                    for keyword in keywords:
                        if keyword in query_lower:
                            return table
        return self.available_tables[0] if self.available_tables else 'data'

    def _find_best_column(self, query, table_name, prefer_numeric=False):
        if table_name not in self.table_columns:
            return 'amount' if prefer_numeric else 'id'
        query_lower = query.lower()
        table_cols = self.table_columns[table_name]
        if prefer_numeric and table_cols['numeric']:
            for col in table_cols['numeric']:
                if col.lower() in query_lower:
                    return col
            return table_cols['numeric'][0] if table_cols['numeric'] else 'id'
        for col in table_cols['all']:
            if col.lower() in query_lower:
                return col
        for col in table_cols['all']:
            col_parts = col.lower().split('_')
            for part in col_parts:
                if part in query_lower and len(part) > 2:
                    return col
        column_mappings = OrderedDict([
            ('amount', ['amount', 'price', 'cost', 'value']),
            ('total_amount', ['total amount', 'invoice total', 'grand total', 'final amount']),
            ('quantity', ['quantity', 'qty', 'count', 'number']),
            ('date', ['date', 'time', 'created', 'updated']),
            ('name', ['name', 'title', 'description', 'text']),
            ('id', ['id', 'identifier', 'key', 'number']),
            ('description', ['description', 'desc', 'item', 'product']),
            ('customer_name', ['customer', 'client', 'buyer']),
            ('company_name', ['company', 'vendor', 'supplier', 'firm']),
            ('gst_number', ['gst', 'tax', 'gstin']),
            ('invoice_number', ['invoice', 'bill', 'receipt', 'number']),
            ('hsn', ['hsn', 'code', 'tax code']),
            ('gst', ['gst', 'tax'])
        ])
        query_terms = query_lower.split()
        for col in table_cols['all']:
            col_lower = col.lower()
            for term in query_terms:
                if term in col_lower:
                    return col
        for query_term in query_terms:
            for category, keywords in column_mappings.items():
                if query_term in keywords:
                    for col in table_cols['all']:
                        if category in col.lower():
                            return col
        return table_cols['all'][0] if table_cols['all'] else 'id'

    def _analyze_query_intent(self, query):
        query_lower = query.lower()
        intent_patterns = [
            (r'\b(record|jisme|usme|jis|uska|sabse|which|where|pura|full|complete)\b.*\b(highest|max|maximum|largest|biggest)\b', 'CONDITIONAL_MAX'),
            (r'\b(record|jisme|usme|jis|uska|sabse|which|where|pura|full|complete)\b.*\b(lowest|min|minimum|smallest|least)\b', 'CONDITIONAL_MIN'),
            (r'\b(highest|max|maximum|largest|biggest)\b.*\b(record|jisme|usme|jis|uska|sabse|which|where|pura|full|complete)\b', 'CONDITIONAL_MAX'),
            (r'\b(lowest|min|minimum|smallest|least)\b.*\b(record|jisme|usme|jis|uska|sabse|which|where|pura|full|complete)\b', 'CONDITIONAL_MIN'),
            (r'\b(find|search|show|get|fetch|where|with|having|contains|like|dekho|dikhao|show me)\b.*\b(value|equal|equals|=)\b', 'SEARCH'),
            (r'\b(all|records|rows|data|pura|complete|full)\b.*\b(where|with|having|contains|dekho|dikhao)\b', 'FULL_RECORD'),
            (r'\b(highest|maximum|max|largest|biggest|sabse\s*(jyada|zyada|bada))\b', 'MAX'),
            (r'\b(lowest|minimum|min|smallest|least|sabse\s*(kam|chota))\b', 'MIN'),
            (r'\b(count|number|how\s*many|kitne|kitna)\b', 'COUNT'),
            (r'\b(average|avg|mean|ausat)\b', 'AVERAGE'),
            (r'\b(total|sum|add|jod|yog)\b(?!\s*(amount|_amount))', 'SUM'),
            (r'\b(show|list|display|view|all\s*data|dikhao|dekho|write|kya\s*likha|sab)\b.*\b(and|aur)\b', 'DISPLAY_MULTIPLE'),
            (r'\b(show|list|display|view|all\s*data|dikhao|dekho|write|kya\s*likha|sab)\b', 'FULL_RECORD'),
            (r'\b(\w+)\b.*\bof\b.*\b(highest|max|maximum|largest|biggest)\b', 'CONDITIONAL_MAX_COL'),
            (r'\b(\w+)\b.*\bof\b.*\b(lowest|min|minimum|smallest|least)\b', 'CONDITIONAL_MIN_COL'),
            (r'\b(which|what|konsa|kaun sa|kaunsi)\b.*\b(highest|max|largest|biggest)\b', 'CONDITIONAL_MAX_COL'),
            (r'\b(which|what|konsa|kaun sa|kaunsi)\b.*\b(lowest|min|smallest|least)\b', 'CONDITIONAL_MIN_COL'),
        ]
        for pattern, intent in intent_patterns:
            if re.search(pattern, query_lower):
                return intent
        return 'DISPLAY_COLUMN'

    def _extract_search_criteria(self, query):
        query_lower = query.lower()
        criteria = {}
        stop_words = ['me', 'hai', 'kya', 'ke', 'ka', 'ki', 'ko', 'se', 'from', 'in', 'of', 'the', 'is', 'are', 'and', 'aur', 'with', 'which', 'has', 'have', 'had', 'a', 'an', 'that', 'this', 'those', 'these']
        agg_keywords = ['highest', 'lowest', 'max', 'min', 'maximum', 'minimum', 'biggest', 'smallest', 'count', 'sum', 'total', 'average', 'avg']
        table_name = self._find_best_table(query)
        available_columns = self.table_columns.get(table_name, {}).get('all', [])
        numeric_columns = self.table_columns.get(table_name, {}).get('numeric', [])
        words = query_lower.split()
        i = 0
        while i < len(words):
            word = words[i]
            if word in stop_words or word in agg_keywords:
                i += 1
                continue
            if word in available_columns:
                if word in numeric_columns:
                    i += 1
                    continue
                if i < len(words) - 1 and words[i + 1] not in stop_words:
                    criteria[word] = words[i + 1]
                    i += 2
                else:
                    i += 1
            else:
                i += 1
        return criteria

    def _build_dynamic_query(self, query, table_name):
        where_conditions = []
        search_criteria = self._extract_search_criteria(query)
        query_lower = query.lower()
        if table_name in self.table_columns:
            available_columns = self.table_columns[table_name]['all']
            numeric_columns = self.table_columns[table_name]['numeric']
            jsonb_columns = self.table_columns[table_name].get('jsonb', [])
            for search_term, search_value in search_criteria.items():
                if search_term in available_columns and search_term not in ['aggregation', 'agg_column']:
                    if search_term in numeric_columns:
                        continue
                    if search_term in jsonb_columns:
                        where_conditions.append(f"items->0->>'{search_term}' ILIKE '%{search_value}%'")
                    else:
                        if search_value.isdigit() or search_value.replace('.', '', 1).isdigit():
                            where_conditions.append(f"{search_term} = {search_value}")
                        else:
                            where_conditions.append(f"{search_term} ILIKE '%{search_value}%'")
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)
        intent = self._analyzeandroidx_query_intent(query)
        jsonb_columns = self.table_columns.get(table_name, {}).get('jsonb', [])
        target_column = self._find_best_column(query, table_name)
        if intent == 'FULL_RECORD':
            return f"SELECT * FROM {table_name}{where_clause}"
        if intent in ['CONDITIONAL_MAX', 'CONDITIONAL_MIN']:
            agg_func = 'MAX' if intent == 'CONDITIONAL_MAX' else 'MIN'
            agg_column = self._find_best_column(query, table_name, prefer_numeric=True)
            total_amount_phrases = ['total amount', 'invoice total', 'grand total', 'final amount']
            if any(phrase in query_lower for phrase in total_amount_phrases) and table_name in self.table_columns:
                if 'total_amount' in self.table_columns[table_name]['all']:
                    agg_column = 'total_amount'
            subquery = f"SELECT {agg_func}({agg_column}) FROM {table_name}{where_clause}"
            main_where = f"WHERE {agg_column} = ({subquery})"
            if where_conditions:
                main_where += " AND " + " AND ".join(where_conditions)
            return f"SELECT * FROM {table_name} {main_where}"
        if intent in ['CONDITIONAL_MAX_COL', 'CONDITIONAL_MIN_COL']:
            agg_func = 'MAX' if intent == 'CONDITIONAL_MAX_COL' else 'MIN'
            agg_column = self._find_best_column(query, table_name, prefer_numeric=True)
            target_column = self._find_best_column(query, table_name)
            if ' of ' in query:
                parts = query.split(' of ', 1)
                target_desc = parts[0].strip()
                agg_desc = parts[1].strip()
                target_column = self._find_best_column(target_desc, table_name)
                agg_column = self._find_best_column(agg_desc, table_name, prefer_numeric=True)
            total_amount_phrases = ['total amount', 'invoice total', 'grand total', 'final amount']
            if any(phrase in query_lower for phrase in total_amount_phrases) and table_name in self.table_columns:
                if 'total_amount' in self.table_columns[table_name]['all']:
                    agg_column = 'total_amount'
            subquery = f"SELECT {agg_func}({agg_column}) FROM {table_name}{where_clause}"
            main_where = f"WHERE {agg_column} = ({subquery})"
            if where_conditions:
                main_where += " AND " + " AND ".join(where_conditions)
            return f"SELECT {target_column} FROM {table_name} {main_where}"
        if intent == 'MAX':
            if target_column in jsonb_columns:
                select_clause = f"SELECT MAX((items->0->>'{target_column}')::numeric) as max_value"
            else:
                select_clause = f"SELECT MAX({target_column}) as max_value"
        elif intent == 'MIN':
            if target_column in jsonb_columns:
                select_clause = f"SELECT MIN((items->0->>'{target_column}')::numeric) as min_value"
            else:
                select_clause = f"SELECT MIN({target_column}) as min_value"
        elif intent == 'SUM':
            if target_column in jsonb_columns:
                select_clause = f"SELECT SUM((items->0->>'{target_column}')::numeric) as total_sum"
            else:
                select_clause = f"SELECT SUM({target_column}) as total_sum"
        elif intent == 'AVERAGE':
            if target_column in jsonb_columns:
                select_clause = f"SELECT AVG((items->0->>'{target_column}')::numeric) as average_value"
            else:
                select_clause = f"SELECT AVG({target_column}) as average_value"
        elif intent == 'COUNT':
            if target_column in jsonb_columns:
                select_clause = f"SELECT COUNT({target_column}) as total_count"
            else:
                select_clause = f"SELECT COUNT({target_column}) as total_count"
        elif intent == 'DISPLAY_MULTIPLE':
            columns = [col for col in self.table_columns.get(table_name, {}).get('all', []) if col.lower() in query_lower]
            if not columns:
                columns = [target_column]
            select_clause = f"SELECT {', '.join(columns)}" if columns else "SELECT *"
        else:
            select_clause = f"SELECT {target_column}" if target_column != 'items' else f"SELECT items->0->>'description' as description"
        from_clause = f" FROM {table_name}"
        limit_clause = ""
        if intent in ['DISPLAY_COLUMN', 'DISPLAY_MULTIPLE', 'SEARCH'] and not where_conditions:
            limit_clause = " LIMIT 10"
        return select_clause + from_clause + where_clause + limit_clause

    def _process_query_directly(self, query):
        query_lower = query.lower()
        if self.data_source['type'] in ['postgresql', 'mysql']:
            try:
                engine = sqlalchemy.create_engine(self.data_source['conn_str'])
                table_name = self._find_best_table(query)
                intent = self._analyze_query_intent(query)
                target_column = self._find_best_column(query, table_name)
                sql_query = self._build_dynamic_query(query, table_name)
                with engine.connect() as conn:
                    result = conn.execute(sqlalchemy.text(sql_query)).fetchall()
                    if not result:
                        return "No data found"
                    def format_value(value):
                        if isinstance(value, float):
                            return f"{value:,.2f}" if value % 1 else f"{int(value):,}"
                        elif isinstance(value, int):
                            return f"{value:,}"
                        return str(value)
                    if intent == 'MAX':
                        value = result[0][0] if result[0][0] is not None else 0
                        return format_value(value)
                    elif intent == 'MIN':
                        value = result[0][0] if result[0][0] is not None else 0
                        return format_value(value)
                    elif intent == 'SUM':
                        value = result[0][0] if result[0][0] is not None else 0
                        return format_value(value)
                    elif intent == 'COUNT':
                        count = result[0][0] if result else 0
                        return format_value(count)
                    elif intent == 'AVERAGE':
                        value = result[0][0] if result[0][0] is not None else 0
                        return format_value(value)
                    elif intent in ['CONDITIONAL_MAX', 'CONDITIONAL_MIN', 'FULL_RECORD']:
                        col_names = result[0]._fields if result else []
                        response = ""
                        for i, row in enumerate(result):
                            response += f"\n-[ RECORD {i+1} ]-{'-'*60}\n"
                            for j, value in enumerate(row):
                                col_name = col_names[j] if j < len(col_names) else f"Column_{j+1}"
                                response += f"{col_name.ljust(20)} | {format_value(value)}\n"
                        return response
                    elif intent in ['CONDITIONAL_MAX_COL', 'CONDITIONAL_MIN_COL']:
                        if result and len(result) > 0:
                            return format_value(result[0][0])
                        return "No data found"
                    elif intent == 'DISPLAY_MULTIPLE':
                        columns = [col for col in self.table_columns.get(table_name, {}).get('all', []) if col.lower() in query_lower]
                        if not columns:
                            columns = [target_column]
                        response = ""
                        for i, row in enumerate(result):
                            values = [format_value(val) for val in row]
                            response += f"Record {i+1}: {', '.join(values)}\n"
                        return response
                    elif intent in ['DISPLAY_COLUMN', 'SEARCH']:
                        column_name = target_column
                        response = ""
                        for i, row in enumerate(result):
                            value = format_value(row[0])
                            response += f"Record {i+1}: {value}\n"
                        return response
                    else:
                        response = ""
                        for i, row in enumerate(result):
                            response += f"{i+1}: {format_value(row[0])}\n"
                        return response
            except Exception as e:
                return f"Error: {str(e)}"
        command = self._generate_command_rule_based_files(query)
        return self._execute_command(command)

    def _generate_command_rule_based_files(self, query):
        query_lower = query.lower()
        source_type = self.data_source['type']
        column_map = {
            'total amount': 'total_amount',
            'amount': 'amount',
            'sales': 'sales',
            'price': 'price',
            'quantity': 'quantity',
            'description': 'description',
            'customer_name': 'customer_name',
            'company_name': 'company_name',
            'invoice_number': 'invoice_number',
            'invoice_date': 'invoice_date',
            'hsn': 'hsn',
            'gst': 'gst'
        }
        if source_type == 'tally':
            if re.search(r'compan|firm|organi', query_lower):
                return "companies"
            elif re.search(r'ledger|account', query_lower):
                if re.search(r'total|sum|add', query_lower):
                    return "sum:ClosingBalance"
                elif re.search(r'average|avg|mean', query_lower):
                    return "avg:ClosingBalance"
                elif re.search(r'max|maximum|highest', query_lower):
                    return "max:ClosingBalance"
                elif re.search(r'min|minimum|lowest', query_lower):
                    return "min:ClosingBalance"
                elif re.search(r'count|number', query_lower):
                    return "count:ledgers"
                elif re.search(r'full|complete|all', query_lower):
                    return "full:ledger"
                else:
                    return "ledgers"
            else:
                return "companies"
        column_name = 'amount'
        for query_term, db_column in column_map.items():
            if query_term in query_lower:
                column_name = db_column
                break
        patterns = {
            r'total|sum|add': {
                'excel': f"df['{column_name}'].sum()",
                'csv': f"df['{column_name}'].sum()",
            },
            r'count|number': {
                'excel': "len(df)",
                'csv': "len(df)",
            },
            r'average|avg|mean': {
                'excel': f"df['{column_name}'].mean()",
                'csv': f"df['{column_name}'].mean()",
            },
            r'max|maximum|highest': {
                'excel': f"df['{column_name}'].max()",
                'csv': f"df['{column_name}'].max()",
            },
            r'min|minimum|lowest': {
                'excel': f"df['{column_name}'].min()",
                'csv': f"df['{column_name}'].min()",
            },
            r'description|customer_name|company_name|invoice_number|invoice_date|hsn|gst': {
                'excel': f"df['{column_name}'].tolist()",
                'csv': f"df['{column_name}'].tolist()",
            },
            r'record|jisme|usme|jis|uska|sabse|which|where|pura|full|complete': {
                'excel': "df.to_dict('records')",
                'csv': "df.to_dict('records')",
            }
        }
        for pattern, commands in patterns.items():
            if re.search(pattern, query_lower):
                return commands.get(source_type, f"df.head()")
        defaults = {
            'excel': f"df['{column_name}'].tolist()",
            'csv': f"df['{column_name}'].tolist()",
            'tally': "ledgers"
        }
        return defaults.get(source_type, f"df.head()")

    def _clean_command(self, command):
        for delimiter in ['\n', ';', '```', '#']:
            if delimiter in command:
                command = command.split(delimiter)[0]
        for prefix in ['Command:', 'SQL:', 'Excel:', 'Tally:', 'Answer:']:
            if prefix in command:
                command = command.split(prefix)[-1]
        return command.strip()

    def _execute_command(self, command):
        command = self._clean_command(command)
        if self.data_source['type'] == 'excel':
            return self._execute_excel(command)
        elif self.data_source['type'] == 'csv':
            return self._execute_csv(command)
        elif self.data_source['type'] in ['postgresql', 'mysql']:
            return self._execute_sql(command)
        elif self.data_source['type'] == 'tally':
            return self._execute_tally(command)
        else:
            return "Unsupported data source"

    def _execute_excel(self, command):
        try:
            df = pd.read_excel(self.data_source['path'])
            if 'amount' in command and 'amount' not in df.columns:
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    command = command.replace('amount', numeric_cols[0])
            if 'to_dict' in command:
                return df.to_dict('records')
            safe_env = {
                'df': df,
                'pd': pd,
                'len': len,
                '__builtins__': {}
            }
            result = eval(command, safe_env)
            if isinstance(result, (int, float)):
                return f"{result:,.2f}" if isinstance(result, float) else str(result)
            elif isinstance(result, list):
                return "\n".join([str(x) for x in result])
            elif isinstance(result, pd.Series):
                return "\n".join([str(x) for x in result])
            elif isinstance(result, dict):
                return str([result])
            else:
                return str(result)
        except FileNotFoundError:
            return f"Excel file not found: {self.data_source['path']}"
        except Exception as e:
            return f"Excel Error: {str(e)}"
        
    def _execute_csv(self, command):
        try:
            df = pd.read_csv(self.data_source['path'])
            if 'amount' in command and 'amount' not in df.columns:
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    command = command.replace('amount', numeric_cols[0])
            if 'to_dict' in command:
                return df.to_dict('records')
            safe_env = {
                'df': df,
                'pd': pd,
                'len': len,
                '__builtins__': {}
            }
            result = eval(command, safe_env)
            if isinstance(result, (int, float)):
                return f"{result:,.2f}" if isinstance(result, float) else str(result)
            elif isinstance(result, list):
                return "\n".join([str(x) for x in result])
            elif isinstance(result, pd.Series):
                return "\n".join([str(x) for x in result])
            elif isinstance(result, dict):
                return str([result])
            else:
                return str(result)
        except FileNotFoundError:
            return f"CSV file not found: {self.data_source['path']}"
        except Exception as e:
            return f"CSV Error: {str(e)}"
        
    def _execute_sql(self, command):
        try:
            engine = sqlalchemy.create_engine(self.data_source['conn_str'])
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text(command))
                if command.strip().lower().startswith('select'):
                    rows = result.fetchall()
                    if not rows:
                        return "No data found"
                    if len(rows) == 1 and len(rows[0]) == 1:
                        return str(rows[0][0])
                    return "\n".join([str(row) for row in rows])
                else:
                    return f"{result.rowcount} rows affected"
        except Exception as e:
            return f"SQL Error: {str(e)}"

    def _execute_tally(self, command):
        try:
            host = self.data_source.get('host', 'localhost')
            port = self.data_source.get('port', 9000)
            url = f'http://{host}:{port}'
            if ':' in command:
                operation, param = command.split(':', 1)
                operation = operation.strip().lower()
                param = param.strip()
            else:
                operation = command.strip().lower()
                param = ''
            COMPANY_XML = """<ENVELOPE>
                <HEADER>
                    <VERSION>1</VERSION>
                    <TALLYREQUEST>Export</TALLYREQUEST>
                    <TYPE>Collection</TYPE>
                    <ID>Company Collection</ID>
                </HEADER>
                <BODY>
                    <DESC>
                        <STATICVARIABLES>
                            <SVEXPORTFORMAT>$$  SysName:XML</SVEXPORTFORMAT>
                        </STATICVARIABLES>
                        <TDL>
                            <TDLMESSAGE>
                                <COLLECTION NAME="Company Collection" ISMODIFY="No">
                                    <TYPE>Company</TYPE>
                                    <FETCH>NAME</FETCH>
                                </COLLECTION>
                            </TDLMESSAGE>
                        </TDL>
                    </DESC>
                </BODY>
            </ENVELOPE>"""
            LEDGER_XML = """<ENVELOPE>
                <HEADER>
                    <VERSION>1</VERSION>
                    <TALLYREQUEST>Export</TALLYREQUEST>
                    <TYPE>Collection</TYPE>
                    <ID>Ledger Collection</ID>
                </HEADER>
                <BODY>
                    <DESC>
                        <STATICVARIABLES>
                            <SVEXPORTFORMAT>  $$SysName:XML</SVEXPORTFORMAT>
                        </STATICVARIABLES>
                        <TDL>
                            <TDLMESSAGE>
                                <COLLECTION NAME="Ledger Collection" ISMODIFY="No">
                                    <TYPE>Ledger</TYPE>
                                    <FETCH>*</FETCH>
                                </COLLECTION>
                            </TDLMESSAGE>
                        </TDL>
                    </DESC>
                </BODY>
            </ENVELOPE>"""
            if operation == 'companies':
                xml_request = COMPANY_XML
            elif operation == 'ledgers':
                xml_request = LEDGER_XML
            elif operation == 'max':
                xml_request = f"""<ENVELOPE>
                    <HEADER>
                        <TALLYREQUEST>Export</TALLYREQUEST>
                        <TYPE>Data</TYPE>
                        <ID>List of Ledgers</ID>
                    </HEADER>
                    <BODY>
                        <DESC>
                            <STATICVARIABLES>
                                <SVEXPORTFORMAT>$$  SysName:XML</SVEXPORTFORMAT>
                            </STATICVARIABLES>
                            <TDL>
                                <TDLMESSAGE>
                                    <REPORT NAME="List of Ledgers" ISMODIFY="No">
                                        <FORMS>Ledger</FORMS>
                                        <VARIABLE>
                                            <SVSORTFIELD>{param or 'ClosingBalance'}</SVSORTFIELD>
                                            <SVSORTORDER>Descending</SVSORTORDER>
                                        </VARIABLE>
                                        <FETCH>{param or 'ClosingBalance'}</FETCH>
                                    </REPORT>
                                </TDLMESSAGE>
                            </TDL>
                        </DESC>
                    </BODY>
                </ENVELOPE>"""
            elif operation == 'min':
                xml_request = f"""<ENVELOPE>
                    <HEADER>
                        <TALLYREQUEST>Export</TALLYREQUEST>
                        <TYPE>Data</TYPE>
                        <ID>List of Ledgers</ID>
                    </HEADER>
                    <BODY>
                        <DESC>
                            <STATICVARIABLES>
                                <SVEXPORTFORMAT>  $$SysName:XML</SVEXPORTFORMAT>
                            </STATICVARIABLES>
                            <TDL>
                                <TDLMESSAGE>
                                    <REPORT NAME="List of Ledgers" ISMODIFY="No">
                                        <FORMS>Ledger</FORMS>
                                        <VARIABLE>
                                            <SVSORTFIELD>{param or 'ClosingBalance'}</SVSORTFIELD>
                                            <SVSORTORDER>Ascending</SVSORTORDER>
                                        </VARIABLE>
                                        <FETCH>{param or 'ClosingBalance'}</FETCH>
                                    </REPORT>
                                </TDLMESSAGE>
                            </TDL>
                        </DESC>
                    </BODY>
                </ENVELOPE>"""
            elif operation == 'sum':
                xml_request = f"""<ENVELOPE>
                    <HEADER>
                        <TALLYREQUEST>Export</TALLYREQUEST>
                        <TYPE>Data</TYPE>
                        <ID>Ledger Summary</ID>
                    </HEADER>
                    <BODY>
                        <DESC>
                            <STATICVARIABLES>
                                <SVEXPORTFORMAT>$$  SysName:XML</SVEXPORTFORMAT>
                            </STATICVARIABLES>
                            <TDL>
                                <TDLMESSAGE>
                                    <REPORT NAME="Ledger Summary" ISMODIFY="No">
                                        <FORMS>Ledger</FORMS>
                                        <VARIABLE>
                                            <SVAGGREGATEMETHOD>Sum</SVAGGREGATEMETHOD>
                                            <SVAGGREGATEFIELD>{param or 'ClosingBalance'}</SVAGGREGATEFIELD>
                                        </VARIABLE>
                                        <FETCH>{param or 'ClosingBalance'}</FETCH>
                                    </REPORT>
                                </TDLMESSAGE>
                            </TDL>
                        </DESC>
                    </BODY>
                </ENVELOPE>"""
            elif operation == 'avg':
                xml_request = f"""<ENVELOPE>
                    <HEADER>
                        <TALLYREQUEST>Export</TALLYREQUEST>
                        <TYPE>Data</TYPE>
                        <ID>Ledger Summary</ID>
                    </HEADER>
                    <BODY>
                        <DESC>
                            <STATICVARIABLES>
                                <SVEXPORTFORMAT>  $$SysName:XML</SVEXPORTFORMAT>
                            </STATICVARIABLES>
                            <TDL>
                                <TDLMESSAGE>
                                    <REPORT NAME="Ledger Summary" ISMODIFY="No">
                                        <FORMS>Ledger</FORMS>
                                        <VARIABLE>
                                            <SVAGGREGATEMETHOD>Average</SVAGGREGATEMETHOD>
                                            <SVAGGREGATEFIELD>{param or 'ClosingBalance'}</SVAGGREGATEFIELD>
                                        </VARIABLE>
                                        <FETCH>{param or 'ClosingBalance'}</FETCH>
                                    </REPORT>
                                </TDLMESSAGE>
                            </TDL>
                        </DESC>
                    </BODY>
                </ENVELOPE>"""
            elif operation == 'count':
                xml_request = f"""<ENVELOPE>
                    <HEADER>
                        <TALLYREQUEST>Export</TALLYREQUEST>
                        <TYPE>Collection</TYPE>
                        <ID>Ledger Collection</ID>
                    </HEADER>
                    <BODY>
                        <DESC>
                            <STATICVARIABLES>
                                <SVEXPORTFORMAT>$$  SysName:XML</SVEXPORTFORMAT>
                            </STATICVARIABLES>
                            <TDL>
                                <TDLMESSAGE>
                                    <COLLECTION NAME="Ledger Collection" ISMODIFY="No">
                                        <TYPE>Ledger</TYPE>
                                        <FETCH>NAME</FETCH>
                                        <COMPUTE>Count:  $$CollectionField:$NAME:Count:Ledger</COMPUTE>
                                    </COLLECTION>
                                </TDLMESSAGE>
                            </TDL>
                        </DESC>
                    </BODY>
                </ENVELOPE>"""
            elif operation == 'full':
                xml_request = f"""<ENVELOPE>
                    <HEADER>
                        <VERSION>1</VERSION>
                        <TALLYREQUEST>Export</TALLYREQUEST>
                        <TYPE>Collection</TYPE>
                        <ID>Ledger Details</ID>
                    </HEADER>
                    <BODY>
                        <DESC>
                            <STATICVARIABLES>
                                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                            </STATICVARIABLES>
                            <TDL>
                                <TDLMESSAGE>
                                    <COLLECTION NAME="Ledger Details" ISMODIFY="No">
                                        <TYPE>Ledger</TYPE>
                                        <FETCH>
                                            NAME, PARENT, ADDRESS, STATE, COUNTRY, 
                                            PINCODE, EMAIL, PHONE, MOBILE, GSTIN,
                                            OPENINGBALANCE, CLOSINGBALANCE, ALTEREDON
                                        </FETCH>
                                    </COLLECTION>
                                </TDLMESSAGE>
                            </TDL>
                        </DESC>
                    </BODY>
                </ENVELOPE>"""
                response = requests.post(
                    url,
                    data=xml_request,
                    headers={'Content-Type': 'application/xml'},
                    timeout=10
                )
                if "License server is Running" in response.text:
                    return (
                        "Tally XML service is not enabled!\n"
                        "Please enable XML in Tally:\n"
                        "1. Open Tally\n"
                        "2. Press F12 > Configure\n"
                        "3. Go to Advanced Configuration\n"
                        "4. Set 'Enable XML' to Yes\n"
                        "5. Restart Tally"
                    )
                if response.status_code != 200:
                    return f"Tally connection failed: {response.status_code} - {response.text}"
                parser = etree.XMLParser(recover=True)
                root = etree.fromstring(response.content, parser=parser)
                namespaces = {}
                ledgers = root.findall('.//LEDGER')
                if not ledgers:
                    return "No ledgers found"
                
                # Define parse_date function for sorting
                def parse_date(date_str):
                    if not date_str:
                        return None
                    try:
                        return datetime.strptime(date_str, "%d/%m/%y").date()
                    except ValueError:
                        return None
                
                ledger_entries = []
                ledger_data_list = []
                ledger_name_width = 50
                parent_width = 30
                opening_balance_width = 20
                closing_balance_width = 20
                altered_on_width = 15
                
                # Collect ledger data and formatted entries
                for ledger in ledgers:
                    name = ledger.attrib.get('NAME', '').strip()
                    if not name:
                        name_elem = ledger.find('NAME')
                        if name_elem is not None and name_elem.text:
                            name = name_elem.text.strip()
                    if not name:
                        continue
                    fields = {
                        'PARENT': '',
                        'ADDRESS': '',
                        'STATE': '',
                        'COUNTRY': '',
                        'PINCODE': '',
                        'EMAIL': '',
                        'PHONE': '',
                        'MOBILE': '',
                        'GSTIN': '',
                        'OPENINGBALANCE': '',
                        'CLOSINGBALANCE': '',
                        'ALTEREDON': ''
                    }
                    for field in fields.keys():
                        elem = ledger.find(field)
                        if elem is not None and elem.text:
                            fields[field] = elem.text.strip()
                    altered_on = fields.get('ALTEREDON', '')
                    if altered_on:
                        try:
                            date_obj = datetime.strptime(altered_on, "%Y%m%d")
                            formatted_date = date_obj.strftime("%d/%m/%y")
                        except ValueError:
                            formatted_date = altered_on
                    else:
                        formatted_date = ""
                    opening_display = fields['OPENINGBALANCE']
                    if opening_display:
                        try:
                            balance = float(opening_display)
                            opening_display = f"₹{balance:,.2f}" if balance != 0 else "₹0.00"
                        except:
                            pass
                    closing_display = fields['CLOSINGBALANCE']
                    if closing_display:
                        try:
                            balance = float(closing_display)
                            closing_display = f"₹{balance:,.2f}" if balance != 0 else "₹0.00"
                        except:
                            pass
                    ledger_line = (
                        name.ljust(ledger_name_width) + " | " +
                        fields['PARENT'].ljust(parent_width) + " | " +
                        opening_display.rjust(opening_balance_width) + " | " +
                        closing_display.rjust(closing_balance_width) + " | " +
                        formatted_date.ljust(altered_on_width)
                    )
                    details = []
                    for field in ['ADDRESS', 'STATE', 'COUNTRY', 'PINCODE', 'EMAIL', 'PHONE', 'MOBILE', 'GSTIN']:
                        if fields[field]:
                            details.append(f"    ◦ {field}: {fields[field]}")
                    formatted = ledger_line
                    if details:
                        formatted += "\n" + "\n".join(details)
                    altered_on_date = parse_date(formatted_date)
                    ledger_entries.append((formatted, altered_on_date))
                    ledger_data = {
                        'LEDGER NAME': name,
                        'PARENT': fields['PARENT'],
                        'OPENING BALANCE': fields['OPENINGBALANCE'],
                        'CLOSING BALANCE': fields['CLOSINGBALANCE'],
                        'ALTERED ON': formatted_date
                    }
                    ledger_data_list.append(ledger_data)
                
                if ledger_data_list:
                    # Sort ledger_entries by altered_on_date in descending order
                    default_date = datetime(1900, 1, 1).date()
                    ledger_entries.sort(key=lambda x: x[1] if x[1] is not None else default_date, reverse=True)
                    formatted_results = [entry[0] for entry in ledger_entries]
                    
                    # Database saving logic
                    if 'db_conn_str' in self.data_source:
                        try:
                            db_engine = sqlalchemy.create_engine(self.data_source['db_conn_str'])
                            with db_engine.connect() as conn:
                                # Check if table exists
                                table_exists = conn.execute(sqlalchemy.text(
                                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tally_ledger_data')"
                                )).fetchone()[0]
                                if not table_exists:
                                    # Create table if it doesn't exist
                                    create_table_query = """
                                    CREATE TABLE tally_ledger_data (
                                    ledger_name TEXT,
                                    parent TEXT,
                                    opening_balance NUMERIC,
                                    closing_balance NUMERIC,
                                    altered_on DATE
                                    )
                                    """
                                    conn.execute(sqlalchemy.text(create_table_query))
                                    conn.commit()
                                
                                # Get max altered_on date from database
                                max_date_result = conn.execute(sqlalchemy.text(
                                    "SELECT MAX(altered_on) FROM tally_ledger_data"
                                )).fetchone()
                                max_date = max_date_result[0] if max_date_result else None
                                
                                # Function to parse balance
                                def parse_balance(balance_str):
                                    if not balance_str:
                                        return 0.0
                                    cleaned = balance_str.replace('₹', '').replace(',', '').strip()
                                    try:
                                        return float(cleaned)
                                    except ValueError:
                                        return 0.0
                                
                                # Prepare data to insert
                                new_records = []
                                for ledger in ledger_data_list:
                                    altered_on_str = ledger['ALTERED ON']
                                    if altered_on_str:
                                        try:
                                            altered_on = datetime.strptime(altered_on_str, "%d/%m/%y").date()
                                            # If no data in table or date is newer, add to new_records
                                            if max_date is None or altered_on > max_date:
                                                new_records.append({
                                                    'ledger_name': ledger['LEDGER NAME'],
                                                    'parent': ledger['PARENT'],
                                                    'opening_balance': parse_balance(ledger['OPENING BALANCE']),
                                                    'closing_balance': parse_balance(ledger['CLOSING BALANCE']),
                                                    'altered_on': altered_on
                                                })
                                        except ValueError:
                                            continue
                                
                                if new_records:
                                    # Insert new records into database
                                    df_new = pd.DataFrame(new_records)
                                    df_new.to_sql('tally_ledger_data', db_engine, if_exists='append', index=False)
                                    conn.commit()
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Inserted {len(new_records)} new records into the database.")
                                else:
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No new data to insert.")
                        except Exception as e:
                            print(f"Database error: {str(e)}")
                    
                    # Prepare header for display
                    header = (
                        "LEDGER NAME".ljust(ledger_name_width) + " | " +
                        "PARENT".ljust(parent_width) + " | " +
                        "OPENING BALANCE".rjust(opening_balance_width) + " | " +
                        "CLOSING BALANCE".rjust(closing_balance_width) + " | " +
                        "ALTERED ON".ljust(altered_on_width)
                    )
                    separator = "-" * len(header)
                    # return header + "\n" + separator + "\n" + "\n\n".join(formatted_results) + f"\n\nData has been saved to {csv_path}"
                else:
                    return "No ledgers found"
            return "Tally operation completed"
        except etree.ParseError as e:
            error_line = response.text.splitlines()[e.position[0]-1] if e.position and e.position[0] > 0 else ""
            return f"XML Parsing Error: {str(e)}\nProblem at: {error_line}"
        except requests.exceptions.ConnectionError:
            return (
                "Cannot connect to Tally. Make sure:\n"
                "1. Tally is running\n"
                "2. XML services are enabled (F12 > Configure > Advanced Config)\n"
                "3. You're using correct host/port (default: localhost:9000)"
            )
        except Exception as e:
            return f"Tally Error: {str(e)}"

    def show_schema_info(self):
        if not self.available_tables:
            return "No schema information"
        info = ""
        for table in self.available_tables:
            info += f"\nTable: {table}\n"
            if table in self.table_columns:
                cols = self.table_columns[table]['all']
                info += f"  Columns: {cols}\n"
        return info

    def ask(self, question):
        try:
            if question.lower() in ['show tables', 'show schema', 'tables', 'schema']:
                return self.show_schema_info()
            lang = self._detect_language(question)
            processed_question = question
            if lang == 'hi':
                processed_question = self._translate_hindi_to_english(question)
            return self._process_query_directly(processed_question)
        except Exception as e:
            return f"Error: {str(e)}"

class QueryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("AI Query Engine")
        self.root.geometry("1000x800")
        self.root.configure(bg="#faf3f3")
        self.engine = None
        self.current_config = None
        self.auto_fetch_enabled = False  # Flag for automatic fetching
        self.main_frame = ttk.Frame(root, padding=10)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        title_label = ttk.Label(
            self.main_frame, 
            text="🚀 AI Query Engine", 
            font=("Helvetica", 16, "bold"),
            foreground="#2c3e50"
        )
        title_label.pack(pady=10)
        config_frame = ttk.LabelFrame(
            self.main_frame, 
            text="🔧 Data Source Configuration", 
            padding=10
        )
        config_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(config_frame, text="Data Source Type:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.source_type = ttk.Combobox(
            config_frame, 
            values=["Excel File", "CSV File", "PostgreSQL", "MySQL", "Tally ERP"],
            state="readonly",
            width=15
        )
        self.source_type.grid(row=0, column=1, padx=5, pady=5)
        self.source_type.current(0)
        ttk.Label(config_frame, text="File Path:").grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        self.file_path = ttk.Entry(config_frame, width=30)
        self.file_path.grid(row=0, column=3, padx=5, pady=5)
        ttk.Button(
            config_frame, 
            text="Browse", 
            command=self.browse_file,
            width=10
        ).grid(row=0, column=4, padx=5, pady=5)
        self.db_frame = ttk.Frame(config_frame)
        ttk.Label(self.db_frame, text="Host:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.db_host = ttk.Entry(self.db_frame, width=15)
        self.db_host.grid(row=0, column=1, padx=5, pady=2)
        self.db_host.insert(0, "localhost")
        ttk.Label(self.db_frame, text="Port:").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.db_port = ttk.Entry(self.db_frame, width=10)
        self.db_port.grid(row=0, column=3, padx=5, pady=2)
        self.db_port.insert(0, "5432")
        ttk.Label(self.db_frame, text="Database:").grid(row=0, column=4, sticky=tk.W, padx=5, pady=2)
        self.db_name = ttk.Entry(self.db_frame, width=15)
        self.db_name.grid(row=0, column=5, padx=5, pady=2)
        ttk.Label(self.db_frame, text="Username:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.db_user = ttk.Entry(self.db_frame, width=15)
        self.db_user.grid(row=1, column=1, padx=5, pady=2)
        ttk.Label(self.db_frame, text="Password:").grid(row=1, column=2, sticky=tk.W, padx=5, pady=2)
        self.db_pass = ttk.Entry(self.db_frame, width=15, show="*")
        self.db_pass.grid(row=1, column=3, padx=5, pady=2)
        self.tally_frame = ttk.Frame(config_frame)
        ttk.Label(self.tally_frame, text="Host:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.tally_host = ttk.Entry(self.tally_frame, width=15)
        self.tally_host.grid(row=0, column=1, padx=5, pady=2)
        self.tally_host.insert(0, "localhost")
        ttk.Label(self.tally_frame, text="Port:").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        self.tally_port = ttk.Entry(self.tally_frame, width=10)
        self.tally_port.grid(row=0, column=3, padx=5, pady=2)
        self.tally_port.insert(0, "9000")
        self.connect_btn = ttk.Button(
            config_frame, 
            text="Connect", 
            command=self.connect_to_source,
            style="Accent.TButton"
        )
        self.connect_btn.grid(row=2, column=4, padx=5, pady=10)
        self.status_var = tk.StringVar(value="❌ Not connected")
        self.status_label = ttk.Label(
            config_frame, 
            textvariable=self.status_var,
            foreground="red"
        )
        self.status_label.grid(row=2, column=0, columnspan=4, sticky=tk.W, padx=5, pady=5)
        query_frame = ttk.LabelFrame(
            self.main_frame, 
            text="❓ Ask a Question", 
            padding=10
        )
        query_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        ttk.Label(query_frame, text="Enter your question:").pack(anchor=tk.W, padx=5, pady=2)
        self.question_entry = ttk.Entry(query_frame, width=80)
        self.question_entry.pack(fill=tk.X, padx=5, pady=5)
        self.question_entry.bind("<Return>", self.ask_question)
        examples = [
            "Show all ledgers with balances",
            "Display customer names with closing balances",
            "List suppliers with current balances",
            "Count number of transactions"
        ]
        example_label = ttk.Label(
            query_frame, 
            text=f"Examples: {', '.join(examples)}",
            font=("Helvetica", 9),
            foreground="#7f8c8d"
        )
        example_label.pack(anchor=tk.W, padx=5, pady=2)
        ttk.Button(
            query_frame, 
            text="Ask", 
            command=self.ask_question,
            style="Accent.TButton"
        ).pack(anchor=tk.E, padx=5, pady=5)
        ttk.Label(query_frame, text="Results:").pack(anchor=tk.W, padx=5, pady=5)
        self.result_text = scrolledtext.ScrolledText(
            query_frame, 
            height=20,
            wrap=tk.WORD,
            font=("Courier New", 10)
        )
        self.result_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.result_text.config(state=tk.DISABLED)
        self.source_type.bind("<<ComboboxSelected>>", self.update_config_fields)
        self.style = ttk.Style()
        self.style.configure("Accent.TButton", background="#3498db", foreground="white")
        self.style.map("Accent.TButton", background=[("active", "#2980b9")])
        self.source_type.focus_set()
        self.update_config_fields()
        
    def update_config_fields(self, event=None):
        source_type = self.source_type.get()
        self.file_path.grid_forget()
        self.db_frame.grid_forget()
        self.tally_frame.grid_forget()
        if source_type in ["Excel File", "CSV File"]:
            self.file_path.grid(row=0, column=3, padx=5, pady=5)
            self.connect_btn.grid(row=1, column=4, padx=5, pady=10)
        elif source_type in ["PostgreSQL", "MySQL"]:
            self.db_frame.grid(row=0, column=2, columnspan=3, padx=5, pady=5)
            self.connect_btn.grid(row=1, column=4, padx=5, pady=10)
        elif source_type == "Tally ERP":
            self.tally_frame.grid(row=0, column=2, columnspan=2, padx=5, pady=5)
            self.db_frame.grid(row=1, column=0, columnspan=5, padx=5, pady=5)
            self.connect_btn.grid(row=2, column=4, padx=5, pady=10)
            
    def browse_file(self):
        filetypes = []
        if self.source_type.get() == "Excel File":
            filetypes = [("Excel files", "*.xlsx *.xls")]
        elif self.source_type.get() == "CSV File":
            filetypes = [("CSV files", "*.csv")]
        filename = filedialog.askopenfilename(filetypes=filetypes)
        if filename:
            self.file_path.delete(0, tk.END)
            self.file_path.insert(0, filename)
            
    def connect_to_source(self):
        source_type = self.source_type.get()
        config = {}
        try:
            if source_type == "Excel File":
                path = self.file_path.get()
                if not path:
                    messagebox.showerror("Error", "Please select an Excel file")
                    return
                config = {'type': 'excel', 'path': path}
            elif source_type == "CSV File":
                path = self.file_path.get()
                if not path:
                    messagebox.showerror("Error", "Please select a CSV file")
                    return
                config = {'type': 'csv', 'path': path}
            elif source_type == "PostgreSQL":
                config = {
                    'type': 'postgresql',
                    'conn_str': f"postgresql://{self.db_user.get()}:{self.db_pass.get()}@{self.db_host.get()}:{self.db_port.get()}/{self.db_name.get()}"
                }
            elif source_type == "MySQL":
                config = {
                    'type': 'mysql',
                    'conn_str': f"mysql://{self.db_user.get()}:{self.db_pass.get()}@{self.db_host.get()}:{self.db_port.get()}/{self.db_name.get()}"
                }
            elif source_type == "Tally ERP":
                config = {
                    'type': 'tally',
                    'host': self.tally_host.get(),
                    'port': int(self.tally_port.get()),
                    'db_conn_str': f"postgresql://{self.db_user.get()}:{self.db_pass.get()}@{self.db_host.get()}:{self.db_port.get()}/{self.db_name.get()}"
                }
            self.status_var.set("⏳ Connecting...")
            self.root.update()
            threading.Thread(target=self.initialize_engine, args=(config,), daemon=True).start()
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect: {str(e)}")
            self.status_var.set("❌ Connection failed")
            
    def initialize_engine(self, config):
        try:
            self.engine = AIQueryEngine(config)
            self.current_config = config
            self.status_var.set("✅ Connected successfully")
            self.question_entry.focus_set()
        except Exception as e:
            self.status_var.set("❌ Connection failed")
            messagebox.showerror("Initialization Error", f"Failed to initialize engine: {str(e)}")
            
    def ask_question(self, event=None):
        if not self.engine:
            messagebox.showerror("Error", "Please connect to a data source first")
            return
        question = self.question_entry.get().strip()
        if not question:
            return
        self.result_text.config(state=tk.NORMAL)
        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, "⏳ Processing your question...")
        self.result_text.config(state=tk.DISABLED)
        self.root.update()
        threading.Thread(target=self.process_question, args=(question,), daemon=True).start()
        
    def process_question(self, question):
        try:
            start_time = time.time()
            result = self.engine.ask(question)
            elapsed = time.time() - start_time
            self.root.after(0, self.display_results, result, elapsed)
            # Enable auto-fetch after first manual "show all ledgers"
            if question.lower().strip() == "show all ledgers" and self.current_config['type'] == 'tally':
                self.auto_fetch_enabled = True
                self.schedule_auto_fetch()
        except Exception as e:
            self.root.after(0, self.display_error, str(e))
            
    def display_results(self, result, elapsed):
        self.result_text.config(state=tk.NORMAL)
        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, f"🔍 Results (took {elapsed:.2f}s):\n")
        self.result_text.insert(tk.END, "="*80 + "\n\n")
        self.result_text.insert(tk.END, str(result))
        self.result_text.config(state=tk.DISABLED)
        
    def display_error(self, error):
        self.result_text.config(state=tk.NORMAL)
        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, f"❌ Error:\n{'='*80}\n\n{error}")
        self.result_text.config(state=tk.DISABLED)
    
    def schedule_auto_fetch(self):
        """Schedule the auto-fetch to run every 60 seconds."""
        if self.auto_fetch_enabled:
            self.root.after(60000, self.auto_fetch_ledgers)
    
    def auto_fetch_ledgers(self):
        """Run the auto-fetch in a separate thread and schedule the next run."""
        if self.auto_fetch_enabled:
            threading.Thread(target=self.fetch_ledgers, daemon=True).start()
            self.schedule_auto_fetch()
    
    def fetch_ledgers(self):
        """Fetch and save ledgers automatically."""
        try:
            _ = self.engine.ask("show all ledgers")
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Auto-fetch error: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = QueryApp(root)
    root.mainloop()
