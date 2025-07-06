TallyInsightEngine
TallyInsightEngine is a powerful tool that extracts financial data from Tally ERP, stores it in a PostgreSQL database, and provides intelligent querying capabilities through both a desktop GUI and a web interface. It leverages AI to understand natural language queries and retrieve relevant data from the database.
Features

Extract data from Tally ERP and save to PostgreSQL
Intelligent querying using AI for natural language processing
Desktop GUI for easy interaction
Web interface for remote access
Support for multiple data sources (Excel, CSV, PostgreSQL, MySQL, Tally ERP)
Automatic data fetching and updating

Requirements

Python 3.8+
PostgreSQL database
Tally ERP (for data extraction)
Necessary Python libraries (listed in requirements.txt)

Installation

Clone the repository:
git clone https://github.com/yourusername/TallyInsightEngine.git
cd TallyInsightEngine


Create a virtual environment and activate it:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


Install the required packages:
pip install -r requirements.txt


Set up the PostgreSQL database:

Create a database named tall_ydata
Update the database connection details in web_app.py (e.g., host, user, password)
Optionally, update the connection string in desktop_app.py if different from defaults


For Tally ERP integration:

Ensure Tally is running with XML services enabled (F12 > Configure > Advanced Configuration > Enable XML: Yes)
Update the host and port in desktop_app.py if different from localhost:9000


For the desktop application:

Download the FastText language identification model lid.176.ftz from FastText and place it in the project directory.



Usage

Run the desktop application:
python desktop_app.py


Configure the data source (e.g., Tally ERP or PostgreSQL)
Ask questions through the GUI (e.g., "Show all ledgers with balances")


Run the web application:
python web_app.py


Access the web interface at https://localhost:5000 (or your configured IP/port)
Ask questions through the web form (e.g., "Total closing balance for 2024")



Configuration

Database Connection: Update the connection string in web_app.py (e.g., psycopg2.connect(...)) and optionally in desktop_app.py under the Tally configuration.
Tally ERP: Ensure XML services are enabled and update host/port in desktop_app.py if necessary.
SSL Certificates: For the web application, provide paths to cert.pem and key.pem in web_app.py. For development, you can disable SSL by removing the ssl_context parameter in app.run().

File Structure

desktop_app.py: Script for the desktop GUI application that extracts data from Tally and supports multiple data sources.
web_app.py: Script for the web interface that queries the PostgreSQL database using AI.
requirements.txt: List of Python dependencies.
README.md: This file.

Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.
License
This project is licensed under the MIT License.
Contact
For any queries, please contact [your email or GitHub profile].
