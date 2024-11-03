General Project Description
This end-to-end data engineering and artificial intelligence project demonstrates the complete process of acquiring, cleaning, streaming, and storing data in a database. It leverages Python scripts, Kafka for real-time data streaming, and MySQL as the final data storage solution. The aim is to simulate a realistic data pipeline where cleaned e-commerce data is processed and stored for further analysis or modeling.

Process Steps and Explanation by File
download_dataset.py
This script is responsible for automating the download of a dataset from Kaggle. Using the kaggle.json credentials, it authenticates with Kaggle, retrieves the specified e-commerce dataset, and saves it locally. This ensures that the latest data is always available and simplifies the acquisition process for repeated or automated use.

Console Setup: Before running this script, you need to set up Kaggle credentials (kaggle.json). Once configured, the script will download and unzip the dataset directly from Kaggle.
eda.py
The eda.py script performs basic data cleaning and preliminary analysis on the downloaded dataset. It loads the data, corrects encoding issues, and formats key columns (e.g., converting dates and ensuring numeric types). Key steps include:

Filtering out negative quantities.
Removing duplicates and handling missing values in crucial columns.
Creating a new Revenue field to calculate total income for each transaction and extracting the Month for time-based analysis.
The cleaned data is then saved as ecommerce_data_cleaned.csv, which serves as the input for the Kafka producer-consumer pipeline.

kafka_provider.py
This script simulates a real-time data stream by acting as a Kafka producer. It reads each row from the cleaned dataset (ecommerce_data_cleaned.csv) and sends it as a message to a Kafka topic (e-commerce-group). Key configuration steps include:

Setting up the producer with KafkaProducer and specifying serialization for JSON format.

Iterating through the data to send each row as a message, simulating real-time data generation.

Local Setup: To run this script, Kafka and Zookeeper need to be installed and running on the local machine. This requires starting the Zookeeper service and then the Kafka broker. The Kafka topic should also be created prior to streaming data.

kafka_consumer.py
Acting as a consumer, this script receives messages from the Kafka topic and stores them in a MySQL database. For each message (representing a transaction), it inserts the data into a transactions table in MySQL. Key tasks include:

Connecting to a MySQL database using pymysql.

Processing each message received from Kafka, transforming it into the correct format for database insertion.

Storing transaction data with columns such as InvoiceNo, StockCode, Description, and Revenue, making the data readily available for analysis.

Database Setup: MySQL must be installed locally, and the database should be created prior to running the consumer script. Additionally, the MySQL service should be started, and appropriate access permissions set for the user specified in the script.

ecommerce.sql
This SQL file defines the schema for the transactions table in MySQL. The table includes fields for transaction information such as InvoiceNo, StockCode, Description, Quantity, InvoiceDate, and Revenue. This table is essential for storing structured data received from the Kafka consumer.

Console Setup: To set up the database and table, log into MySQL through the terminal (CMD) and execute commands to create the database and table. Install MySQL and set the environment path, then access the MySQL CLI with:
bash
Copiar código
mysql -u your_username -p
Once logged in, create the database and run ecommerce.sql to define the transactions table schema.
Additional Notes
To replicate this setup, ensure all dependencies (Kafka, MySQL, Python packages) are installed and configured properly. This project showcases the integration of data engineering techniques with streaming and database storage, providing a framework adaptable to various real-time data applications.

#---------------------------------------------------------------------------------------------------------------------


