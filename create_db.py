# ---------------------------------------------
# ToothpickEVE - Database Setup Script
# Purpose: Create a cleaned MySQL database for clinic analytics
#
# Notes:
#   - ❌ No ENUM fields are allowed.
#     All fields that could use ENUM (e.g. gender, marital_status, status)
#     are defined as VARCHAR for flexibility.
#   - ❌ No foreign key constraints.
#     Relationships will be handled using `source_id` fields only.
#   - ✅ Designed for easy data import and transformation from multiple sources.
# ---------------------------------------------

import mysql.connector
from mysql.connector import Error


def create_database_connection(host, user, password):
    """Connect to MySQL server without selecting a specific database"""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        if connection.is_connected():
            print("✅ Successfully connected to MySQL server")
            return connection
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


def create_database(connection, db_name):
    """Create the new cleaned database if it doesn't already exist"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"📦 Database '{db_name}' created or already exists")
        cursor.execute(f"USE {db_name}")
        cursor.close()
    except Error as e:
        print(f"❌ Error creating database: {e}")


def create_tables(connection):
    """Create all tables needed for the cleaned system"""
    cursor = connection.cursor()

    # ---------------- Patients Table ----------------
    patients_table = """
    CREATE TABLE IF NOT EXISTS patients (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original patient ID from source system',
        first_name VARCHAR(100),
        father_name VARCHAR(100),
        last_name VARCHAR(100),
        mother_name VARCHAR(100),
        id_nb VARCHAR(50),
        date_of_birth DATE,
        gender VARCHAR(20),
        marital_status VARCHAR(20),
        nationality VARCHAR(100),
        phone VARCHAR(100),
        phone_alt VARCHAR(100),
        email VARCHAR(100),
        address_line1 TEXT,
        address_line2 TEXT,
        city VARCHAR(50),
        state VARCHAR(50),
        zip_code VARCHAR(10),
        country VARCHAR(100),
        blood_group VARCHAR(5),
        allergies TEXT,
        medical_history TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id)
    )
    """

    # ---------------- Patient Relationships Table ----------------
    patient_relationships_table = """
    CREATE TABLE IF NOT EXISTS patient_relationships (
        id INT AUTO_INCREMENT PRIMARY KEY,
        patient_id INT COMMENT 'Maps to source_id of patient',
        related_patient_id INT COMMENT 'Maps to source_id of related patient',
        relationship_type VARCHAR(50),
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL
    )
    """

    # ---------------- Doctors Table ----------------
    doctors_table = """
    CREATE TABLE IF NOT EXISTS doctors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original doctor ID from source system',
        title VARCHAR(50),
        first_name VARCHAR(50),
        father_name VARCHAR(100),
        last_name VARCHAR(50),
        specialization VARCHAR(100),
        qualification VARCHAR(200),
        license_number VARCHAR(50),
        phone VARCHAR(100),
        phone_alt VARCHAR(100),
        email VARCHAR(100),
        department VARCHAR(100),
        consultation_fee DECIMAL(10, 2),
        available_days VARCHAR(100),
        available_hours VARCHAR(100),
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id)
    )
    """

    # ---------------- Appointments Table ----------------
    appointments_table = """
    CREATE TABLE IF NOT EXISTS appointments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original appointment ID from source system',
        patient_id VARCHAR(50) COMMENT 'Patient source_id (not FK)',
        doctor_id VARCHAR(50) COMMENT 'Doctor source_id (not FK)',
        appointment_date DATE,
        appointment_time TIME,
        duration VARCHAR(50),
        duration_minutes INT,
        revision_number INT,
        room VARCHAR(50),
        status VARCHAR(50),
        missed BOOLEAN DEFAULT FALSE,
        reason_for_visit TEXT,
        diagnosis TEXT,
        prescription TEXT,
        notes TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id),
        INDEX idx_patient_id (patient_id),
        INDEX idx_doctor_id (doctor_id),
        INDEX idx_appointment_date (appointment_date)
    )
    """

    # ---------------- Invoices Table ----------------
    invoices_table = """
    CREATE TABLE IF NOT EXISTS invoices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original invoice ID from source system',
        invoice_number VARCHAR(50),
        patient_id VARCHAR(50) COMMENT 'Patient source_id (not FK)',
        doctor_id VARCHAR(50) COMMENT 'Doctor source_id (not FK)',
        appointment_id VARCHAR(50) COMMENT 'Appointment source_id (not FK)',
        invoice_date DATE,
        due_date DATE,
        status VARCHAR(50),
        currency VARCHAR(10) DEFAULT 'USD',
        subtotal DECIMAL(10, 2) DEFAULT 0.00,
        discount_type VARCHAR(20),
        discount_value DECIMAL(10, 2) DEFAULT 0.00,
        tax DECIMAL(10, 2) DEFAULT 0.00,
        total_amount DECIMAL(10, 2),
        amount_paid DECIMAL(10, 2) DEFAULT 0.00,
        balance_due DECIMAL(10, 2),
        notes TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id),
        INDEX idx_patient_id (patient_id),
        INDEX idx_invoice_date (invoice_date)
    )
    """

    # ---------------- Invoice Items Table ----------------
    invoice_items_table = """
    CREATE TABLE IF NOT EXISTS invoice_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) COMMENT 'Original item ID from source system',
        invoice_id INT COMMENT 'Links to invoices.id (internal)',
        invoice_source_id VARCHAR(50) COMMENT 'Original invoice ID',
        description TEXT,
        unit_price DECIMAL(10, 2),
        quantity INT DEFAULT 1,
        total_amount DECIMAL(10, 2),
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_invoice_id (invoice_id),
        INDEX idx_invoice_source_id (invoice_source_id)
    )
    """

    # ---------------- Payments Table ----------------
    payments_table = """
    CREATE TABLE IF NOT EXISTS payments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original payment ID from source system',
        invoice_id INT COMMENT 'Links to invoices.id (internal)',
        invoice_source_id VARCHAR(50) COMMENT 'Original invoice ID',
        patient_id VARCHAR(50) COMMENT 'Patient source_id (not FK)',
        payment_method VARCHAR(50),
        amount DECIMAL(10, 2),
        original_amount DECIMAL(10, 2),
        currency VARCHAR(10) DEFAULT 'USD',
        reference_number VARCHAR(100),
        payment_date DATE,
        notes TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        deleted_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id),
        INDEX idx_invoice_id (invoice_id),
        INDEX idx_invoice_source_id (invoice_source_id),
        INDEX idx_patient_id (patient_id),
        INDEX idx_payment_date (payment_date)
    )
    """

    # ---------------- Treatments Table ----------------
    treatments_table = """
    CREATE TABLE IF NOT EXISTS treatments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original treatment/operation ID from source system',
        patient_id VARCHAR(50) COMMENT 'Patient source_id (not FK)',
        doctor_id VARCHAR(50) COMMENT 'Doctor source_id (not FK)',
        tooth_number VARCHAR(20),
        procedure_code VARCHAR(50),
        procedure_name VARCHAR(200),
        procedure_group VARCHAR(100),
        treatment_plan VARCHAR(100),
        status VARCHAR(50),
        price DECIMAL(10, 2),
        planned_date DATE,
        start_date DATE,
        completion_date DATE,
        notes TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id),
        INDEX idx_patient_id (patient_id),
        INDEX idx_doctor_id (doctor_id),
        INDEX idx_status (status)
    )
    """

    # ---------------- Inventory Table ----------------
    inventory_table = """
    CREATE TABLE IF NOT EXISTS inventory (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE COMMENT 'Original item ID from source system',
        category VARCHAR(100),
        name VARCHAR(200),
        sku VARCHAR(100),
        description TEXT,
        unit_of_measure VARCHAR(50),
        size DECIMAL(10, 2),
        quantity_in_stock DECIMAL(10, 2) DEFAULT 0.00,
        unit_size DECIMAL(10, 2),
        average_purchase_price DECIMAL(10, 2),
        selling_price DECIMAL(10, 2),
        minimum_quantity_warning INT,
        minimum_quantity_critical INT,
        currency VARCHAR(10) DEFAULT 'USD',
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        deleted_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id),
        INDEX idx_name (name)
    )
    """

    # Store table definitions
    tables = {
        'patients': patients_table,
        'patient_relationships': patient_relationships_table,
        'doctors': doctors_table,
        'appointments': appointments_table,
        'invoices': invoices_table,
        'invoice_items': invoice_items_table,
        'payments': payments_table,
        'treatments': treatments_table,
        'inventory': inventory_table
    }

    try:
        for table_name, table_query in tables.items():
            cursor.execute(table_query)
            print(f"🧱 Table '{table_name}' created successfully")

        connection.commit()
        cursor.close()
        print("\n✅ All tables created successfully!")
    except Error as e:
        print(f"❌ Error creating tables: {e}")


def main():
    """Main function to initialize the database"""
    HOST = 'localhost'
    USER = 'root'
    PASSWORD = 'P@ssw0rd8899'
    DATABASE_NAME = 'hammoud_patient_management_system'

    # Step 1: Connect to server
    connection = create_database_connection(HOST, USER, PASSWORD)

    if connection:
        # Step 2: Create or select database
        create_database(connection, DATABASE_NAME)

        # Step 3: Create all tables
        create_tables(connection)

        # Step 4: Close connection
        if connection.is_connected():
            connection.close()
            print("\n🔒 MySQL connection closed")


if __name__ == "__main__":
    main()