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
        source_id INT,  -- Link to original system patient ID
        first_name VARCHAR(100),
        father_name VARCHAR(100),
        last_name VARCHAR(100),
        mother_name VARCHAR(100),
        id_nb VARCHAR(50),
        date_of_birth DATE,
        gender VARCHAR(20),  -- ENUM not allowed (use VARCHAR)
        marital_status VARCHAR(20),  -- ENUM not allowed
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
        patient_id INT,           -- maps to source_id of patient
        related_patient_id INT,   -- maps to source_id of related patient
        relationship_type VARCHAR(50),
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL
    )
    """

    # ---------------- Doctors Table ----------------
    doctors_table = """
    CREATE TABLE IF NOT EXISTS doctors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id INT,  -- Original doctor ID
        title VARCHAR(50),
        first_name VARCHAR(50),
        father_name VARCHAR(100),
        last_name VARCHAR(50),
        specialization VARCHAR(100),
        qualification VARCHAR(200),
        license_number VARCHAR(50) UNIQUE,
        phone VARCHAR(100),
        phone_alt VARCHAR(100),
        email VARCHAR(100) UNIQUE,
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
        source_id INT COMMENT 'Original appointment ID from source system',
        patient_id INT COMMENT 'Patient source_id (not FK)',
        doctor_id INT COMMENT 'Doctor source_id (not FK)',
        appointment_date DATE,
        appointment_time TIME,
        duration VARCHAR(50),
        revision_number INT,
        room VARCHAR(50),
        status VARCHAR(50),  -- ENUM not allowed (Scheduled, Completed, etc.)
        missed BOOLEAN DEFAULT FALSE,
        reason_for_visit TEXT,
        diagnosis TEXT,
        prescription TEXT,
        notes TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        INDEX idx_source_id (source_id)
    )
    """

    # ---------------- Billing Table ----------------
    billing_table = """
    CREATE TABLE IF NOT EXISTS billing (
        id INT AUTO_INCREMENT PRIMARY KEY,
        patient_id INT COMMENT 'Patient source_id',
        appointment_id INT COMMENT 'Appointment source_id',
        bill_date DATE,
        consultation_charges DECIMAL(10, 2) DEFAULT 0.00,
        medication_charges DECIMAL(10, 2) DEFAULT 0.00,
        test_charges DECIMAL(10, 2) DEFAULT 0.00,
        other_charges DECIMAL(10, 2) DEFAULT 0.00,
        total_amount DECIMAL(10, 2),
        discount DECIMAL(10, 2) DEFAULT 0.00,
        tax DECIMAL(10, 2) DEFAULT 0.00,
        net_amount DECIMAL(10, 2),
        payment_status VARCHAR(50),  -- ENUM not allowed (Paid, Pending, etc.)
        payment_method VARCHAR(50),  -- ENUM not allowed (Cash, Card, etc.)
        payment_date DATE,
        notes TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL
    )
    """

    # Store table definitions
    tables = {
        'patients': patients_table,
        'doctors': doctors_table,
        'appointments': appointments_table,
        'billing': billing_table,
        'patient_relationships': patient_relationships_table
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
    DATABASE_NAME = 'patient_management_system'

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