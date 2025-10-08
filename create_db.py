import mysql.connector
from mysql.connector import Error


def create_database_connection(host, user, password):
    """Create a connection to MySQL server"""
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
    """Create database if it doesn't exist"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"📦 Database '{db_name}' created or already exists")
        cursor.execute(f"USE {db_name}")
        cursor.close()
    except Error as e:
        print(f"❌ Error creating database: {e}")


def create_tables(connection):
    """Create all tables for the patient management system"""
    cursor = connection.cursor()

    # Patients table
    patients_table = """
    CREATE TABLE IF NOT EXISTS patients (
        id INT AUTO_INCREMENT PRIMARY KEY,
        org_id INT,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        father_name VARCHAR(100),
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
        INDEX idx_org_id (org_id)
    )
    """

    # Patient Relationships table
    patient_relationships_table = """
    CREATE TABLE IF NOT EXISTS patient_relationships (
        id INT AUTO_INCREMENT PRIMARY KEY,
        patient_id INT NOT NULL,
        related_patient_id INT NOT NULL,
        relationship_type VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    # Doctors table
    doctors_table = """
    CREATE TABLE IF NOT EXISTS doctors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        org_id INT,
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_org_id (org_id)
    )
    """

    # Appointments table
    appointments_table = """
    CREATE TABLE IF NOT EXISTS appointments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        org_id INT COMMENT 'Organization or clinic ID',
        patient_id INT COMMENT 'Patient ID',
        doctor_id INT COMMENT 'Doctor ID',
        appointment_date DATE,
        appointment_time TIME,
        period VARCHAR(50) COMMENT 'Morning, Afternoon, Evening, or custom label',
        revision_number INT COMMENT 'Number of times the appointment has been revised or rescheduled',
        room VARCHAR(50) COMMENT 'Room or location where the appointment will take place',
        status VARCHAR(50) COMMENT 'Scheduled, Completed, Cancelled, No-Show',
        reason_for_visit TEXT,
        diagnosis TEXT,
        prescription TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_org_id (org_id)
    )
    """

    # Billing table
    billing_table = """
    CREATE TABLE IF NOT EXISTS billing (
        id INT AUTO_INCREMENT PRIMARY KEY,
        patient_id INT COMMENT 'Patient ID',
        appointment_id INT COMMENT 'Appointment ID',
        bill_date DATE,
        consultation_charges DECIMAL(10, 2) DEFAULT 0.00,
        medication_charges DECIMAL(10, 2) DEFAULT 0.00,
        test_charges DECIMAL(10, 2) DEFAULT 0.00,
        other_charges DECIMAL(10, 2) DEFAULT 0.00,
        total_amount DECIMAL(10, 2),
        discount DECIMAL(10, 2) DEFAULT 0.00,
        tax DECIMAL(10, 2) DEFAULT 0.00,
        net_amount DECIMAL(10, 2),
        payment_status VARCHAR(50) COMMENT 'Paid, Pending, Partially Paid',
        payment_method VARCHAR(50) COMMENT 'Cash, Card, Insurance, Online',
        payment_date DATE,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """

    # Dictionary of tables for easy iteration
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
    # Database configuration
    HOST = 'localhost'
    USER = 'root'  # Change to your MySQL username
    PASSWORD = 'P@ssw0rd8899'  # Change to your MySQL password
    DATABASE_NAME = 'patient_management_system'

    # Create connection
    connection = create_database_connection(HOST, USER, PASSWORD)

    if connection:
        # Create database
        create_database(connection, DATABASE_NAME)

        # Create tables
        create_tables(connection)

        # Close connection
        if connection.is_connected():
            connection.close()
            print("\n🔒 MySQL connection closed")


if __name__ == "__main__":
    main()