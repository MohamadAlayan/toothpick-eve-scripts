import os
import re
import pyodbc
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import traceback
import json


# ================================================================
# CONFIGURATION
# ================================================================

class Config:
    """Centralized configuration"""
    # MSSQL Settings
    MSSQL_SERVER = "localhost"
    MSSQL_DATABASE = "BizriDental"
    USE_WINDOWS_AUTH = True
    MSSQL_USERNAME = None
    MSSQL_PASSWORD = None

    # MySQL Settings
    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "P@ssw0rd8899"
    MYSQL_DATABASE = "patient_management_system"

    # Migration Settings
    BATCH_SIZE = 100  # Process in batches
    DEBUG_MODE = True  # Enable detailed logging
    TEST_MODE = False  # If True, only process first 10 records
    CREATE_TABLES = True  # Auto-create tables if missing


# ================================================================
# DATABASE CONNECTIONS
# ================================================================

def create_mssql_connection(server, database, use_windows_auth=True, username=None, password=None):
    """Connect to SQL Server with improved error handling."""
    try:
        if use_windows_auth:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

        # Try newer driver first, fallback to older
        try:
            conn = pyodbc.connect(conn_str)
        except:
            conn_str = conn_str.replace("ODBC Driver 17 for SQL Server", "SQL Server")
            conn = pyodbc.connect(conn_str)

        print("✅ Connected to SQL Server")
        return conn
    except pyodbc.Error as e:
        print(f"❌ SQL Server connection error: {e}")
        return None


def create_mysql_connection(host, user, password, database):
    """Connect to MySQL with improved error handling."""
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=False,  # Explicit transaction control
            use_unicode=True,
            charset='utf8mb4'
        )
        if conn.is_connected():
            print("✅ Connected to MySQL")
            return conn
    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        return None


# ================================================================
# TABLE CREATION
# ================================================================

def create_tables_if_missing(mysql_conn):
    """Create necessary tables if they don't exist."""
    cursor = mysql_conn.cursor()

    # Create patients table
    patients_table = """
    CREATE TABLE IF NOT EXISTS patients (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE NOT NULL,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        father_name VARCHAR(100),
        mother_name VARCHAR(100),
        id_nb VARCHAR(50),
        date_of_birth DATE,
        gender VARCHAR(10),
        marital_status VARCHAR(20),
        nationality VARCHAR(100),
        phone VARCHAR(50),
        phone_alt VARCHAR(50),
        email VARCHAR(100),
        address_line1 TEXT,
        address_line2 TEXT,
        city VARCHAR(50),
        state VARCHAR(50),
        zip_code VARCHAR(10),
        blood_group VARCHAR(5),
        allergies TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_source_id (source_id),
        INDEX idx_name (first_name, last_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    # Create doctors table
    doctors_table = """
    CREATE TABLE IF NOT EXISTS doctors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source_id VARCHAR(50) UNIQUE NOT NULL,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        phone VARCHAR(50),
        phone_alt VARCHAR(50),
        license_number VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_source_id (source_id),
        INDEX idx_name (first_name, last_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    # Create migration_log table
    migration_log_table = """
    CREATE TABLE IF NOT EXISTS migration_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(50),
        source_id VARCHAR(50),
        operation VARCHAR(20),
        status VARCHAR(20),
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_status (status),
        INDEX idx_table_source (table_name, source_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    try:
        cursor.execute(patients_table)
        cursor.execute(doctors_table)
        cursor.execute(migration_log_table)
        mysql_conn.commit()
        print("✅ Tables verified/created successfully")
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        raise
    finally:
        cursor.close()


# ================================================================
# UTILITIES
# ================================================================

def ensure_logs_folder():
    """Ensure the logs directory exists."""
    if not os.path.exists("logs"):
        os.makedirs("logs")
    return "logs"


def log_error(mysql_conn, table_name, source_id, operation, error_message):
    """Log errors to database for tracking."""
    try:
        cursor = mysql_conn.cursor()
        query = """
        INSERT INTO migration_log (table_name, source_id, operation, status, error_message)
        VALUES (%s, %s, %s, 'ERROR', %s)
        """
        cursor.execute(query, (table_name, str(source_id), operation, str(error_message)[:1000]))
        mysql_conn.commit()
        cursor.close()
    except:
        pass  # Don't let logging errors stop migration


def load_nationality_mapping(mssql_conn):
    """Load nationality mapping from SQL Server."""
    try:
        cursor = mssql_conn.cursor()
        cursor.execute("SELECT ID, Name FROM Nationality")
        mapping = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.close()
        print(f"🌍 Loaded {len(mapping)} nationalities")
        return mapping
    except Exception as e:
        print(f"⚠️ Could not load nationalities: {e}")
        return {}


def parse_full_name(company, first_nm, last_nm, father_nm):
    """Parse and build full name from various fields."""
    first = str(first_nm).strip() if first_nm and str(first_nm) != 'None' else ""
    last = str(last_nm).strip() if last_nm and str(last_nm) != 'None' else ""
    father = str(father_nm).strip() if father_nm and str(father_nm) != 'None' else ""
    company = str(company).strip() if company and str(company) != 'None' else ""

    # If no first/last but have company, try to parse company
    if not first and not last and company:
        parts = company.split()
        if len(parts) >= 3:
            first, father, last = parts[0], parts[1], " ".join(parts[2:])
        elif len(parts) == 2:
            first, last = parts[0], parts[1]
        elif len(parts) == 1:
            first = parts[0]

    return first or None, last or None, father or None


def normalize_gender(g):
    """Normalize gender values."""
    if not g or str(g) == 'None':
        return None
    g = str(g).strip().lower()
    if g in ["male", "m", "1"]:
        return "Male"
    if g in ["female", "f", "2"]:
        return "Female"
    return None


def clean_phone(phone):
    """Clean and format phone numbers."""
    if not phone or str(phone) == 'None':
        return None
    # Remove common non-digit characters but keep + for international
    cleaned = re.sub(r'[^\d+]', '', str(phone).strip())
    return cleaned if cleaned else None


def safe_date(date_value):
    """Safely convert date values."""
    if not date_value or str(date_value) == 'None':
        return None
    try:
        # If it's already a date object, return it
        if hasattr(date_value, 'date'):
            return date_value.date()
        elif hasattr(date_value, 'year'):
            return date_value
        # Try to parse string
        from dateutil import parser
        return parser.parse(str(date_value)).date()
    except:
        return None


def safe_string(value, max_length=None):
    """Safely convert to string with length limit."""
    if value is None or str(value) == 'None':
        return None
    s = str(value).strip()
    if not s:
        return None
    if max_length and len(s) > max_length:
        s = s[:max_length]
    return s


# ================================================================
# PATIENT MIGRATION
# ================================================================

def migrate_patients(mssql_conn, mysql_conn, nationality_map):
    """Migrate patients with improved error handling and batching."""
    print("\n" + "=" * 60)
    print("PATIENT MIGRATION STARTED")
    print("=" * 60)

    try:
        # Fetch source data
        mssql_cursor = mssql_conn.cursor()
        query = """
            SELECT ID, COMPANY, FIRST_NM, LAST_NM, FATHER_NM, MOTHER, ID_NO, 
                   BDATE, GENDER, MARITALSTATUS, NATIONALITY, PHONE, MOBILE, 
                   EMAIL, ADDR1, ADDR2, CITY, STATE, ZIP, Bloodgroup, allergies
            FROM CUST 
            WHERE ACTIVE = 1
        """

        if Config.TEST_MODE:
            query += " AND ID IN (SELECT TOP 10 ID FROM CUST WHERE ACTIVE = 1)"

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} active patients to migrate")

        if total_records == 0:
            print("⚠️ No records found to migrate")
            return

        # Prepare MySQL
        mysql_cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO patients (
            source_id, first_name, last_name, father_name, mother_name, 
            id_nb, date_of_birth, gender, marital_status, nationality, 
            phone, phone_alt, email, address_line1, address_line2, 
            city, state, zip_code, blood_group, allergies
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            father_name = VALUES(father_name),
            mother_name = VALUES(mother_name),
            id_nb = VALUES(id_nb),
            date_of_birth = VALUES(date_of_birth),
            gender = VALUES(gender),
            marital_status = VALUES(marital_status),
            nationality = VALUES(nationality),
            phone = VALUES(phone),
            phone_alt = VALUES(phone_alt),
            email = VALUES(email),
            address_line1 = VALUES(address_line1),
            address_line2 = VALUES(address_line2),
            city = VALUES(city),
            state = VALUES(state),
            zip_code = VALUES(zip_code),
            blood_group = VALUES(blood_group),
            allergies = VALUES(allergies)
        """

        # Process records
        inserted = 0
        updated = 0
        errors = []
        batch_data = []

        for i, row in enumerate(rows, 1):
            try:
                # Parse name
                first, last, father = parse_full_name(
                    row.COMPANY, row.FIRST_NM, row.LAST_NM, row.FATHER_NM
                )

                # Get nationality
                nationality = None
                if row.NATIONALITY:
                    if isinstance(row.NATIONALITY, (int, float)):
                        nationality = nationality_map.get(int(row.NATIONALITY))
                    else:
                        nationality = safe_string(row.NATIONALITY, 100)

                # Prepare data tuple
                data = (
                    safe_string(row.ID, 50),  # source_id
                    safe_string(first, 50),  # first_name
                    safe_string(last, 50),  # last_name
                    safe_string(father, 100),  # father_name
                    safe_string(row.MOTHER, 100),  # mother_name
                    safe_string(row.ID_NO, 50),  # id_nb
                    safe_date(row.BDATE),  # date_of_birth
                    normalize_gender(row.GENDER),  # gender
                    safe_string(row.MARITALSTATUS, 20),  # marital_status
                    nationality,  # nationality
                    clean_phone(row.PHONE),  # phone
                    clean_phone(row.MOBILE),  # phone_alt
                    safe_string(row.EMAIL, 100),  # email
                    safe_string(row.ADDR1),  # address_line1
                    safe_string(row.ADDR2),  # address_line2
                    safe_string(row.CITY, 50),  # city
                    safe_string(row.STATE, 50),  # state
                    safe_string(row.ZIP, 10),  # zip_code
                    safe_string(row.Bloodgroup, 5),  # blood_group
                    safe_string(row.allergies)  # allergies
                )

                # Execute insert/update
                mysql_cursor.execute(insert_query, data)

                if mysql_cursor.rowcount == 1:
                    inserted += 1
                elif mysql_cursor.rowcount == 2:
                    updated += 1

                # Commit in batches
                if i % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    print(f"  Progress: {i}/{total_records} ({i * 100 // total_records}%) - Inserted: {inserted}, Updated: {updated}")

            except Exception as e:
                error_msg = f"Patient ID {row.ID}: {str(e)}"
                errors.append(error_msg)
                log_error(mysql_conn, 'patients', row.ID, 'INSERT/UPDATE', str(e))

                if Config.DEBUG_MODE:
                    print(f"  ❌ {error_msg}")
                    if len(errors) <= 3:  # Show traceback for first 3 errors
                        traceback.print_exc()

        # Final commit
        mysql_conn.commit()

        # Summary
        print("\n" + "-" * 60)
        print(f"✅ PATIENT MIGRATION COMPLETED")
        print(f"   Total Records: {total_records}")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        # Save error log
        if errors:
            log_path = os.path.join(ensure_logs_folder(), "patient_errors.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Patient Migration Errors - {datetime.now()}\n")
                f.write(f"Total Errors: {len(errors)}\n\n")
                for error in errors:
                    f.write(f"{error}\n")
            print(f"   Error details saved to: {log_path}")

        mssql_cursor.close()
        mysql_cursor.close()

    except Exception as e:
        print(f"❌ Critical error in patient migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


# ================================================================
# DOCTOR MIGRATION
# ================================================================

def parse_doctor_name(name):
    """Parse doctor name from vendor field."""
    if not name:
        return None, None

    # Clean the name
    n = str(name).strip()
    n = re.sub(r"^\d+\s*-\s*", "", n)  # Remove leading numbers
    n = re.sub(r"^(Dr\.?|DR\.?|dr\.?)\s*", "", n, flags=re.IGNORECASE)  # Remove Dr. prefix
    n = re.sub(r"/\d+$", "", n)  # Remove trailing /numbers

    if "+" in n:
        n = n.split("+")[0].strip()

    # Parse into first and last
    parts = n.split()
    if not parts:
        return None, None

    first = parts[0]
    last = " ".join(parts[1:]) if len(parts) > 1 else None

    return first, last


def is_likely_doctor(name):
    """Filter out non-doctor vendor records."""
    if not name:
        return False

    n = str(name).lower()

    # Keywords that indicate non-doctor entities
    exclude_keywords = [
        "company", "co.", "corp", "inc", "ltd", "llc",
        "laboratory", "lab", "labs",
        "pharmacy", "pharma", "medical supplies",
        "clinic", "center", "hospital",
        "equipment", "supplies", "trading",
        "شركة", "مختبر", "صيدلية"  # Arabic terms
    ]

    # Check for exclusion keywords
    for keyword in exclude_keywords:
        if keyword in n:
            return False

    # Positive indicators
    if any(prefix in n for prefix in ["dr.", "dr ", "doctor"]):
        return True

    # If it has a person-like structure (2-4 words), likely a doctor
    parts = name.strip().split()
    if 2 <= len(parts) <= 4:
        return True

    return False


def migrate_doctors(mssql_conn, mysql_conn):
    """Migrate doctors with improved filtering and error handling."""
    print("\n" + "=" * 60)
    print("DOCTOR MIGRATION STARTED")
    print("=" * 60)

    try:
        # Fetch source data
        mssql_cursor = mssql_conn.cursor()
        query = "SELECT VENDSRH, COMPANY, PHONE, CONTACT FROM Vend"

        if Config.TEST_MODE:
            query = "SELECT TOP 10 VENDSRH, COMPANY, PHONE, CONTACT FROM Vend"

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} vendor records")

        if total_records == 0:
            print("⚠️ No records found to migrate")
            return

        # Prepare MySQL
        mysql_cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO doctors (
            source_id, first_name, last_name, phone, phone_alt, license_number
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            phone = VALUES(phone),
            phone_alt = VALUES(phone_alt),
            license_number = VALUES(license_number)
        """

        # Process records
        inserted = 0
        updated = 0
        skipped = []
        errors = []

        for i, row in enumerate(rows, 1):
            sid = safe_string(row.VENDSRH, 50)
            company = safe_string(row.COMPANY)
            phone = clean_phone(row.PHONE)
            contact = clean_phone(row.CONTACT)

            try:
                # Check if likely a doctor
                if not is_likely_doctor(company):
                    skipped.append(f"{sid}: {company} (filtered as non-doctor)")
                    continue

                # Parse name
                first, last = parse_doctor_name(company)

                if not first:
                    skipped.append(f"{sid}: {company} (could not parse name)")
                    continue

                # Prepare data
                data = (
                    sid,  # source_id
                    safe_string(first, 50),  # first_name
                    safe_string(last, 50),  # last_name
                    phone,  # phone
                    contact,  # phone_alt
                    None
                )

                # Execute insert/update
                mysql_cursor.execute(insert_query, data)

                if mysql_cursor.rowcount == 1:
                    inserted += 1
                elif mysql_cursor.rowcount == 2:
                    updated += 1

                # Commit in batches
                if i % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    processed = inserted + updated + len(skipped)
                    print(f"  Progress: {processed}/{total_records} - Inserted: {inserted}, Updated: {updated}, Skipped: {len(skipped)}")

            except Exception as e:
                error_msg = f"Doctor ID {sid}: {str(e)}"
                errors.append(error_msg)
                log_error(mysql_conn, 'doctors', sid, 'INSERT/UPDATE', str(e))

                if Config.DEBUG_MODE:
                    print(f"  ❌ {error_msg}")

        # Final commit
        mysql_conn.commit()

        # Summary
        print("\n" + "-" * 60)
        print(f"✅ DOCTOR MIGRATION COMPLETED")
        print(f"   Total Records: {total_records}")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Skipped: {len(skipped)}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        # Save logs
        log_dir = ensure_logs_folder()

        if skipped:
            log_path = os.path.join(log_dir, "doctors_skipped.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Skipped Doctors - {datetime.now()}\n")
                f.write(f"Total Skipped: {len(skipped)}\n\n")
                for skip in skipped:
                    f.write(f"{skip}\n")
            print(f"   Skipped records saved to: {log_path}")

        if errors:
            log_path = os.path.join(log_dir, "doctors_errors.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Doctor Migration Errors - {datetime.now()}\n")
                f.write(f"Total Errors: {len(errors)}\n\n")
                for error in errors:
                    f.write(f"{error}\n")
            print(f"   Error details saved to: {log_path}")

        mssql_cursor.close()
        mysql_cursor.close()

    except Exception as e:
        print(f"❌ Critical error in doctor migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


# ================================================================
# VERIFICATION
# ================================================================

def verify_migration(mysql_conn):
    """Verify migration results."""
    print("\n" + "=" * 60)
    print("MIGRATION VERIFICATION")
    print("=" * 60)

    try:
        cursor = mysql_conn.cursor()

        # Check patients
        cursor.execute("SELECT COUNT(*) FROM patients")
        patient_count = cursor.fetchone()[0]
        print(f"✅ Patients in database: {patient_count}")

        # Check doctors
        cursor.execute("SELECT COUNT(*) FROM doctors")
        doctor_count = cursor.fetchone()[0]
        print(f"✅ Doctors in database: {doctor_count}")

        # Check migration log
        cursor.execute("SELECT status, COUNT(*) FROM migration_log GROUP BY status")
        log_stats = cursor.fetchall()
        if log_stats:
            print("\n📊 Migration Log Summary:")
            for status, count in log_stats:
                print(f"   {status}: {count}")

        # Sample data
        print("\n📋 Sample Patient Records:")
        cursor.execute("SELECT source_id, first_name, last_name FROM patients LIMIT 5")
        for row in cursor.fetchall():
            print(f"   ID: {row[0]}, Name: {row[1]} {row[2]}")

        print("\n📋 Sample Doctor Records:")
        cursor.execute("SELECT source_id, first_name, last_name FROM doctors LIMIT 5")
        for row in cursor.fetchall():
            print(f"   ID: {row[0]}, Name: {row[1]} {row[2]}")

        cursor.close()

    except Exception as e:
        print(f"❌ Error during verification: {e}")


# ================================================================
# MAIN
# ================================================================

def main():
    """Main migration process with enhanced error handling."""
    print("\n" + "=" * 60)
    print("🚀 TOOTHPICK EVE DATA MIGRATION TOOL")
    print("=" * 60)
    print(f"Mode: {'TEST' if Config.TEST_MODE else 'PRODUCTION'}")
    print(f"Debug: {'ON' if Config.DEBUG_MODE else 'OFF'}")
    print(f"Batch Size: {Config.BATCH_SIZE}")
    print("=" * 60)

    # Establish connections
    print("\n📡 Establishing Database Connections...")

    mssql = create_mssql_connection(
        Config.MSSQL_SERVER,
        Config.MSSQL_DATABASE,
        Config.USE_WINDOWS_AUTH,
        Config.MSSQL_USERNAME,
        Config.MSSQL_PASSWORD
    )

    mysql = create_mysql_connection(
        Config.MYSQL_HOST,
        Config.MYSQL_USER,
        Config.MYSQL_PASSWORD,
        Config.MYSQL_DATABASE
    )

    if not mssql or not mysql:
        print("\n❌ Migration aborted: Could not establish database connections")
        return 1

    try:
        # Create tables if needed
        if Config.CREATE_TABLES:
            print("\n🔧 Verifying/Creating Tables...")
            create_tables_if_missing(mysql)

        # Load reference data
        print("\n📚 Loading Reference Data...")
        nationality_map = load_nationality_mapping(mssql)

        # Run migrations
        migrate_patients(mssql, mysql, nationality_map)
        migrate_doctors(mssql, mysql)

        # Verify results
        verify_migration(mysql)

        print("\n" + "=" * 60)
        print("✅ MIGRATION COMPLETED SUCCESSFULLY")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ Migration failed with error: {e}")
        traceback.print_exc()
        return 1

    finally:
        # Clean up connections
        print("\n🔒 Closing Database Connections...")
        if mssql:
            mssql.close()
            print("   SQL Server connection closed")
        if mysql and mysql.is_connected():
            mysql.close()
            print("   MySQL connection closed")


if __name__ == "__main__":
    exit_code = main()
    input("\nPress Enter to exit...")
    exit(exit_code)