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

def setup_database_tables(mysql_conn):
    """
    Verify required tables.
    - Ensure 'patients' and 'doctors' exist (stop if missing)
    - Create 'migration_log' automatically if it's missing
    """
    cursor = mysql_conn.cursor()
    required_existing = ["patients", "doctors"]
    log_table = "migration_log"

    try:
        # Get all existing tables
        cursor.execute("SHOW TABLES")
        existing = {row[0] for row in cursor.fetchall()}

        # Check required ones first
        missing_required = [t for t in required_existing if t not in existing]
        if missing_required:
            raise Exception(f"Missing required tables: {', '.join(missing_required)}. Please create them before running migration.")

        # Create migration_log if not exists
        if log_table not in existing:
            print("⚠️ 'migration_log' table not found. Creating it automatically...")
            cursor.execute("""
                CREATE TABLE migration_log (
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
            """)
            mysql_conn.commit()
            print("✅ 'migration_log' table created successfully.")
        else:
            print("✅ All required tables exist. Proceeding with migration...")

    except Exception as e:
        print(f"❌ Table verification error: {e}")
        raise  # Stop script execution
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
    if not g or str(g) == 'None': return None
    g = str(g).strip().lower()
    if g in ["male", "m", "1"]: return "Male"
    if g in ["female", "f", "2"]: return "Female"
    return None


def clean_phone(phone):
    """Clean and format phone numbers."""
    if not phone or str(phone) == 'None': return None
    cleaned = re.sub(r'[^\d+]', '', str(phone).strip())
    return cleaned if cleaned else None


def safe_datetime(dt_value):
    """Safely convert to a datetime object, handling various input types."""
    if not dt_value or str(dt_value) == 'None':
        return None
    try:
        if isinstance(dt_value, datetime):
            return dt_value
        from dateutil import parser
        return parser.parse(str(dt_value))
    except (ValueError, TypeError):
        return None


def safe_date(date_value):
    """Safely convert date values."""
    dt = safe_datetime(date_value)
    return dt.date() if dt else None


def safe_string(value, max_length=None):
    """Safely convert to string with length limit."""
    if value is None or str(value) == 'None': return None
    s = str(value).strip()
    if not s: return None
    if max_length and len(s) > max_length: s = s[:max_length]
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
        mssql_cursor = mssql_conn.cursor()
        query = """
            SELECT ID, COMPANY, FIRST_NM, LAST_NM, FATHER_NM, MOTHER, ID_NO, 
                   BDATE, GENDER, MARITALSTATUS, NATIONALITY, PHONE, MOBILE, 
                   EMAIL, ADDR1, ADDR2, CITY, STATE, ZIP, Bloodgroup, allergies,
                   DATEADDED, Lastupdate
            FROM CUST 
            WHERE ACTIVE = 1
        """
        if Config.TEST_MODE:
            query += " AND ID IN (SELECT TOP 10 ID FROM CUST WHERE ACTIVE = 1)"

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} active patients to migrate")
        if total_records == 0: return

        mysql_cursor = mysql_conn.cursor()
        insert_query = """
        INSERT INTO patients (
            source_id, first_name, last_name, father_name, mother_name, 
            id_nb, date_of_birth, gender, marital_status, nationality, 
            phone, phone_alt, email, address_line1, address_line2, 
            city, state, zip_code, blood_group, allergies, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name), last_name = VALUES(last_name),
            father_name = VALUES(father_name), mother_name = VALUES(mother_name),
            id_nb = VALUES(id_nb), date_of_birth = VALUES(date_of_birth),
            gender = VALUES(gender), marital_status = VALUES(marital_status),
            nationality = VALUES(nationality), phone = VALUES(phone),
            phone_alt = VALUES(phone_alt), email = VALUES(email),
            address_line1 = VALUES(address_line1), address_line2 = VALUES(address_line2),
            city = VALUES(city), state = VALUES(state), zip_code = VALUES(zip_code),
            blood_group = VALUES(blood_group), allergies = VALUES(allergies),
            updated_at = VALUES(updated_at)
        """
        inserted, updated, errors = 0, 0, []

        for i, row in enumerate(rows, 1):
            try:
                first, last, father = parse_full_name(row.COMPANY, row.FIRST_NM, row.LAST_NM, row.FATHER_NM)
                nationality = nationality_map.get(int(row.NATIONALITY)) if isinstance(row.NATIONALITY, (int, float)) else safe_string(row.NATIONALITY, 100)

                data = (
                    safe_string(row.ID, 50), safe_string(first, 50), safe_string(last, 50),
                    safe_string(father, 100), safe_string(row.MOTHER, 100), safe_string(row.ID_NO, 50),
                    safe_date(row.BDATE), normalize_gender(row.GENDER), safe_string(row.MARITALSTATUS, 20),
                    nationality, clean_phone(row.PHONE), clean_phone(row.MOBILE),
                    safe_string(row.EMAIL, 100), safe_string(row.ADDR1), safe_string(row.ADDR2),
                    safe_string(row.CITY, 50), safe_string(row.STATE, 50), safe_string(row.ZIP, 10),
                    safe_string(row.Bloodgroup, 5), safe_string(row.allergies),
                    safe_datetime(row.DATEADDED), safe_datetime(row.Lastupdate)
                )
                mysql_cursor.execute(insert_query, data)
                if mysql_cursor.rowcount == 1:
                    inserted += 1
                elif mysql_cursor.rowcount == 2:
                    updated += 1

                if i % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    print(f"  Progress: {i}/{total_records} ({i * 100 // total_records}%) - Inserted: {inserted}, Updated: {updated}")
            except Exception as e:
                error_msg = f"Patient ID {row.ID}: {str(e)}"
                errors.append(error_msg)
                log_error(mysql_conn, 'patients', row.ID, 'INSERT/UPDATE', str(e))
                if Config.DEBUG_MODE: print(f"  ❌ {error_msg}")

        mysql_conn.commit()
        print("\n" + "-" * 60 + f"\n✅ PATIENT MIGRATION COMPLETED\n   Total: {total_records}, Inserted: {inserted}, Updated: {updated}, Errors: {len(errors)}\n" + "-" * 60)

        if errors:
            log_path = os.path.join(ensure_logs_folder(), "patient_errors.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(errors))
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
    if not name: return None, None
    n = str(name).strip()
    n = re.sub(r"^\d+\s*-\s*", "", n)
    n = re.sub(r"^(Dr\.?|DR\.?|dr\.?)\s*", "", n, flags=re.IGNORECASE)
    if "+" in n: n = n.split("+")[0].strip()
    parts = n.split()
    if not parts: return None, None
    return parts[0], " ".join(parts[1:]) if len(parts) > 1 else None


def is_likely_doctor(name):
    """Filter out non-doctor vendor records."""
    if not name: return False
    n = str(name).lower()
    exclude = ["company", "lab", "pharmacy", "clinic", "center", "hospital", "equipment", "supplies", "trading", "شركة", "مختبر", "صيدلية"]
    if any(keyword in n for keyword in exclude): return False
    return True


def migrate_doctors(mssql_conn, mysql_conn):
    """Migrate doctors with improved filtering and error handling."""
    print("\n" + "=" * 60 + "\nDOCTOR MIGRATION STARTED\n" + "=" * 60)
    try:
        mssql_cursor = mssql_conn.cursor()
        query = "SELECT VENDSRH, COMPANY, PHONE, CONTACT FROM Vend"
        if Config.TEST_MODE: query = "SELECT TOP 10 VENDSRH, COMPANY, PHONE, CONTACT FROM Vend"
        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} vendor records to process")
        if total_records == 0: return

        mysql_cursor = mysql_conn.cursor()
        insert_query = """
        INSERT INTO doctors (source_id, first_name, last_name, phone, phone_alt) 
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name), last_name = VALUES(last_name),
            phone = VALUES(phone), phone_alt = VALUES(phone_alt)
        """
        inserted, updated, skipped, errors = 0, 0, [], []

        for i, row in enumerate(rows, 1):
            sid = safe_string(row.VENDSRH, 50)
            company = safe_string(row.COMPANY)
            try:
                if not is_likely_doctor(company):
                    skipped.append(f"{sid}: {company} (filtered)")
                    continue
                first, last = parse_doctor_name(company)
                if not first:
                    skipped.append(f"{sid}: {company} (no name parsed)")
                    continue

                data = (sid, safe_string(first, 50), safe_string(last, 50), clean_phone(row.PHONE), clean_phone(row.CONTACT))
                mysql_cursor.execute(insert_query, data)

                if mysql_cursor.rowcount == 1:
                    inserted += 1
                elif mysql_cursor.rowcount == 2:
                    updated += 1

                if i % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
            except Exception as e:
                errors.append(f"Doctor ID {sid}: {str(e)}")
                log_error(mysql_conn, 'doctors', sid, 'INSERT/UPDATE', str(e))

        mysql_conn.commit()
        print(
            "\n" + "-" * 60 + f"\n✅ DOCTOR MIGRATION COMPLETED\n   Total: {total_records}, Inserted: {inserted}, Updated: {updated}, Skipped: {len(skipped)}, Errors: {len(errors)}\n" + "-" * 60)

        log_dir = ensure_logs_folder()
        if skipped:
            with open(os.path.join(log_dir, "doctors_skipped.log"), "w", encoding="utf-8") as f: f.write("\n".join(skipped))
            print(f"   Skipped records log saved.")
        if errors:
            with open(os.path.join(log_dir, "doctors_errors.log"), "w", encoding="utf-8") as f: f.write("\n".join(errors))
            print(f"   Error details log saved.")

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
    print("\n" + "=" * 60 + "\nMIGRATION VERIFICATION\n" + "=" * 60)
    try:
        cursor = mysql_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM patients");
        print(f"✅ Patients in database: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM doctors");
        print(f"✅ Doctors in database: {cursor.fetchone()[0]}")
        cursor.execute("SELECT status, COUNT(*) FROM migration_log GROUP BY status")
        log_stats = cursor.fetchall()
        if log_stats:
            print("\n📊 Migration Log Summary:")
            for status, count in log_stats: print(f"   {status}: {count}")
        cursor.close()
    except Exception as e:
        print(f"❌ Error during verification: {e}")


# ================================================================
# MAIN
# ================================================================

def main():
    """Main migration process."""
    print("\n" + "=" * 60 + "\n🚀 TOOTHPICK EVE DATA MIGRATION TOOL\n" + "=" * 60)
    print(f"Mode: {'TEST' if Config.TEST_MODE else 'PRODUCTION'}, Debug: {'ON' if Config.DEBUG_MODE else 'OFF'}, Batch Size: {Config.BATCH_SIZE}")
    print("=" * 60)

    mssql = create_mssql_connection(Config.MSSQL_SERVER, Config.MSSQL_DATABASE, Config.USE_WINDOWS_AUTH, Config.MSSQL_USERNAME, Config.MSSQL_PASSWORD)
    mysql = create_mysql_connection(Config.MYSQL_HOST, Config.MYSQL_USER, Config.MYSQL_PASSWORD, Config.MYSQL_DATABASE)

    if not mssql or not mysql:
        print("\n❌ Aborted: Could not establish database connections")
        return 1

    try:
        setup_database_tables(mysql)

        nationality_map = load_nationality_mapping(mssql)
        migrate_patients(mssql, mysql, nationality_map)
        migrate_doctors(mssql, mysql)
        verify_migration(mysql)

        print("\n" + "=" * 60 + "\n✅ MIGRATION COMPLETED SUCCESSFULLY\n" + "=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ MIGRATION FAILED WITH A CRITICAL ERROR: {e}")
        return 1

    finally:
        print("\n🔒 Closing Database Connections...")
        if mssql: mssql.close(); print("   SQL Server connection closed")
        if mysql and mysql.is_connected(): mysql.close(); print("   MySQL connection closed")


if __name__ == "__main__":
    exit_code = main()
    input("\nPress Enter to exit...")
    exit(exit_code)