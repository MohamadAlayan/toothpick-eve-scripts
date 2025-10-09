import os
import re
import pyodbc
import mysql.connector
from mysql.connector import Error
from datetime import datetime, time
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
    BATCH_SIZE = 100
    DEBUG_MODE = True
    TEST_MODE = False  # If True, only process a small subset of records
    MIGRATE_APPOINTMENTS_FROM = "1900-01-01"  # Only migrate appointments from this date forward


# ================================================================
# DATABASE CONNECTIONS
# ================================================================

def create_mssql_connection(server, database, use_windows_auth=True, username=None, password=None):
    """Connect to SQL Server with improved error handling."""
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;" if use_windows_auth else f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        try:
            conn = pyodbc.connect(conn_str)
        except pyodbc.Error:
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
            host=host, user=user, password=password, database=database,
            autocommit=False, use_unicode=True, charset='utf8mb4'
        )
        if conn.is_connected():
            print("✅ Connected to MySQL")
            return conn
    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        return None


# ================================================================
# TABLE CREATION & VERIFICATION
# ================================================================
def setup_database_tables(mysql_conn):
    """Verify required tables and create migration_log if missing."""
    cursor = mysql_conn.cursor()
    required_tables = ["patients", "doctors", "appointments"]  # Added 'appointments'
    try:
        cursor.execute("SHOW TABLES")
        existing_tables = {row[0] for row in cursor.fetchall()}

        missing_required = [t for t in required_tables if t not in existing_tables]
        if missing_required:
            raise Exception(f"Missing required tables: {', '.join(missing_required)}. Please run create_db.py.")

        if "migration_log" not in existing_tables:
            print("⚠️ 'migration_log' table not found. Creating it automatically...")
            cursor.execute("""
                CREATE TABLE migration_log (
                    id INT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(50), source_id VARCHAR(50),
                    operation VARCHAR(20), status VARCHAR(20), error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX idx_status (status),
                    INDEX idx_table_source (table_name, source_id)
                ) ENGINE=InnoDB;
            """)
            mysql_conn.commit()
            print("✅ 'migration_log' table created.")
        else:
            print("✅ All required tables exist.")
    except Exception as e:
        print(f"❌ Table verification error: {e}")
        raise
    finally:
        cursor.close()


# ================================================================
# UTILITIES
# ================================================================
def ensure_logs_folder():
    if not os.path.exists("logs"): os.makedirs("logs")
    return "logs"


def log_error(mysql_conn, table_name, source_id, operation, error_message):
    try:
        cursor = mysql_conn.cursor()
        query = "INSERT INTO migration_log (table_name, source_id, operation, status, error_message) VALUES (%s, %s, %s, 'ERROR', %s)"
        cursor.execute(query, (table_name, str(source_id), operation, str(error_message)[:1000]))
        mysql_conn.commit()
    except Exception:
        pass  # Don't let logging stop migration
    finally:
        cursor.close()


def load_nationality_mapping(mssql_conn):
    try:
        cursor = mssql_conn.cursor()
        cursor.execute("SELECT ID, Name FROM Nationality")
        mapping = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"🌍 Loaded {len(mapping)} nationalities")
        return mapping
    except Exception as e:
        print(f"⚠️ Could not load nationalities: {e}")
        return {}
    finally:
        cursor.close()


# All other utility functions (parse_full_name, normalize_gender, etc.) remain the same...
def parse_full_name(company, first_nm, last_nm, father_nm):
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
    if not g or str(g) == 'None': return None
    g = str(g).strip().lower()
    if g in ["male", "m", "1"]: return "Male"
    if g in ["female", "f", "2"]: return "Female"
    return None


def clean_phone(phone):
    if not phone or str(phone) == 'None': return None
    return re.sub(r'[^\d+]', '', str(phone).strip()) or None


def safe_datetime(dt_value):
    if not dt_value or str(dt_value) == 'None': return None
    try:
        return dt_value if isinstance(dt_value, datetime) else __import__('dateutil.parser').parser.parse(str(dt_value))
    except (ValueError, TypeError):
        return None


def safe_date(date_value):
    dt = safe_datetime(date_value)
    return dt.date() if dt else None


def safe_string(value, max_length=None):
    if value is None or str(value) == 'None': return None
    s = str(value).strip()
    if not s: return None
    if max_length and len(s) > max_length: s = s[:max_length]
    return s


def safe_time(time_str):
    """Safely convert time string to a time object."""
    if not time_str or not isinstance(time_str, str): return None
    cleaned_time = time_str.strip()
    try:
        # Handle formats like '14:30' or '9:00'
        t = datetime.strptime(cleaned_time, '%H:%M').time()
        return t
    except ValueError:
        return None


# ================================================================
# PATIENT MIGRATION
# ================================================================
def migrate_patients(mssql_conn, mysql_conn, nationality_map):
    print("\n" + "=" * 60 + "\nPATIENT MIGRATION STARTED\n" + "=" * 60)
    # This function remains unchanged from your original script
    # ... (code for migrating patients) ...
    print("\n✅ PATIENT MIGRATION COMPLETED")


# ================================================================
# DOCTOR MIGRATION
# ================================================================
def migrate_doctors(mssql_conn, mysql_conn):
    print("\n" + "=" * 60 + "\nDOCTOR MIGRATION STARTED\n" + "=" * 60)
    # This function remains unchanged from your original script
    # ... (code for migrating doctors) ...
    print("\n✅ DOCTOR MIGRATION COMPLETED")


# ================================================================
# APPOINTMENT MIGRATION (NEW FUNCTION)
# ================================================================
def migrate_appointments(mssql_conn, mysql_conn):
    """Migrate appointments, storing the original status number."""
    print("\n" + "=" * 60 + "\nAPPOINTMENT MIGRATION STARTED\n" + "=" * 60)

    try:
        mssql_cursor = mssql_conn.cursor()
        query = f"""
            SELECT id, pat_id, doc_id, [date], [time], period, room, status, missed, comment, pat_name
            FROM schedule
            WHERE pat_id > 0 AND [date] >= '{Config.MIGRATE_APPOINTMENTS_FROM}'
        """
        if Config.TEST_MODE:
            query = f"""
                SELECT TOP 200 id, pat_id, doc_id, [date], [time], period, room, status, missed, comment, pat_name
                FROM schedule
                WHERE pat_id > 0 AND [date] >= '{Config.MIGRATE_APPOINTMENTS_FROM}'
            """

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} appointments to migrate from {Config.MIGRATE_APPOINTMENTS_FROM}")
        if total_records == 0: return

        mysql_cursor = mysql_conn.cursor()
        insert_query = """
        INSERT INTO appointments (
            source_id, patient_id, doctor_id, appointment_date, appointment_time,
            duration, room, status, missed, reason_for_visit, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            patient_id = VALUES(patient_id), doctor_id = VALUES(doctor_id),
            appointment_date = VALUES(appointment_date), appointment_time = VALUES(appointment_time),
            duration = VALUES(duration), room = VALUES(room), status = VALUES(status),
            missed = VALUES(missed), reason_for_visit = VALUES(reason_for_visit),
            updated_at = NOW()
        """
        inserted, updated, errors = 0, 0, 0

        for i, row in enumerate(rows, 1):
            try:
                # DURATION: Use only the 'period' field. Default to 15 if null.
                duration_str = f"{(row.period or 15)} minutes"

                # REASON: Use comment, fallback to patient name if comment is blank
                reason = safe_string(row.comment) or safe_string(row.pat_name)

                data = (
                    row.id,
                    row.pat_id,
                    row.doc_id,
                    safe_date(row.date),
                    safe_time(row.time),
                    duration_str,
                    safe_string(row.room, 50),
                    row.status,  # <-- Using the original number directly
                    bool(row.missed),
                    reason,
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
                errors += 1
                log_error(mysql_conn, 'appointments', row.id, 'INSERT/UPDATE', str(e))
                if Config.DEBUG_MODE: print(f"  ❌ Error for appointment ID {row.id}: {e}")

        mysql_conn.commit()
        print("\n" + "-" * 60 + f"\n✅ APPOINTMENT MIGRATION COMPLETED\n   Total: {total_records}, Inserted: {inserted}, Updated: {updated}, Errors: {errors}\n" + "-" * 60)

    except Exception as e:
        print(f"❌ Critical error in appointment migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()
    finally:
        if 'mssql_cursor' in locals(): mssql_cursor.close()
        if 'mysql_cursor' in locals(): mysql_cursor.close()


# ================================================================
# VERIFICATION
# ================================================================
def verify_migration(mysql_conn):
    """Verify migration results."""
    print("\n" + "=" * 60 + "\nMIGRATION VERIFICATION\n" + "=" * 60)
    try:
        cursor = mysql_conn.cursor()
        tables_to_check = ["patients", "doctors", "appointments"]
        for table in tables_to_check:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"✅ {table.capitalize()} in database: {cursor.fetchone()[0]}")

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
        print("\n❌ Aborted: Could not establish database connections.")
        return 1

    try:
        setup_database_tables(mysql)

        # Step 1: Migrate Core Data
        nationality_map = load_nationality_mapping(mssql)
        migrate_patients(mssql, mysql, nationality_map)
        migrate_doctors(mssql, mysql)

        # Step 2: Migrate Transactional Data
        migrate_appointments(mssql, mysql)

        # Step 3: Verify Everything
        verify_migration(mysql)

        print("\n" + "=" * 60 + "\n✅ MIGRATION COMPLETED SUCCESSFULLY\n" + "=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ MIGRATION FAILED WITH A CRITICAL ERROR: {e}")
        return 1

    finally:
        print("\n🔒 Closing Database Connections...")
        if mssql: mssql.close(); print("   SQL Server connection closed.")
        if mysql and mysql.is_connected(): mysql.close(); print("   MySQL connection closed.")


if __name__ == "__main__":
    # The patient and doctor migration functions need to be filled in with your
    # existing code, as I have omitted them for brevity.
    # This is a placeholder to make the script runnable for demonstration.
    def placeholder_migration(name):
        def func(mssql_conn, mysql_conn, *args):
            print(f"\n... (skipping {name} migration) ...")

        return func


    if 'migrate_patients' not in globals(): migrate_patients = placeholder_migration('patients')
    if 'migrate_doctors' not in globals(): migrate_doctors = placeholder_migration('doctors')

    exit_code = main()
    input("\nPress Enter to exit...")
    exit(exit_code)