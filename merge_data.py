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
    TEST_MODE = False
    MIGRATE_APPOINTMENTS_FROM = "1900-01-01"


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
    required_tables = ["patients", "doctors", "appointments"]
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
    if not os.path.exists("logs"):
        os.makedirs("logs")
    return "logs"


def log_error(mysql_conn, table_name, source_id, operation, error_message):
    try:
        cursor = mysql_conn.cursor()
        query = "INSERT INTO migration_log (table_name, source_id, operation, status, error_message) VALUES (%s, %s, %s, 'ERROR', %s)"
        cursor.execute(query, (table_name, str(source_id), operation, str(error_message)[:1000]))
        mysql_conn.commit()
        cursor.close()
    except Exception:
        pass


def load_nationality_mapping(mssql_conn):
    try:
        cursor = mssql_conn.cursor()
        cursor.execute("SELECT ID, Name FROM Nationality")
        mapping = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"🌍 Loaded {len(mapping)} nationalities")
        cursor.close()
        return mapping
    except Exception as e:
        print(f"⚠️ Could not load nationalities: {e}")
        return {}


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
    if not g or str(g) == 'None':
        return None
    g = str(g).strip().lower()
    if g in ["male", "m", "1"]:
        return "male"
    if g in ["female", "f", "2"]:
        return "female"
    return None


def clean_phone(phone):
    if not phone or str(phone) == 'None':
        return None
    return re.sub(r'[^\d+]', '', str(phone).strip()) or None


def safe_datetime(dt_value):
    if not dt_value or str(dt_value) == 'None':
        return None
    try:
        return dt_value if isinstance(dt_value, datetime) else __import__('dateutil.parser').parser.parse(str(dt_value))
    except (ValueError, TypeError):
        return None


def safe_date(date_value):
    dt = safe_datetime(date_value)
    return dt.date() if dt else None


def safe_string(value, max_length=None):
    if value is None or str(value) == 'None':
        return None
    s = str(value).strip()
    if not s:
        return None
    if max_length and len(s) > max_length:
        s = s[:max_length]
    return s


def safe_time(time_str):
    """Safely convert time string to a time object."""
    if not time_str or not isinstance(time_str, str):
        return None
    cleaned_time = time_str.strip()
    try:
        t = datetime.strptime(cleaned_time, '%H:%M').time()
        return t
    except ValueError:
        return None


def is_likely_doctor(company_name):
    """Check if vendor name suggests it's NOT a doctor/person (i.e., a company/lab)."""
    if not company_name:
        return False

    company_lower = company_name.lower()

    # Exclude obvious companies/labs/suppliers
    exclude_keywords = [
        'company for dental', 'dental products', 'company for dent',
        'dental care', 'dental store', 'dental supplies',
        'laboratory', 'lab ', 'مختبر',  # Arabic for laboratory
        's.a.l', 'sal', 'sarl', 'co.', 'corp', 'inc.', 'ltd', 'llc',
        'trading', 'group', 'gases', 'droguerie', 'store',
        'medical group', 'marketing', 'liquid', 'sanita'
    ]

    # If it contains obvious company indicators, skip it
    if any(keyword in company_lower for keyword in exclude_keywords):
        return False

    # Skip entries that look like just clinic numbers
    if company_name.strip().lower() in ['clinic', 'center']:
        return False

    # Otherwise, assume it's a doctor/person
    return True


def parse_doctor_name(company):
    """
    Extract first and last name from doctor company name.
    Handles various formats including:
    - Numbers with dashes: "8-MOHAMED EL BIZRI", "3- Dr Lena Makary"
    - Doctor prefixes: "Dr", "DR", "Dr.", "Doctor"
    - Slashes with numbers: "Dr Joseph/15", "Michel Al-Haddad/1"
    - Single letter prefixes: "D Bernard Kikano"
    - Combined formats: "9-Dr.Mohamad+Zeina", "12-dr khaled hajjar"

    Args:
        company (str): The company/vendor name from the database

    Returns:
        tuple: (first_name, last_name) or (None, None) if parsing fails
    """
    if not company:
        return None, None

    # Clean the input
    name = company.strip()

    # Step 1: Remove leading numbers with optional dash/dot (e.g., "8-", "910- ", "3.")
    name = re.sub(r'^\d+[-.]?\s*', '', name)

    # Step 2: Remove standalone single letter prefixes like "D " (but not "Dr")
    name = re.sub(r'^D\s+(?!r)', '', name, flags=re.IGNORECASE)

    # Step 3: Remove doctor titles and prefixes (case insensitive)
    # Matches: Dr, Dr., DR, Doctor, DDS, DMD, PhD, MD, DVM
    name = re.sub(r'\b(dr|doctor|dds|dmd|phd|md|dvm)\.?\s*', '', name, flags=re.IGNORECASE)

    # Step 4: Remove trailing slashes with numbers (e.g., "/15", "/1")
    name = re.sub(r'/\d+$', '', name)

    # Step 5: Handle special separators like "+" (e.g., "Mohamad+Zeina")
    # Replace with space for proper splitting
    name = name.replace('+', ' ')

    # Step 6: Remove extra whitespace
    name = ' '.join(name.split())

    # Check if we have a valid name left
    if not name:
        return None, None

    # Step 7: Split into parts
    parts = name.split()

    if len(parts) >= 2:
        # First name is first part, last name is everything else
        first_name = parts[0]
        last_name = ' '.join(parts[1:])
        return first_name, last_name
    elif len(parts) == 1:
        # Only one name part - return as first name
        return parts[0], None

    return None, None


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
            query = """
                SELECT TOP 100 ID, COMPANY, FIRST_NM, LAST_NM, FATHER_NM, MOTHER, ID_NO, 
                       BDATE, GENDER, MARITALSTATUS, NATIONALITY, PHONE, MOBILE, 
                       EMAIL, ADDR1, ADDR2, CITY, STATE, ZIP, Bloodgroup, allergies,
                       DATEADDED, Lastupdate
                FROM CUST 
                WHERE ACTIVE = 1
            """

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} active patients to migrate")

        if total_records == 0:
            print("⚠️ No records found to migrate")
            return

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

                nationality = None
                if row.NATIONALITY:
                    if isinstance(row.NATIONALITY, (int, float)):
                        nationality = nationality_map.get(int(row.NATIONALITY))
                    else:
                        nationality = safe_string(row.NATIONALITY, 100)

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
                if Config.DEBUG_MODE and len(errors) <= 3:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ PATIENT MIGRATION COMPLETED")
        print(f"   Total Records: {total_records}")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

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
def migrate_doctors(mssql_conn, mysql_conn):
    """Migrate doctors with improved filtering and error handling."""
    print("\n" + "=" * 60)
    print("DOCTOR MIGRATION STARTED")
    print("=" * 60)

    try:
        mssql_cursor = mssql_conn.cursor()
        query = "SELECT VENDSRH, COMPANY, PHONE, CONTACT FROM Vend WHERE TYPE = 2"

        if Config.TEST_MODE:
            query = "SELECT TOP 50 VENDSRH, COMPANY, PHONE, CONTACT FROM Vend WHERE TYPE = 2"

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        total_records = len(rows)
        print(f"📊 Found {total_records} doctors (TYPE=2) to migrate")

        if total_records == 0:
            print("⚠️ No records found to migrate")
            return

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
                    processed = inserted + updated + len(skipped)
                    print(f"  Progress: {processed}/{total_records} - Inserted: {inserted}, Updated: {updated}, Skipped: {len(skipped)}")

            except Exception as e:
                error_msg = f"Doctor ID {sid}: {str(e)}"
                errors.append(error_msg)
                log_error(mysql_conn, 'doctors', sid, 'INSERT/UPDATE', str(e))
                if Config.DEBUG_MODE and len(errors) <= 3:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ DOCTOR MIGRATION COMPLETED")
        print(f"   Total Records: {total_records}")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Skipped: {len(skipped)}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        log_dir = ensure_logs_folder()

        if skipped:
            with open(os.path.join(log_dir, "doctors_skipped.log"), "w", encoding="utf-8") as f:
                f.write(f"Skipped Doctors - {datetime.now()}\n\n")
                f.write("\n".join(skipped))
            print(f"   Skipped records log saved.")

        if errors:
            with open(os.path.join(log_dir, "doctors_errors.log"), "w", encoding="utf-8") as f:
                f.write(f"Doctor Migration Errors - {datetime.now()}\n\n")
                f.write("\n".join(errors))
            print(f"   Error details log saved.")

        mssql_cursor.close()
        mysql_cursor.close()

    except Exception as e:
        print(f"❌ Critical error in doctor migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


# ================================================================
# APPOINTMENT MIGRATION
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
        if total_records == 0:
            return

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
                duration_str = f"{(row.period or 15)} minutes"
                reason = safe_string(row.comment) or safe_string(row.pat_name)

                data = (
                    row.id, row.pat_id, row.doc_id,
                    safe_date(row.date), safe_time(row.time),
                    duration_str, safe_string(row.room, 50),
                    row.status, bool(row.missed), reason,
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
                if Config.DEBUG_MODE:
                    print(f"  ❌ Error for appointment ID {row.id}: {e}")

        mysql_conn.commit()
        print("\n" + "-" * 60 + f"\n✅ APPOINTMENT MIGRATION COMPLETED\n   Total: {total_records}, Inserted: {inserted}, Updated: {updated}, Errors: {errors}\n" + "-" * 60)

    except Exception as e:
        print(f"❌ Critical error in appointment migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()
    finally:
        if 'mssql_cursor' in locals():
            mssql_cursor.close()
        if 'mysql_cursor' in locals():
            mysql_cursor.close()


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
            for status, count in log_stats:
                print(f"   {status}: {count}")
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

        nationality_map = load_nationality_mapping(mssql)
        # migrate_patients(mssql, mysql, nationality_map)
        migrate_doctors(mssql, mysql)
        # migrate_appointments(mssql, mysql)

        verify_migration(mysql)

        print("\n" + "=" * 60 + "\n✅ MIGRATION COMPLETED SUCCESSFULLY\n" + "=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ MIGRATION FAILED WITH A CRITICAL ERROR: {e}")
        traceback.print_exc()
        return 1

    finally:
        print("\n🔒 Closing Database Connections...")
        if mssql:
            mssql.close()
            print("   SQL Server connection closed.")
        if mysql and mysql.is_connected():
            mysql.close()
            print("   MySQL connection closed.")


if __name__ == "__main__":
    exit_code = main()
    input("\nPress Enter to exit...")
    exit(exit_code)