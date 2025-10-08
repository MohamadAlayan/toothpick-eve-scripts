import pyodbc
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import re


# =====================================================================
# DATABASE CONNECTION FUNCTIONS (No changes needed here)
# =====================================================================

def create_mssql_connection(server, database, use_windows_auth=True, username=None, password=None):
    """Create a connection to Microsoft SQL Server."""
    try:
        if use_windows_auth:
            conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        else:
            conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

        connection = pyodbc.connect(conn_str)
        print("✅ Successfully connected to SQL Server")
        return connection
    except pyodbc.Error as e:
        print(f"❌ Error connecting to SQL Server: {e}")
        return None


def create_mysql_connection(host, user, password, database):
    """Create a connection to a MySQL server."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("✅ Successfully connected to MySQL")
            return connection
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


# =====================================================================
# PATIENT MIGRATION FUNCTIONS (Updated for Upsert)
# =====================================================================

def load_nationality_mapping(mssql_conn):
    """Load nationality mapping from SQL Server database table"""
    try:
        cursor = mssql_conn.cursor()
        query = "SELECT ID, Name FROM Nationality"
        cursor.execute(query)
        nationality_map = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.close()
        print(f"✅ Loaded {len(nationality_map)} nationalities from database")
        return nationality_map
    except Exception as e:
        print(f"⚠️ Could not load nationalities: {e}. Using empty mapping.")
        return {}


def parse_full_name(company_name, first_nm, last_nm, father_nm):
    """Parse full name from COMPANY field or use individual name fields"""
    first_name = str(first_nm).strip() if first_nm else ""
    last_name = str(last_nm).strip() if last_nm else ""
    father_name = str(father_nm).strip() if father_nm else ""
    company_name = str(company_name).strip() if company_name else ""

    if not first_name and not last_name and company_name:
        parts = company_name.split()
        if len(parts) >= 3:
            first_name, father_name, last_name = parts[0], parts[1], ' '.join(parts[2:])
        elif len(parts) == 2:
            first_name, last_name = parts[0], parts[1]
        elif len(parts) == 1:
            first_name = parts[0]
    return first_name, last_name, father_name


def normalize_gender(gender):
    """Normalize gender value"""
    if not gender:
        return None
    gender_str = str(gender).strip().lower()
    if gender_str in ['male', 'm']:
        return 'male'
    if gender_str in ['female', 'f']:
        return 'female'
    return gender_str


def get_nationality_name(nationality_id, nationality_map):
    """Convert nationality ID to country name"""
    if nationality_id is None: return None
    if isinstance(nationality_id, str): return nationality_id.strip() or None
    try:
        return nationality_map.get(int(nationality_id), str(nationality_id))
    except (ValueError, TypeError):
        return str(nationality_id)


def format_phone(phone):
    """Clean and format phone number"""
    if not phone: return None
    return ' '.join(str(phone).strip().split()) or None


def migrate_patients(mssql_conn, mysql_conn, nationality_map, org_id=1):
    """Migrate patient data from SQL Server to MySQL using an UPSERT strategy."""
    print("\n" + "=" * 50)
    print("--- Starting Patient Migration (Upsert Mode) ---")
    print("=" * 50)

    try:
        mssql_cursor = mssql_conn.cursor()
        query = """
        SELECT ID, COMPANY, FIRST_NM, LAST_NM, FATHER_NM, MOTHER, ID_NO, BDATE, GENDER,
               MARITALSTATUS, NATIONALITY, PHONE, MOBILE, EMAIL, ADDR1, ADDR2, CITY,
               STATE, ZIP, Bloodgroup, allergies
        FROM CUST WHERE ACTIVE = 1
        """
        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        print(f"📊 Found {len(rows)} patients to process from source.")

        mysql_cursor = mysql_conn.cursor()

        upsert_query = """
        INSERT INTO patients (
            source_id, org_id, first_name, last_name, father_name, mother_name, id_nb, date_of_birth,
            gender, marital_status, nationality, phone, phone_alt, email, address_line1,
            address_line2, city, state, zip_code, blood_group, allergies
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            org_id=VALUES(org_id), first_name=VALUES(first_name), last_name=VALUES(last_name),
            father_name=VALUES(father_name), mother_name=VALUES(mother_name), id_nb=VALUES(id_nb),
            date_of_birth=VALUES(date_of_birth), gender=VALUES(gender), marital_status=VALUES(marital_status),
            nationality=VALUES(nationality), phone=VALUES(phone), phone_alt=VALUES(phone_alt), email=VALUES(email),
            address_line1=VALUES(address_line1), address_line2=VALUES(address_line2), city=VALUES(city),
            state=VALUES(state), zip_code=VALUES(zip_code), blood_group=VALUES(blood_group), allergies=VALUES(allergies);
        """

        inserted_count, updated_count, error_count = 0, 0, 0
        errors_log = []

        for row in rows:
            try:
                first_name, last_name, father_name = parse_full_name(row.COMPANY, row.FIRST_NM, row.LAST_NM, row.FATHER_NM)

                values = (
                    row.IID,  # --- MODIFICATION: Added source_id ---
                    org_id,
                    first_name[:50] if first_name else None,
                    last_name[:50] if last_name else None,
                    father_name[:100] if father_name else None,
                    str(row.MOTHER).strip()[:100] if row.MOTHER else None,
                    str(row.ID_NO).strip()[:50] if row.ID_NO else None,
                    row.BDATE,
                    normalize_gender(row.GENDER),
                    None,
                    get_nationality_name(row.NATIONALITY, nationality_map),
                    format_phone(row.PHONE),
                    format_phone(row.MOBILE),
                    str(row.EMAIL).strip()[:100] if row.EMAIL else None,
                    str(row.ADDR1).strip() if row.ADDR1 else None,
                    str(row.ADDR2).strip() if row.ADDR2 else None,
                    str(row.CITY).strip()[:50] if row.CITY else None,
                    str(row.STATE).strip()[:50] if row.STATE else None,
                    str(row.ZIP).strip()[:10] if row.ZIP else None,
                    str(row.Bloodgroup).strip()[:5] if row.Bloodgroup else None,
                    str(row.allergies).strip() if row.allergies else None
                )
                mysql_cursor.execute(upsert_query, values)

                # --- MODIFICATION: Check rowcount to see if it was an insert or update ---
                # For INSERT, rowcount is 1. For UPDATE, rowcount is 2. For no change, it's 0.
                if mysql_cursor.rowcount == 1:
                    inserted_count += 1
                elif mysql_cursor.rowcount == 2:
                    updated_count += 1

            except Exception as e:
                error_count += 1
                error_msg = f"Patient Source ID {row.IID}: {e}"
                errors_log.append(error_msg)

        mysql_conn.commit()
        print(f"✅ Patient processing completed. Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}")

        if errors_log:
            with open('migration_errors.log', 'w', encoding='utf-8') as f:
                f.write(f"Patient Migration Errors - {datetime.now()}\n" + "=" * 30 + "\n\n")
                f.write("\n".join(errors_log))
            print("📝 Patient error log saved to migration_errors.log")

        mysql_cursor.close()
        mssql_cursor.close()
    except Exception as e:
        print(f"❌ A critical patient migration error occurred: {e}")
        mysql_conn.rollback()


# =====================================================================
# DOCTOR MIGRATION FUNCTIONS (Updated for Upsert)
# =====================================================================

def is_likely_doctor(company_name):
    """Heuristic to determine if a record is a person/doctor or a company/lab."""
    if not company_name: return True
    name_lower = company_name.lower()
    non_doctor_keywords = [
        'company', 'lab', 'x-ray', 'gas', 'dental', 'prodent', 'medical',
        'group', 'store', 'marketing', 'supplies', 'trading', 'sanita',
        'boeckr', 'pharmacol', 'for dental', 'products', 'مختبر', 'شركة'
    ]
    return not any(keyword in name_lower for keyword in non_doctor_keywords)


def parse_doctor_name(company_name):
    """Parses a doctor's name from various formats."""
    if not company_name: return None, None
    name = re.sub(r'^\d+\s*-\s*', '', company_name.strip())
    name = re.sub(r'^(Dr\.?|Dr-)\s*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'/\d+$', '', name)
    if '+' in name: name = name.split('+')[0]
    parts = name.strip().split()
    if not parts: return None, None
    first_name = parts[0]
    last_name = ' '.join(parts[1:]) if len(parts) > 1 else None
    return first_name, last_name


def clean_phone_number(phone_str):
    """Cleans and normalizes a phone number string."""
    if not phone_str: return None
    cleaned = re.sub(r'[^\d\s/\-()+]', '', phone_str)
    return ' '.join(cleaned.split()).strip() or None


def migrate_doctors(mssql_conn, mysql_conn, org_id=1):
    """Fetches, transforms, and migrates doctor data using an UPSERT strategy."""
    print("\n" + "=" * 50)
    print("--- Starting Doctor Migration (Upsert Mode) ---")
    print("=" * 50)

    try:
        mssql_cursor = mssql_conn.cursor()
        query = "SELECT VENDSRH, COMPANY, PHONE, CONTACT FROM Vend"
        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()
        print(f"📊 Found {len(rows)} total records in source table 'Vend'.")

        mysql_cursor = mysql_conn.cursor()
        # --- MODIFICATION: Updated query for UPSERT ---
        upsert_query = """
        INSERT INTO doctors (
            source_id, org_id, first_name, last_name, phone, phone_alt, license_number
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            org_id=VALUES(org_id), first_name=VALUES(first_name), last_name=VALUES(last_name),
            phone=VALUES(phone), phone_alt=VALUES(phone_alt), license_number=VALUES(license_number);
        """

        inserted_count, updated_count, error_count = 0, 0, 0
        skipped_log, error_log = [], []

        for row in rows:
            vendsrh, company, phone, contact = row
            original_company = company.strip() if company else ""

            try:
                if not is_likely_doctor(original_company):
                    skipped_log.append(f"Source ID {vendsrh}: '{original_company}' (Reason: Matched non-doctor keyword)")
                    continue

                first_name, last_name = parse_doctor_name(original_company)
                if not first_name:
                    skipped_log.append(f"Source ID {vendsrh}: '{original_company}' (Reason: Could not parse a valid name)")
                    continue

                values = (
                    str(vendsrh),  # --- MODIFICATION: Added source_id ---
                    org_id,
                    first_name[:50],
                    last_name[:50] if last_name else None,
                    clean_phone_number(phone),
                    clean_phone_number(contact),
                    str(vendsrh)  # Using source ID as license number
                )

                mysql_cursor.execute(upsert_query, values)

                # --- MODIFICATION: Check rowcount to see if it was an insert or update ---
                if mysql_cursor.rowcount == 1:
                    inserted_count += 1
                elif mysql_cursor.rowcount == 2:
                    updated_count += 1

            except Exception as e:
                error_count += 1
                error_log.append(f"Source ID {vendsrh}: '{original_company}' - ERROR: {e}")

        mysql_conn.commit()

        print("\n" + "=" * 30)
        print("DOCTOR MIGRATION SUMMARY")
        print("=" * 30)
        print(f"✅ Inserted new doctors: {inserted_count}")
        print(f"🔄 Updated existing doctors: {updated_count}")
        print(f"⏭️  Skipped records:        {len(skipped_log)} (see skipped_doctors.log)")
        print(f"❌ Errored records:        {len(error_log)} (see doctors_migration_errors.log)")

        if skipped_log:
            with open('skipped_doctors.log', 'w', encoding='utf-8') as f:
                f.write(f"Skipped Doctor Records - {datetime.now()}\n" + "=" * 30 + "\n\n")
                f.write("\n".join(skipped_log))

        if error_log:
            with open('doctors_migration_errors.log', 'w', encoding='utf-8') as f:
                f.write(f"Doctor Error Records - {datetime.now()}\n" + "=" * 30 + "\n\n")
                f.write("\n".join(error_log))

        mysql_cursor.close()
        mssql_cursor.close()
    except Exception as e:
        print(f"❌ A critical doctor migration error occurred: {e}")
        mysql_conn.rollback()


# =====================================================================
# MAIN SCRIPT EXECUTION (Updated for better connection handling)
# =====================================================================

def main():
    """Main function to orchestrate the entire migration process."""
    print("🚀 Starting Full Data Migration: Patients and Doctors (Upsert Mode) 🚀")

    # --- CONFIGURE YOUR DATABASE CONNECTIONS HERE ---
    MSSQL_SERVER = 'localhost'
    MSSQL_DATABASE = 'BizriDental'
    USE_WINDOWS_AUTH = True
    MSSQL_USERNAME = None
    MSSQL_PASSWORD = None

    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = 'P@ssw0rd8899'
    MYSQL_DATABASE = 'patient_management_system'
    ORG_ID = 1

    mssql_conn = create_mssql_connection(MSSQL_SERVER, MSSQL_DATABASE, USE_WINDOWS_AUTH, MSSQL_USERNAME, MSSQL_PASSWORD)
    mysql_conn = create_mysql_connection(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)

    if mssql_conn and mysql_conn:
        try:
            # Step 1: Migrate Patients
            nationality_map = load_nationality_mapping(mssql_conn)
            migrate_patients(mssql_conn, mysql_conn, nationality_map, ORG_ID)

            # Step 2: Migrate Doctors
            # --- MODIFICATION: No need to reconnect, just reuse the existing connection ---
            migrate_doctors(mssql_conn, mysql_conn, ORG_ID)
        finally:
            # Cleanly close connections
            if mssql_conn: mssql_conn.close()
            if mysql_conn and mysql_conn.is_connected(): mysql_conn.close()
            print("\n🔒 Database connections closed.")
    else:
        print("\n❌ Migration aborted due to initial database connection failure.")
        return

    print("\n" + "=" * 50)
    print("✅ Full migration process completed.")
    print("=" * 50)


if __name__ == "__main__":
    main()