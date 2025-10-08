import os
import re
import pyodbc
import mysql.connector
from mysql.connector import Error
from datetime import datetime


# ================================================================
# CONNECTIONS
# ================================================================

def create_mssql_connection(server, database, use_windows_auth=True, username=None, password=None):
    """Connect to SQL Server."""
    try:
        if use_windows_auth:
            conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        else:
            conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        print("✅ Connected to SQL Server")
        return conn
    except pyodbc.Error as e:
        print(f"❌ SQL Server connection error: {e}")
        return None


def create_mysql_connection(host, user, password, database):
    """Connect to MySQL."""
    try:
        conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
        if conn.is_connected():
            print("✅ Connected to MySQL")
            return conn
    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        return None


# ================================================================
# UTILITIES
# ================================================================

def ensure_logs_folder():
    """Make sure the logs directory exists."""
    if not os.path.exists("logs"):
        os.makedirs("logs")


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
    """Build full name if missing."""
    first = str(first_nm).strip() if first_nm else ""
    last = str(last_nm).strip() if last_nm else ""
    father = str(father_nm).strip() if father_nm else ""
    company = str(company).strip() if company else ""

    if not first and not last and company:
        parts = company.split()
        if len(parts) >= 3:
            first, father, last = parts[0], parts[1], " ".join(parts[2:])
        elif len(parts) == 2:
            first, last = parts
        else:
            first = parts[0]
    return first, last, father


def normalize_gender(g):
    """Return standardized gender text."""
    if not g:
        return None
    g = str(g).strip().lower()
    if g in ["male", "m"]:
        return "male"
    if g in ["female", "f"]:
        return "female"
    return g


def clean_phone(phone):
    """Clean phone format."""
    if not phone:
        return None
    return " ".join(str(phone).strip().split()) or None


def get_nationality_name(nationality_id, mapping):
    """Translate nationality ID to name."""
    if nationality_id is None:
        return None
    if isinstance(nationality_id, str):
        return nationality_id.strip() or None
    try:
        return mapping.get(int(nationality_id), str(nationality_id))
    except Exception:
        return str(nationality_id)


# ================================================================
# PATIENT MIGRATION
# ================================================================

def migrate_patients(mssql_conn, mysql_conn, nationality_map):
    print("\n=== PATIENT MIGRATION STARTED ===")

    try:
        mssql_cursor = mssql_conn.cursor()
        mssql_cursor.execute("""
            SELECT ID, COMPANY, FIRST_NM, LAST_NM, FATHER_NM, MOTHER, ID_NO, BDATE, GENDER,
                   MARITALSTATUS, NATIONALITY, PHONE, MOBILE, EMAIL, ADDR1, ADDR2, CITY,
                   STATE, ZIP, Bloodgroup, allergies
            FROM CUST WHERE ACTIVE = 1
        """)
        rows = mssql_cursor.fetchall()
        print(f"📊 {len(rows)} patients fetched from source")

        mysql_cursor = mysql_conn.cursor()
        query = """
        INSERT INTO patients (
            source_id, first_name, last_name, father_name, mother_name, id_nb, date_of_birth,
            gender, marital_status, nationality, phone, phone_alt, email, address_line1,
            address_line2, city, state, zip_code, blood_group, allergies
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name=VALUES(first_name), last_name=VALUES(last_name),
            father_name=VALUES(father_name), mother_name=VALUES(mother_name),
            id_nb=VALUES(id_nb), date_of_birth=VALUES(date_of_birth),
            gender=VALUES(gender), marital_status=VALUES(marital_status),
            nationality=VALUES(nationality), phone=VALUES(phone),
            phone_alt=VALUES(phone_alt), email=VALUES(email),
            address_line1=VALUES(address_line1), address_line2=VALUES(address_line2),
            city=VALUES(city), state=VALUES(state), zip_code=VALUES(zip_code),
            blood_group=VALUES(blood_group), allergies=VALUES(allergies);
        """

        inserted, updated, errors = 0, 0, []

        for row in rows:
            try:
                first, last, father = parse_full_name(row.COMPANY, row.FIRST_NM, row.LAST_NM, row.FATHER_NM)
                data = (
                    row.ID,
                    first[:50] or None,
                    last[:50] or None,
                    father[:100] or None,
                    str(row.MOTHER).strip()[:100] if row.MOTHER else None,
                    str(row.ID_NO).strip()[:50] if row.ID_NO else None,
                    row.BDATE,
                    normalize_gender(row.GENDER),
                    str(row.MARITALSTATUS).strip() if row.MARITALSTATUS else None,
                    get_nationality_name(row.NATIONALITY, nationality_map),
                    clean_phone(row.PHONE),
                    clean_phone(row.MOBILE),
                    str(row.EMAIL).strip()[:100] if row.EMAIL else None,
                    str(row.ADDR1).strip() if row.ADDR1 else None,
                    str(row.ADDR2).strip() if row.ADDR2 else None,
                    str(row.CITY).strip()[:50] if row.CITY else None,
                    str(row.STATE).strip()[:50] if row.STATE else None,
                    str(row.ZIP).strip()[:10] if row.ZIP else None,
                    str(row.Bloodgroup).strip()[:5] if row.Bloodgroup else None,
                    str(row.allergies).strip() if row.allergies else None
                )

                mysql_cursor.execute(query, data)
                if mysql_cursor.rowcount == 1:
                    inserted += 1
                elif mysql_cursor.rowcount == 2:
                    updated += 1

            except Exception as e:
                errors.append(f"Patient ID {row.ID}: {e}")

        mysql_conn.commit()
        print(f"✅ Patients migrated — inserted: {inserted}, updated: {updated}, errors: {len(errors)}")

        if errors:
            ensure_logs_folder()
            with open("logs/patient_errors.log", "w", encoding="utf-8") as f:
                f.write(f"Patient Migration Errors - {datetime.now()}\n\n")
                f.write("\n".join(errors))

        mysql_cursor.close()
        mssql_cursor.close()

    except Exception as e:
        print(f"❌ Critical patient migration error: {e}")
        mysql_conn.rollback()


# ================================================================
# DOCTOR MIGRATION
# ================================================================

def is_likely_doctor(name):
    """Simple filter to exclude company-like records."""
    if not name:
        return True
    n = name.lower()
    # exclude = [
    #     "company", "lab", "x-ray", "gas", "dental", "prodent", "medical",
    #     "group", "store", "marketing", "supplies", "trading", "sanita",
    #     "pharma", "مختبر", "شركة"
    # ]
    exclude = []
    return not any(k in n for k in exclude)


def parse_doctor_name(name):
    """Extract first and last name from doctor/company text."""
    if not name:
        return None, None
    n = re.sub(r"^\d+\s*-\s*", "", name.strip())
    n = re.sub(r"^(Dr\.?|Dr-)\s*", "", n, flags=re.IGNORECASE)
    n = re.sub(r"/\d+$", "", n)
    if "+" in n:
        n = n.split("+")[0]
    parts = n.split()
    if not parts:
        return None, None
    first = parts[0]
    last = " ".join(parts[1:]) if len(parts) > 1 else None
    return first, last


def migrate_doctors(mssql_conn, mysql_conn):
    print("\n=== DOCTOR MIGRATION STARTED ===")

    try:
        mssql_cursor = mssql_conn.cursor()
        mssql_cursor.execute("SELECT VENDSRH, COMPANY, PHONE, CONTACT FROM Vend")
        rows = mssql_cursor.fetchall()
        print(f"📊 {len(rows)} vendor records fetched")

        mysql_cursor = mysql_conn.cursor()
        query = """
        INSERT INTO doctors (
            source_id, first_name, last_name, phone, phone_alt, license_number
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name=VALUES(first_name),
            last_name=VALUES(last_name),
            phone=VALUES(phone),
            phone_alt=VALUES(phone_alt),
            license_number=VALUES(license_number);
        """

        inserted, updated, skipped, errors = 0, 0, [], []

        for row in rows:
            sid, company, phone, contact = row
            name = company.strip() if company else ""

            try:
                if not is_likely_doctor(name):
                    skipped.append(f"{sid}: {name} (filtered)")
                    continue

                first, last = parse_doctor_name(name)
                if not first:
                    skipped.append(f"{sid}: {name} (invalid name)")
                    continue

                data = (
                    str(sid),
                    first[:50],
                    last[:50] if last else None,
                    clean_phone(phone),
                    clean_phone(contact),
                    str(sid)
                )

                mysql_cursor.execute(query, data)
                if mysql_cursor.rowcount == 1:
                    inserted += 1
                elif mysql_cursor.rowcount == 2:
                    updated += 1

            except Exception as e:
                errors.append(f"Doctor ID {sid}: {e}")

        mysql_conn.commit()
        print(f"✅ Doctors migrated — inserted: {inserted}, updated: {updated}, skipped: {len(skipped)}, errors: {len(errors)}")

        ensure_logs_folder()
        if skipped:
            with open("logs/doctors_skipped.log", "w", encoding="utf-8") as f:
                f.write(f"Skipped Doctors - {datetime.now()}\n\n")
                f.write("\n".join(skipped))
        if errors:
            with open("logs/doctors_errors.log", "w", encoding="utf-8") as f:
                f.write(f"Doctor Errors - {datetime.now()}\n\n")
                f.write("\n".join(errors))

        mysql_cursor.close()
        mssql_cursor.close()

    except Exception as e:
        print(f"❌ Critical doctor migration error: {e}")
        mysql_conn.rollback()


# ================================================================
# MAIN
# ================================================================

def main():
    print("🚀 Starting migration for ToothpickEVE")

    MSSQL_SERVER = "localhost"
    MSSQL_DATABASE = "BizriDental"
    USE_WINDOWS_AUTH = True
    MSSQL_USERNAME = None
    MSSQL_PASSWORD = None

    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "P@ssw0rd8899"
    MYSQL_DATABASE = "patient_management_system"

    mssql = create_mssql_connection(MSSQL_SERVER, MSSQL_DATABASE, USE_WINDOWS_AUTH, MSSQL_USERNAME, MSSQL_PASSWORD)
    mysql = create_mysql_connection(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)

    if not (mssql and mysql):
        print("❌ Migration aborted due to connection failure.")
        return

    try:
        nationality_map = load_nationality_mapping(mssql)
        migrate_patients(mssql, mysql, nationality_map)
        migrate_doctors(mssql, mysql)
    finally:
        if mssql:
            mssql.close()
        if mysql and mysql.is_connected():
            mysql.close()
        print("\n🔒 Connections closed.\n✅ Migration completed.")


if __name__ == "__main__":
    main()
