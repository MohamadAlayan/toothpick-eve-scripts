import pyodbc
import mysql.connector
from mysql.connector import Error
from datetime import datetime


def create_mssql_connection(server, database, use_windows_auth=True, username=None, password=None):
    """Create connection to Microsoft SQL Server"""
    try:
        if use_windows_auth:
            connection_string = (
                f'DRIVER={{SQL Server}};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'Trusted_Connection=yes;'
            )
        else:
            connection_string = (
                f'DRIVER={{SQL Server}};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'UID={username};'
                f'PWD={password}'
            )

        connection = pyodbc.connect(connection_string)
        print("✅ Successfully connected to SQL Server")
        return connection
    except Exception as e:
        print(f"❌ Error connecting to SQL Server: {e}")
        return None


def create_mysql_connection(host, user, password, database):
    """Create connection to MySQL server"""
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


def load_nationality_mapping(mssql_conn):
    """Load nationality mapping from SQL Server database table"""
    try:
        cursor = mssql_conn.cursor()
        query = "SELECT ID, Name FROM Nationality"
        cursor.execute(query)

        nationality_map = {}
        for row in cursor.fetchall():
            nationality_map[row[0]] = row[1]

        cursor.close()
        print(f"✅ Loaded {len(nationality_map)} nationalities from database")
        return nationality_map

    except Exception as e:
        print(f"⚠️ Could not load nationalities: {e}")
        print("Using empty nationality mapping...")
        return {}


def parse_full_name(company_name, first_nm, last_nm, father_nm):
    """Parse full name from COMPANY field or use individual name fields"""
    first_nm = str(first_nm).strip() if first_nm else ""
    last_nm = str(last_nm).strip() if last_nm else ""
    father_nm = str(father_nm).strip() if father_nm else ""
    company_name = str(company_name).strip() if company_name else ""

    first_name = first_nm if first_nm else ""
    last_name = last_nm if last_nm else ""
    father_name = father_nm if father_nm else ""

    # Parse from COMPANY if individual fields are empty
    if not first_name and not last_name and company_name:
        parts = company_name.split()
        if len(parts) >= 3:
            first_name = parts[0]
            father_name = parts[1]
            last_name = ' '.join(parts[2:])
        elif len(parts) == 2:
            first_name = parts[0]
            last_name = parts[1]
        elif len(parts) == 1:
            first_name = parts[0]

    return first_name, last_name, father_name


def normalize_gender(gender):
    """Normalize gender value - saves any value even if invalid"""
    if not gender:
        return None

    gender = str(gender).strip()
    gender_upper = gender.upper()

    if gender_upper in ['MALE', 'M']:
        return 'Male'
    elif gender_upper in ['FEMALE', 'F']:
        return 'Female'
    else:
        return gender if gender else None


def get_nationality_name(nationality_id, nationality_map):
    """Convert nationality ID to country name"""
    if nationality_id is None:
        return None

    # If already a string, return it
    if isinstance(nationality_id, str):
        return nationality_id.strip() if nationality_id.strip() else None

    # If it's a number, try to map it
    try:
        nat_id = int(nationality_id)
        return nationality_map.get(nat_id, str(nat_id))
    except (ValueError, TypeError):
        return str(nationality_id) if nationality_id else None


def format_phone(phone):
    """Clean and format phone number - saves any value"""
    if not phone:
        return None

    phone = str(phone).strip()
    phone = ' '.join(phone.split())

    return phone if phone else None


def migrate_patients(mssql_conn, mysql_conn, nationality_map, org_id=1):
    """Migrate patient data from SQL Server to MySQL"""
    try:
        mssql_cursor = mssql_conn.cursor()

        query = """
        SELECT 
            IID,
            COMPANY,
            FIRST_NM,
            LAST_NM,
            FATHER_NM,
            MOTHER,
            ID_NO,
            BDATE,
            GENDER,
            MARITALSTATUS,
            NATIONALITY,
            PHONE,
            MOBILE,
            EMAIL,
            ADDR1,
            ADDR2,
            CITY,
            STATE,
            ZIP,
            Bloodgroup,
            allergies
        FROM CUST
        WHERE ACTIVE = 1
        """

        mssql_cursor.execute(query)
        rows = mssql_cursor.fetchall()

        print(f"📊 Found {len(rows)} patients to migrate")

        mysql_cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO patients (
            org_id, first_name, last_name, father_name, mother_name,
            id_nb, date_of_birth, gender, marital_status, nationality,
            phone, phone_alt, email, address_line1, address_line2,
            city, state, zip_code, blood_group, allergies
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        success_count = 0
        error_count = 0
        errors_log = []

        for row in rows:
            try:
                iid = row[0]
                company = row[1]
                first_nm = row[2]
                last_nm = row[3]
                father_nm = row[4]
                mother = row[5]
                id_no = row[6]
                bdate = row[7]
                gender = row[8]
                marital_status = row[9]
                nationality = row[10]
                phone = row[11]
                mobile = row[12]
                email = row[13]
                addr1 = row[14]
                addr2 = row[15]
                city = row[16]
                state = row[17]
                zip_code = row[18]
                blood_group = row[19]
                allergies = row[20]

                first_name, last_name, father_name = parse_full_name(
                    company, first_nm, last_nm, father_nm
                )

                normalized_gender = normalize_gender(gender)
                nationality_name = get_nationality_name(nationality, nationality_map)
                phone_formatted = format_phone(phone)
                mobile_formatted = format_phone(mobile)

                values = (
                    org_id,
                    first_name[:50] if first_name else None,
                    last_name[:50] if last_name else None,
                    father_name[:100] if father_name else None,
                    str(mother).strip()[:100] if mother and str(mother).strip() else None,
                    str(id_no).strip()[:50] if id_no and str(id_no).strip() else None,
                    bdate if bdate else None,
                    normalized_gender,
                    None,
                    nationality_name,
                    phone_formatted,
                    mobile_formatted,
                    str(email).strip()[:100] if email and str(email).strip() else None,
                    str(addr1).strip() if addr1 and str(addr1).strip() else None,
                    str(addr2).strip() if addr2 and str(addr2).strip() else None,
                    str(city).strip()[:50] if city and str(city).strip() else None,
                    str(state).strip()[:50] if state and str(state).strip() else None,
                    str(zip_code).strip()[:10] if zip_code and str(zip_code).strip() else None,
                    str(blood_group).strip()[:5] if blood_group and str(blood_group).strip() else None,
                    str(allergies).strip() if allergies and str(allergies).strip() else None
                )

                mysql_cursor.execute(insert_query, values)
                success_count += 1

                if success_count % 100 == 0:
                    print(f"✅ Migrated {success_count} patients...")

            except Exception as e:
                error_count += 1
                error_msg = f"IID {iid}: {str(e)}"
                errors_log.append(error_msg)
                print(f"❌ Error: {error_msg}")

        mysql_conn.commit()

        print(f"\n{'=' * 50}")
        print(f"✅ Successfully migrated: {success_count} patients")
        print(f"❌ Errors: {error_count} patients")
        print(f"{'=' * 50}")

        if errors_log:
            with open('migration_errors.log', 'w', encoding='utf-8') as f:
                f.write(f"Migration Errors - {datetime.now()}\n")
                f.write("=" * 50 + "\n\n")
                for error in errors_log:
                    f.write(f"{error}\n")
            print(f"📝 Error log saved: migration_errors.log")

        mysql_cursor.close()
        mssql_cursor.close()

    except Exception as e:
        print(f"❌ Migration error: {e}")


def main():
    # ============================================
    # SQL SERVER CONFIGURATION
    # ============================================
    MSSQL_SERVER = 'localhost'
    MSSQL_DATABASE = 'BizriDental'
    USE_WINDOWS_AUTH = True
    MSSQL_USERNAME = None
    MSSQL_PASSWORD = None

    # ============================================
    # MYSQL CONFIGURATION
    # ============================================
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = 'P@ssw0rd8899'
    MYSQL_DATABASE = 'patient_management_system'

    ORG_ID = 1

    print("🚀 Starting Patient Data Migration")
    print("=" * 50)

    # Connect to SQL Server
    mssql_conn = create_mssql_connection(
        MSSQL_SERVER,
        MSSQL_DATABASE,
        USE_WINDOWS_AUTH,
        MSSQL_USERNAME,
        MSSQL_PASSWORD
    )

    # Connect to MySQL
    mysql_conn = create_mysql_connection(
        MYSQL_HOST,
        MYSQL_USER,
        MYSQL_PASSWORD,
        MYSQL_DATABASE
    )

    if mssql_conn and mysql_conn:
        # Load nationalities
        nationality_map = load_nationality_mapping(mssql_conn)

        # Migrate patients
        migrate_patients(mssql_conn, mysql_conn, nationality_map, ORG_ID)

        # Close connections
        if mssql_conn:
            mssql_conn.close()
            print("\n🔒 SQL Server connection closed")

        if mysql_conn.is_connected():
            mysql_conn.close()
            print("🔒 MySQL connection closed")
    else:
        print("❌ Failed to establish database connections")


if __name__ == "__main__":
    main()