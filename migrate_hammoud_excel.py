# ---------------------------------------------
# ToothpickEVE - Hammoud Excel Migration Script
# Purpose: Import data from Hammoud Clinics Excel file to MySQL
#
# Features:
#   - ✅ Clean and validate all data
#   - ✅ Handle name-based lookups (patient/doctor names → IDs)
#   - ✅ Smart data transformation and normalization
#   - ✅ Comprehensive error logging
#   - ✅ Progress tracking and verification
# ---------------------------------------------

import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
from datetime import datetime, time as time_type
import os
import traceback


# ================================================================
# CONFIGURATION
# ================================================================

class Config:
    """Centralized configuration"""
    # MySQL Settings
    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "P@ssw0rd8899"
    MYSQL_DATABASE = "hammoud_patient_management_system"

    # Excel File
    EXCEL_FILE = "databases/Hammoud_Clinics_Data.xlsx"

    # Migration Settings
    BATCH_SIZE = 50
    DEBUG_MODE = True
    TEST_MODE = False  # Set to True to limit records for testing


# ================================================================
# DATABASE CONNECTION
# ================================================================

def create_mysql_connection(host, user, password, database):
    """Connect to MySQL database"""
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=False,
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
# UTILITY FUNCTIONS
# ================================================================

def ensure_logs_folder():
    """Create logs folder if it doesn't exist"""
    if not os.path.exists("logs"):
        os.makedirs("logs")
    return "logs"


def clean_string(value, max_length=None):
    """Clean and sanitize string values"""
    if value is None or pd.isna(value) or str(value).strip() == '':
        return None

    s = str(value).strip()

    # Remove excessive whitespace
    s = ' '.join(s.split())

    if max_length and len(s) > max_length:
        s = s[:max_length]

    return s if s else None


def clean_phone(phone):
    """Clean and format phone numbers: remove spaces, add + prefix if missing"""
    if not phone or pd.isna(phone):
        return None

    # Convert to string and strip
    phone_str = str(phone).strip()

    # Remove all spaces
    phone_str = phone_str.replace(' ', '')

    # Return None if empty after cleanup
    if not phone_str:
        return None

    # Add + prefix if not present
    if not phone_str.startswith('+'):
        phone_str = '+' + phone_str

    return phone_str


def parse_date(date_value):
    """Parse and validate date values"""
    if not date_value or pd.isna(date_value):
        return None

    try:
        # If already a datetime object
        if isinstance(date_value, pd.Timestamp):
            return date_value.date()

        # If string, try parsing
        if isinstance(date_value, str):
            # Try multiple formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                try:
                    return datetime.strptime(date_value.strip(), fmt).date()
                except ValueError:
                    continue

        # Try pandas to_datetime as fallback
        return pd.to_datetime(date_value).date()
    except:
        return None


def parse_time(time_value):
    """Parse time values from datetime"""
    if not time_value or pd.isna(time_value):
        return None

    try:
        if isinstance(time_value, pd.Timestamp):
            return time_value.time()

        if isinstance(time_value, str):
            return datetime.strptime(time_value.strip(), '%H:%M:%S').time()

        return pd.to_datetime(time_value).time()
    except:
        return None


def normalize_gender(gender):
    """Normalize gender values"""
    if not gender or pd.isna(gender):
        return None

    g = str(gender).strip().lower()

    if g in ['male', 'm', '1', 'man']:
        return 'male'
    elif g in ['female', 'f', '2', 'woman']:
        return 'female'

    return None


def parse_name(full_name):
    """Split full name into first and last name"""
    if not full_name or pd.isna(full_name):
        return None, None

    name = str(full_name).strip()
    parts = name.split()

    if len(parts) >= 2:
        first = parts[0]
        last = ' '.join(parts[1:])
        return first, last
    elif len(parts) == 1:
        return parts[0], None

    return None, None


def calculate_duration_minutes(start_date, end_date):
    """Calculate duration in minutes between two datetimes"""
    if not start_date or not end_date or pd.isna(start_date) or pd.isna(end_date):
        return None

    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        duration = (end - start).total_seconds() / 60
        return int(duration) if duration > 0 else None
    except:
        return None


# ================================================================
# LOOKUP FUNCTIONS
# ================================================================

def normalize_name_key(name):
    """Normalize name for lookup - removes extra spaces and lowercases"""
    if not name:
        return None
    # Remove all extra whitespace and convert to lowercase
    return ' '.join(str(name).strip().lower().split())


def create_patient_lookup_map(mysql_conn):
    """Create a mapping of patient names to source_ids"""
    cursor = mysql_conn.cursor()
    query = "SELECT source_id, first_name, last_name FROM patients"
    cursor.execute(query)

    patient_map = {}
    for row in cursor.fetchall():
        source_id, first_name, last_name = row
        if first_name and last_name:
            # Create normalized lookup key
            key = normalize_name_key(f"{first_name} {last_name}")
            if key:
                patient_map[key] = source_id

    cursor.close()
    print(f"📋 Created patient lookup map with {len(patient_map)} entries")
    return patient_map


def create_doctor_lookup_map(mysql_conn):
    """Create a mapping of doctor names to source_ids"""
    cursor = mysql_conn.cursor()
    query = "SELECT source_id, first_name, last_name FROM doctors"
    cursor.execute(query)

    doctor_map = {}
    for row in cursor.fetchall():
        source_id, first_name, last_name = row
        if first_name and last_name:
            # Create normalized lookup key
            key = normalize_name_key(f"{first_name} {last_name}")
            if key:
                doctor_map[key] = source_id

    cursor.close()
    print(f"👨‍⚕️ Created doctor lookup map with {len(doctor_map)} entries")
    return doctor_map


def lookup_patient_id(patient_name, patient_map):
    """Find patient source_id by name with normalized matching"""
    if not patient_name or pd.isna(patient_name):
        return None

    key = normalize_name_key(patient_name)
    if not key:
        return None

    return patient_map.get(key)


def lookup_doctor_id(doctor_name, doctor_map):
    """Find doctor source_id by name with normalized matching"""
    if not doctor_name or pd.isna(doctor_name):
        return None

    key = normalize_name_key(doctor_name)
    if not key:
        return None

    return doctor_map.get(key)


# ================================================================
# MIGRATION FUNCTIONS
# ================================================================

def migrate_patients(mysql_conn, excel_file):
    """Migrate patients from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("PATIENT MIGRATION STARTED")
    print("=" * 60)

    try:
        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='Patients')

        if Config.TEST_MODE:
            df = df.head(20)

        total_records = len(df)
        print(f"📊 Found {total_records} patients in Excel")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO patients (
            source_id, first_name, last_name, father_name, mother_name,
            gender, email, phone, phone_alt, date_of_birth, address_line1,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            father_name = VALUES(father_name),
            mother_name = VALUES(mother_name),
            gender = VALUES(gender),
            email = VALUES(email),
            phone = VALUES(phone),
            phone_alt = VALUES(phone_alt),
            date_of_birth = VALUES(date_of_birth),
            address_line1 = VALUES(address_line1),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []

        for idx, row in df.iterrows():
            try:
                # Parse dates
                created_at = parse_date(row.get('created_at'))
                dob = parse_date(row.get('dob'))

                # Validate DOB (shouldn't be in the future or too recent)
                if dob and dob > datetime.now().date():
                    dob = None

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    clean_string(row.get('first_name'), 100),
                    clean_string(row.get('last_name'), 100),
                    clean_string(row.get('middle_name'), 100),  # Using middle_name as father_name
                    clean_string(row.get('maiden_name'), 100),  # Using maiden_name as mother_name
                    normalize_gender(row.get('gender')),
                    clean_string(row.get('email'), 100),
                    clean_phone(row.get('phone_number')),
                    clean_phone(row.get('alt_number')),
                    dob,
                    clean_string(row.get('address')),
                    created_at,
                    datetime.now()
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

                if (idx + 1) % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    progress = (idx + 1) * 100 // total_records
                    print(f"  Progress: {idx + 1}/{total_records} ({progress}%)")

            except Exception as e:
                error_msg = f"Patient ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE and len(errors) <= 5:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ PATIENT MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        if errors:
            log_path = os.path.join(ensure_logs_folder(), "patient_errors.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Patient Migration Errors - {datetime.now()}\n\n")
                for error in errors:
                    f.write(f"{error}\n")
            print(f"   Error log: {log_path}")

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in patient migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_doctors(mysql_conn, excel_file):
    """Extract and migrate doctors from Appointments and Invoices sheets"""
    print("\n" + "=" * 60)
    print("DOCTOR MIGRATION STARTED")
    print("=" * 60)

    try:
        # Extract unique doctors from multiple sheets
        df_appointments = pd.read_excel(excel_file, sheet_name='Appointments')
        df_invoices = pd.read_excel(excel_file, sheet_name='Invoices')

        # Get unique doctor names
        doctors_from_appts = df_appointments['doctor'].dropna().unique()
        doctors_from_invoices = df_invoices['doctor'].dropna().unique()

        all_doctors = set(list(doctors_from_appts) + list(doctors_from_invoices))

        print(f"📊 Found {len(all_doctors)} unique doctors")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO doctors (
            source_id, title, first_name, last_name, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, skipped = 0, 0, 0

        for idx, doctor_name in enumerate(all_doctors, 1):
            try:
                first_name, last_name = parse_name(doctor_name)

                if not first_name:
                    skipped += 1
                    continue

                # Generate source_id from index (numeric only)
                source_id = str(idx)

                data = (
                    source_id,
                    'Dr',  # Default title
                    clean_string(first_name, 50),
                    clean_string(last_name, 50),
                    datetime.now(),
                    datetime.now()
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

            except Exception as e:
                if Config.DEBUG_MODE:
                    print(f"  ❌ Error for doctor '{doctor_name}': {e}")
                skipped += 1

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ DOCTOR MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Skipped: {skipped}")
        print("-" * 60)

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in doctor migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_appointments(mysql_conn, excel_file):
    """Migrate appointments from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("APPOINTMENT MIGRATION STARTED")
    print("=" * 60)

    try:
        # Create lookup maps
        patient_map = create_patient_lookup_map(mysql_conn)
        doctor_map = create_doctor_lookup_map(mysql_conn)

        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='Appointments')

        if Config.TEST_MODE:
            df = df.head(20)

        total_records = len(df)
        print(f"📊 Found {total_records} appointments in Excel")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO appointments (
            source_id, patient_id, doctor_id, appointment_date, appointment_time,
            duration_minutes, room, status, notes, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            patient_id = VALUES(patient_id),
            doctor_id = VALUES(doctor_id),
            appointment_date = VALUES(appointment_date),
            appointment_time = VALUES(appointment_time),
            duration_minutes = VALUES(duration_minutes),
            room = VALUES(room),
            status = VALUES(status),
            notes = VALUES(notes),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []
        missing_patients, missing_doctors = 0, 0

        for idx, row in df.iterrows():
            try:
                # Lookup patient and doctor IDs
                patient_id = lookup_patient_id(row.get('patient'), patient_map)
                doctor_id = lookup_doctor_id(row.get('doctor'), doctor_map)

                if not patient_id:
                    missing_patients += 1
                    if Config.DEBUG_MODE and missing_patients <= 3:
                        print(f"  ⚠️ Patient not found: {row.get('patient')}")

                if not doctor_id and row.get('doctor'):
                    missing_doctors += 1
                    if Config.DEBUG_MODE and missing_doctors <= 3:
                        print(f"  ⚠️ Doctor not found: {row.get('doctor')}")

                # Parse dates and times
                start_dt = pd.to_datetime(row.get('start_date'))
                end_dt = pd.to_datetime(row.get('end_date'))

                appointment_date = start_dt.date()
                appointment_time = start_dt.time()
                duration_minutes = calculate_duration_minutes(start_dt, end_dt)

                # Map status
                status = clean_string(row.get('status'), 50)
                if not status:
                    status = 'scheduled'

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    patient_id,
                    doctor_id,
                    appointment_date,
                    appointment_time,
                    duration_minutes,
                    clean_string(row.get('room'), 50),
                    status,
                    clean_string(row.get('created_by')),  # Store creator in notes
                    parse_date(row.get('created_at')),
                    datetime.now()
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

                if (idx + 1) % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    progress = (idx + 1) * 100 // total_records
                    print(f"  Progress: {idx + 1}/{total_records} ({progress}%)")

            except Exception as e:
                error_msg = f"Appointment ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE and len(errors) <= 5:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ APPOINTMENT MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print(f"   Missing Patients: {missing_patients}")
        print(f"   Missing Doctors: {missing_doctors}")
        print("-" * 60)

        if errors:
            log_path = os.path.join(ensure_logs_folder(), "appointment_errors.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Appointment Migration Errors - {datetime.now()}\n\n")
                for error in errors:
                    f.write(f"{error}\n")
            print(f"   Error log: {log_path}")

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in appointment migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_invoices(mysql_conn, excel_file):
    """Migrate invoices from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("INVOICE MIGRATION STARTED")
    print("=" * 60)

    try:
        # Create lookup maps
        patient_map = create_patient_lookup_map(mysql_conn)
        doctor_map = create_doctor_lookup_map(mysql_conn)

        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='Invoices')

        # Filter out expenses (is_expense = 1)
        df_invoices = df[df['is_expense'].isna() | (df['is_expense'] != 1.0)]

        if Config.TEST_MODE:
            df_invoices = df_invoices.head(20)

        total_records = len(df_invoices)
        print(f"📊 Found {total_records} patient invoices in Excel (filtered {len(df) - total_records} expenses)")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO invoices (
            source_id, patient_id, doctor_id, invoice_date, due_date,
            status, currency, discount_type, discount_value,
            total_amount, amount_paid, balance_due, notes,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            patient_id = VALUES(patient_id),
            doctor_id = VALUES(doctor_id),
            invoice_date = VALUES(invoice_date),
            due_date = VALUES(due_date),
            status = VALUES(status),
            discount_value = VALUES(discount_value),
            total_amount = VALUES(total_amount),
            amount_paid = VALUES(amount_paid),
            balance_due = VALUES(balance_due),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []
        missing_patients = 0

        for idx, row in df_invoices.iterrows():
            try:
                # Lookup patient and doctor IDs
                patient_id = lookup_patient_id(row.get('patient'), patient_map)
                doctor_id = lookup_doctor_id(row.get('doctor'), doctor_map)

                if not patient_id and row.get('patient'):
                    missing_patients += 1

                # Parse amounts
                total_amount = float(row.get('total_amount', 0)) if not pd.isna(row.get('total_amount')) else 0.0
                amount_paid = float(row.get('total_payments', 0)) if not pd.isna(row.get('total_payments')) else 0.0
                discount_value = float(row.get('discount_value', 0)) if not pd.isna(row.get('discount_value')) else 0.0

                balance_due = total_amount - amount_paid

                # Map status: "payed" -> "paid"
                status = clean_string(row.get('status'), 50)
                if status and status.lower() == 'payed':
                    status = 'paid'

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    patient_id,
                    doctor_id,
                    parse_date(row.get('invoice_date')),
                    parse_date(row.get('due_date')),
                    status,
                    clean_string(row.get('currency'), 10) or 'USD',
                    clean_string(row.get('discount_type'), 20),
                    discount_value,
                    total_amount,
                    amount_paid,
                    balance_due,
                    clean_string(row.get('notes')),
                    parse_date(row.get('created_at')),
                    datetime.now()
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

                if (idx + 1) % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    progress = (idx + 1) * 100 // total_records
                    print(f"  Progress: {idx + 1}/{total_records} ({progress}%)")

            except Exception as e:
                error_msg = f"Invoice ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE and len(errors) <= 5:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ INVOICE MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print(f"   Missing Patients: {missing_patients}")
        print("-" * 60)

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in invoice migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_invoice_items(mysql_conn, excel_file):
    """Migrate invoice line items from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("INVOICE ITEMS MIGRATION STARTED")
    print("=" * 60)

    try:
        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='invoice_items')

        if Config.TEST_MODE:
            df = df.head(50)

        total_records = len(df)
        print(f"📊 Found {total_records} invoice items in Excel")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO invoice_items (
            source_id, invoice_source_id, description,
            unit_price, quantity, total_amount, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            description = VALUES(description),
            unit_price = VALUES(unit_price),
            quantity = VALUES(quantity),
            total_amount = VALUES(total_amount),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []

        for idx, row in df.iterrows():
            try:
                unit_price = float(row.get('unit_price', 0)) if not pd.isna(row.get('unit_price')) else 0.0
                quantity = int(row.get('quantity', 1)) if not pd.isna(row.get('quantity')) else 1
                total_amount = float(row.get('total_amount', 0)) if not pd.isna(row.get('total_amount')) else 0.0

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    clean_string(str(row.get('invoice_id')), 50),  # invoice_source_id
                    clean_string(row.get('description')),
                    unit_price,
                    quantity,
                    total_amount,
                    datetime.now(),
                    datetime.now()
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

                if (idx + 1) % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    progress = (idx + 1) * 100 // total_records
                    print(f"  Progress: {idx + 1}/{total_records} ({progress}%)")

            except Exception as e:
                error_msg = f"Item ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE and len(errors) <= 5:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ INVOICE ITEMS MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in invoice items migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_payments(mysql_conn, excel_file):
    """Migrate payments from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("PAYMENTS MIGRATION STARTED")
    print("=" * 60)

    try:
        # Create patient lookup map
        patient_map = create_patient_lookup_map(mysql_conn)

        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='Payments')

        if Config.TEST_MODE:
            df = df.head(50)

        total_records = len(df)
        print(f"📊 Found {total_records} payments in Excel")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO payments (
            source_id, invoice_source_id, patient_id, payment_method,
            amount, original_amount, currency, reference_number,
            payment_date, created_at, updated_at, deleted_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            payment_method = VALUES(payment_method),
            amount = VALUES(amount),
            payment_date = VALUES(payment_date),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []

        for idx, row in df.iterrows():
            try:
                # Lookup patient ID
                patient_id = lookup_patient_id(row.get('patient'), patient_map)

                amount = float(row.get('amount', 0)) if not pd.isna(row.get('amount')) else 0.0
                original_amount = float(row.get('original_amount', 0)) if not pd.isna(row.get('original_amount')) else amount

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    clean_string(str(row.get('invoice_id')), 50),  # invoice_source_id
                    patient_id,
                    clean_string(row.get('method'), 50),
                    amount,
                    original_amount,
                    clean_string(row.get('currency'), 10) or 'USD',
                    clean_string(row.get('reference_number'), 100),
                    parse_date(row.get('payment_date')),
                    parse_date(row.get('created_at')),
                    datetime.now(),
                    parse_date(row.get('deleted_at'))
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

                if (idx + 1) % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    progress = (idx + 1) * 100 // total_records
                    print(f"  Progress: {idx + 1}/{total_records} ({progress}%)")

            except Exception as e:
                error_msg = f"Payment ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE and len(errors) <= 5:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ PAYMENTS MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in payments migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_treatments(mysql_conn, excel_file):
    """Migrate treatments/operations from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("TREATMENTS MIGRATION STARTED")
    print("=" * 60)

    try:
        # Create lookup maps
        patient_map = create_patient_lookup_map(mysql_conn)

        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='Operations')

        if Config.TEST_MODE:
            df = df.head(50)

        total_records = len(df)
        print(f"📊 Found {total_records} treatments in Excel")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO treatments (
            source_id, patient_id, tooth_number, procedure_code,
            procedure_name, procedure_group, treatment_plan, status,
            price, planned_date, start_date, completion_date, notes,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            start_date = VALUES(start_date),
            completion_date = VALUES(completion_date),
            notes = VALUES(notes),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []
        missing_patients = 0

        for idx, row in df.iterrows():
            try:
                # Lookup patient ID
                patient_id = lookup_patient_id(row.get('patient'), patient_map)

                if not patient_id:
                    missing_patients += 1

                price = float(row.get('price', 0)) if not pd.isna(row.get('price')) else 0.0

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    patient_id,
                    clean_string(row.get('tooth_nb'), 20),
                    clean_string(row.get('code'), 50),
                    clean_string(row.get('name'), 200),
                    clean_string(row.get('group'), 100),
                    clean_string(row.get('treatment_plan'), 100),
                    clean_string(row.get('status'), 50),
                    price,
                    parse_date(row.get('planned_date')),
                    parse_date(row.get('start_date')),
                    parse_date(row.get('done_date')),
                    clean_string(row.get('note')),
                    datetime.now(),
                    datetime.now()
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

                if (idx + 1) % Config.BATCH_SIZE == 0:
                    mysql_conn.commit()
                    progress = (idx + 1) * 100 // total_records
                    print(f"  Progress: {idx + 1}/{total_records} ({progress}%)")

            except Exception as e:
                error_msg = f"Treatment ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE and len(errors) <= 5:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ TREATMENTS MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print(f"   Missing Patients: {missing_patients}")
        print("-" * 60)

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in treatments migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


def migrate_inventory(mysql_conn, excel_file):
    """Migrate inventory/stock from Excel to MySQL"""
    print("\n" + "=" * 60)
    print("INVENTORY MIGRATION STARTED")
    print("=" * 60)

    try:
        # Read Excel sheet
        df = pd.read_excel(excel_file, sheet_name='stock')

        total_records = len(df)
        print(f"📊 Found {total_records} inventory items in Excel")

        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO inventory (
            source_id, category, name, sku, description,
            unit_of_measure, size, quantity_in_stock, unit_size,
            average_purchase_price, selling_price,
            minimum_quantity_warning, minimum_quantity_critical,
            currency, created_at, updated_at, deleted_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            quantity_in_stock = VALUES(quantity_in_stock),
            average_purchase_price = VALUES(average_purchase_price),
            selling_price = VALUES(selling_price),
            updated_at = VALUES(updated_at)
        """

        inserted, updated, errors = 0, 0, []

        for idx, row in df.iterrows():
            try:
                avg_price = float(row.get('average_purchase_price', 0)) if not pd.isna(row.get('average_purchase_price')) else 0.0
                selling_price = float(row.get('default_selling_price', 0)) if not pd.isna(row.get('default_selling_price')) else None
                quantity = float(row.get('remaining_quantity', 0)) if not pd.isna(row.get('remaining_quantity')) else 0.0
                size = float(row.get('size', 0)) if not pd.isna(row.get('size')) else None
                unit_size = float(row.get('remaining_unit_size', 0)) if not pd.isna(row.get('remaining_unit_size')) else None

                data = (
                    clean_string(str(row.get('id')), 50),  # source_id
                    clean_string(row.get('category'), 100),
                    clean_string(row.get('name'), 200),
                    clean_string(row.get('sku'), 100),
                    clean_string(row.get('description')),
                    clean_string(row.get('unit_of_measure'), 50),
                    size,
                    quantity,
                    unit_size,
                    avg_price,
                    selling_price,
                    int(row.get('minimum_quantity_warning', 0)) if not pd.isna(row.get('minimum_quantity_warning')) else None,
                    int(row.get('minimum_quantity_critical_warning', 0)) if not pd.isna(row.get('minimum_quantity_critical_warning')) else None,
                    clean_string(row.get('default_currency'), 10) or 'USD',
                    parse_date(row.get('created_at')),
                    datetime.now(),
                    parse_date(row.get('deleted_at'))
                )

                cursor.execute(insert_query, data)

                if cursor.rowcount == 1:
                    inserted += 1
                elif cursor.rowcount == 2:
                    updated += 1

            except Exception as e:
                error_msg = f"Inventory ID {row.get('id')}: {str(e)}"
                errors.append(error_msg)
                if Config.DEBUG_MODE:
                    print(f"  ❌ {error_msg}")

        mysql_conn.commit()

        print("\n" + "-" * 60)
        print(f"✅ INVENTORY MIGRATION COMPLETED")
        print(f"   Inserted: {inserted}")
        print(f"   Updated: {updated}")
        print(f"   Errors: {len(errors)}")
        print("-" * 60)

        cursor.close()

    except Exception as e:
        print(f"❌ Critical error in inventory migration: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


# ================================================================
# DATA CLEANUP
# ================================================================

def truncate_all_tables(mysql_conn):
    """Drop all existing data from tables before migration"""
    print("\n" + "=" * 60)
    print("CLEANING EXISTING DATA")
    print("=" * 60)

    try:
        cursor = mysql_conn.cursor()

        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # Tables in reverse order of dependencies
        tables = [
            'invoice_items',
            'payments',
            'invoices',
            'appointments',
            'treatments',
            'patient_relationships',
            'inventory',
            'patients',
            'doctors'
        ]

        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            print(f"🗑️  Cleared table: {table}")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        mysql_conn.commit()
        cursor.close()

        print("✅ All tables cleared successfully")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Error truncating tables: {e}")
        traceback.print_exc()
        mysql_conn.rollback()


# ================================================================
# VERIFICATION
# ================================================================

def verify_migration(mysql_conn):
    """Verify migration results and show statistics"""
    print("\n" + "=" * 60)
    print("MIGRATION VERIFICATION")
    print("=" * 60)

    try:
        cursor = mysql_conn.cursor()

        tables = [
            'patients', 'doctors', 'appointments', 'invoices',
            'invoice_items', 'payments', 'treatments', 'inventory'
        ]

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"✅ {table.ljust(20)}: {count:>6} records")

        cursor.close()

    except Exception as e:
        print(f"❌ Error during verification: {e}")


# ================================================================
# MAIN
# ================================================================

def main():
    """Main migration process"""
    print("\n" + "=" * 60)
    print("🚀 TOOTHPICK EVE - HAMMOUD EXCEL MIGRATION")
    print("=" * 60)
    print(f"Mode: {'TEST' if Config.TEST_MODE else 'PRODUCTION'}")
    print(f"Debug: {'ON' if Config.DEBUG_MODE else 'OFF'}")
    print(f"Batch Size: {Config.BATCH_SIZE}")
    print("=" * 60)

    # Check if Excel file exists
    if not os.path.exists(Config.EXCEL_FILE):
        print(f"\n❌ Excel file not found: {Config.EXCEL_FILE}")
        print("Please ensure the file is in the same directory as this script.")
        return 1

    # Connect to MySQL
    mysql = create_mysql_connection(
        Config.MYSQL_HOST,
        Config.MYSQL_USER,
        Config.MYSQL_PASSWORD,
        Config.MYSQL_DATABASE
    )

    if not mysql:
        print("\n❌ Aborted: Could not establish database connection.")
        return 1

    try:
        # Clear existing data first
        truncate_all_tables(mysql)

        # Migration order is important!
        # 1. Patients first (needed for lookups)
        migrate_patients(mysql, Config.EXCEL_FILE)

        # 2. Doctors second (needed for lookups)
        migrate_doctors(mysql, Config.EXCEL_FILE)

        # 3. Appointments (requires patients and doctors)
        migrate_appointments(mysql, Config.EXCEL_FILE)

        # 4. Invoices (requires patients and doctors)
        migrate_invoices(mysql, Config.EXCEL_FILE)

        # 5. Invoice items (requires invoices)
        migrate_invoice_items(mysql, Config.EXCEL_FILE)

        # 6. Payments (requires invoices)
        migrate_payments(mysql, Config.EXCEL_FILE)

        # 7. Treatments (requires patients)
        migrate_treatments(mysql, Config.EXCEL_FILE)

        # 8. Inventory (standalone)
        migrate_inventory(mysql, Config.EXCEL_FILE)

        # Verify results
        verify_migration(mysql)

        print("\n" + "=" * 60)
        print("✅ MIGRATION COMPLETED SUCCESSFULLY")
        print("=" * 60)

        return 0

    except Exception as e:
        print(f"\n❌ MIGRATION FAILED: {e}")
        traceback.print_exc()
        return 1

    finally:
        if mysql and mysql.is_connected():
            mysql.close()
            print("\n🔒 MySQL connection closed")


if __name__ == "__main__":
    exit_code = main()
    input("\nPress Enter to exit...")
    exit(exit_code)