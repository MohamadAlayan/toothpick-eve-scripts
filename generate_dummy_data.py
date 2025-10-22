# ---------------------------------------------
# ToothpickEVE - Dummy Data Generator
# Purpose: Generate realistic dummy data for clinic analytics testing
#
# Features:
#   - 3000+ patients with realistic demographics
#   - 6 years (2020-2026) of appointment history
#   - Complete appointment, treatment, invoice, and payment records
#   - Realistic relationships between entities
#   - Configurable data generation parameters
# ---------------------------------------------

import mysql.connector
from mysql.connector import Error
from faker import Faker
import random
from datetime import datetime, timedelta
from decimal import Decimal

# Initialize Faker for generating realistic data
fake = Faker()

# Configuration
CONFIG = {
    'HOST': 'localhost',
    'USER': 'root',
    'PASSWORD': 'P@ssw0rd8899',
    'DATABASE_NAME': 'dummy_patient_management_system',

    # Data generation parameters
    'NUM_PATIENTS': 5000,
    'NUM_DOCTORS': 25,
    'NUM_INVENTORY_ITEMS': 200,
    'START_DATE': datetime(2020, 1, 1),
    'END_DATE': datetime(2026, 12, 31),

    # Business rules
    'AVG_APPOINTMENTS_PER_PATIENT': 4,
    'APPOINTMENT_SHOW_RATE': 0.85,  # 85% show up
    'TREATMENT_COMPLETION_RATE': 0.75,  # 75% complete treatments
    'PAYMENT_FULL_RATE': 0.70,  # 70% pay in full immediately
    'PAYMENT_PARTIAL_RATE': 0.20,  # 20% pay partially
}

# Reference data
SPECIALIZATIONS = [
    'General Dentistry', 'Orthodontics', 'Periodontics',
    'Endodontics', 'Prosthodontics', 'Oral Surgery',
    'Pediatric Dentistry', 'Cosmetic Dentistry'
]

DENTAL_PROCEDURES = [
    ('D0120', 'Periodic Oral Evaluation', 'Examination', 50),
    ('D0150', 'Comprehensive Oral Evaluation', 'Examination', 80),
    ('D0210', 'Intraoral X-rays', 'Diagnostic', 40),
    ('D0330', 'Panoramic X-ray', 'Diagnostic', 100),
    ('D1110', 'Prophylaxis - Adult', 'Preventive', 90),
    ('D1120', 'Prophylaxis - Child', 'Preventive', 70),
    ('D1206', 'Topical Fluoride', 'Preventive', 35),
    ('D2140', 'Amalgam - One Surface', 'Restorative', 150),
    ('D2150', 'Amalgam - Two Surfaces', 'Restorative', 180),
    ('D2330', 'Resin - One Surface', 'Restorative', 160),
    ('D2391', 'Resin - One Surface Anterior', 'Restorative', 140),
    ('D2740', 'Crown - Porcelain/Ceramic', 'Restorative', 1200),
    ('D2750', 'Crown - Porcelain Fused to Metal', 'Restorative', 1100),
    ('D2950', 'Core Buildup', 'Restorative', 250),
    ('D3310', 'Root Canal - Anterior', 'Endodontics', 600),
    ('D3320', 'Root Canal - Bicuspid', 'Endodontics', 750),
    ('D3330', 'Root Canal - Molar', 'Endodontics', 950),
    ('D4210', 'Gingivectomy', 'Periodontics', 400),
    ('D4341', 'Periodontal Scaling - Per Quadrant', 'Periodontics', 200),
    ('D5110', 'Complete Denture - Upper', 'Prosthodontics', 1500),
    ('D5120', 'Complete Denture - Lower', 'Prosthodontics', 1500),
    ('D5213', 'Partial Denture - Upper', 'Prosthodontics', 1300),
    ('D6010', 'Implant Placement', 'Oral Surgery', 2000),
    ('D6190', 'Implant/Abutment Crown', 'Prosthodontics', 1800),
    ('D7140', 'Extraction - Single Tooth', 'Oral Surgery', 150),
    ('D7210', 'Extraction - Erupted Tooth', 'Oral Surgery', 200),
    ('D7240', 'Extraction - Impacted Tooth', 'Oral Surgery', 350),
    ('D8080', 'Orthodontic Comprehensive Treatment', 'Orthodontics', 5000),
    ('D9110', 'Palliative Treatment', 'Adjunctive', 50),
    ('D9230', 'Anesthesia/Analgesia', 'Adjunctive', 80),
]

INVENTORY_CATEGORIES = [
    'Anesthetics', 'Antibiotics', 'Dental Materials', 'Instruments',
    'Disposables', 'Sterilization', 'X-Ray Supplies', 'Office Supplies'
]

PAYMENT_METHODS = ['Cash', 'Credit Card', 'Debit Card', 'Check', 'Bank Transfer', 'Insurance']

BLOOD_GROUPS = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']

COMMON_ALLERGIES = [
    'Penicillin', 'Latex', 'Local Anesthetics', 'Aspirin',
    'Ibuprofen', 'Codeine', 'None Known'
]


# Phone number formats by country
def generate_phone_number(country=None):
    """Generate realistic phone numbers for different countries"""
    if country is None:
        country = random.choice(['lebanon', 'egypt', 'usa', 'uae', 'ksa', 'qatar'])

    if country == 'lebanon':
        # Lebanon: +961 XX XXX XXX (mobile) or +961 X XXX XXX (landline)
        prefix = random.choice(['3', '70', '71', '76', '78', '79', '81'])  # Mobile prefixes
        if len(prefix) == 1:
            return f"+961 {prefix} {random.randint(100, 999)} {random.randint(100, 999)}"
        else:
            return f"+961 {prefix} {random.randint(100, 999)} {random.randint(100, 999)}"

    elif country == 'egypt':
        # Egypt: +20 1X XXXX XXXX (mobile)
        operator = random.choice(['10', '11', '12', '15'])  # Vodafone, Etisalat, Orange, WE
        return f"+20 {operator} {random.randint(1000, 9999)} {random.randint(1000, 9999)}"

    elif country == 'usa':
        # USA: +1 (XXX) XXX-XXXX
        area_code = random.randint(200, 999)
        exchange = random.randint(200, 999)
        number = random.randint(1000, 9999)
        return f"+1 ({area_code}) {exchange}-{number}"

    elif country == 'uae':
        # UAE: +971 5X XXX XXXX (mobile)
        operator = random.choice(['50', '52', '54', '55', '56', '58'])
        return f"+971 {operator} {random.randint(100, 999)} {random.randint(1000, 9999)}"

    elif country == 'ksa':
        # Saudi Arabia: +966 5X XXX XXXX (mobile)
        operator = random.choice(['50', '53', '54', '55', '56', '57', '58', '59'])
        return f"+966 {operator} {random.randint(100, 999)} {random.randint(1000, 9999)}"

    elif country == 'qatar':
        # Qatar: +974 XXXX XXXX (mobile)
        prefix = random.choice(['3', '5', '6', '7'])
        return f"+974 {prefix}{random.randint(100, 999)} {random.randint(1000, 9999)}"

    return fake.phone_number()  # Fallback


def create_database_connection():
    """Connect to MySQL server and create/use the dummy database"""
    try:
        connection = mysql.connector.connect(
            host=CONFIG['HOST'],
            user=CONFIG['USER'],
            password=CONFIG['PASSWORD']
        )
        if connection.is_connected():
            print("✅ Connected to MySQL server")

            cursor = connection.cursor()

            # FIX: Set SQL mode to allow all datetime values
            cursor.execute("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'")
            print("✅ SQL mode configured")

            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {CONFIG['DATABASE_NAME']}")
            print(f"📦 Database '{CONFIG['DATABASE_NAME']}' created or already exists")
            cursor.execute(f"USE {CONFIG['DATABASE_NAME']}")
            cursor.close()

            return connection
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


def create_tables(connection):
    """Create all necessary tables (copy from original script)"""
    cursor = connection.cursor()

    tables = {
        'patients': """
            CREATE TABLE IF NOT EXISTS patients (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                first_name VARCHAR(100),
                father_name VARCHAR(100),
                last_name VARCHAR(100),
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
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_source_id (source_id)
            )
        """,
        'patient_relationships': """
            CREATE TABLE IF NOT EXISTS patient_relationships (
                id INT AUTO_INCREMENT PRIMARY KEY,
                patient_id INT,
                related_patient_id INT,
                relationship_type VARCHAR(50),
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL
            )
        """,
        'doctors': """
            CREATE TABLE IF NOT EXISTS doctors (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                title VARCHAR(50),
                first_name VARCHAR(50),
                father_name VARCHAR(100),
                last_name VARCHAR(50),
                specialization VARCHAR(255),
                qualification VARCHAR(200),
                license_number VARCHAR(50),
                phone VARCHAR(100),
                phone_alt VARCHAR(100),
                email VARCHAR(100),
                department VARCHAR(100),
                consultation_fee DECIMAL(10, 2),
                available_days VARCHAR(100),
                available_hours VARCHAR(100),
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_source_id (source_id)
            )
        """,
        'appointments': """
            CREATE TABLE IF NOT EXISTS appointments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                patient_id VARCHAR(50),
                doctor_id VARCHAR(50),
                appointment_date DATE,
                appointment_time TIME,
                duration VARCHAR(50),
                duration_minutes INT,
                revision_number INT,
                room VARCHAR(50),
                status VARCHAR(50),
                missed BOOLEAN DEFAULT FALSE,
                reason_for_visit TEXT,
                diagnosis TEXT,
                prescription TEXT,
                notes TEXT,
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_patient_id (patient_id),
                INDEX idx_doctor_id (doctor_id),
                INDEX idx_appointment_date (appointment_date)
            )
        """,
        'invoices': """
            CREATE TABLE IF NOT EXISTS invoices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                invoice_number VARCHAR(50),
                patient_id VARCHAR(50),
                doctor_id VARCHAR(50),
                appointment_id VARCHAR(50),
                invoice_date DATE,
                due_date DATE,
                status VARCHAR(50),
                currency VARCHAR(10) DEFAULT 'USD',
                subtotal DECIMAL(10, 2) DEFAULT 0.00,
                discount_type VARCHAR(20),
                discount_value DECIMAL(10, 2) DEFAULT 0.00,
                tax DECIMAL(10, 2) DEFAULT 0.00,
                total_amount DECIMAL(10, 2),
                amount_paid DECIMAL(10, 2) DEFAULT 0.00,
                balance_due DECIMAL(10, 2),
                notes TEXT,
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_source_id (source_id),
                INDEX idx_patient_id (patient_id)
            )
        """,
        'invoice_items': """
            CREATE TABLE IF NOT EXISTS invoice_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50),
                invoice_id INT,
                invoice_source_id VARCHAR(50),
                description TEXT,
                unit_price DECIMAL(10, 2),
                quantity INT DEFAULT 1,
                total_amount DECIMAL(10, 2),
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_invoice_id (invoice_id)
            )
        """,
        'payments': """
            CREATE TABLE IF NOT EXISTS payments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                invoice_id INT,
                invoice_source_id VARCHAR(50),
                patient_id VARCHAR(50),
                payment_method VARCHAR(50),
                amount DECIMAL(10, 2),
                original_amount DECIMAL(10, 2),
                currency VARCHAR(10),
                reference_number VARCHAR(100),
                payment_date DATE,
                notes TEXT,
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                deleted_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_invoice_id (invoice_id),
                INDEX idx_patient_id (patient_id)
            )
        """,
        'treatments': """
            CREATE TABLE IF NOT EXISTS treatments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                patient_id VARCHAR(50),
                doctor_id VARCHAR(50),
                tooth_number VARCHAR(20),
                procedure_code VARCHAR(50),
                procedure_name VARCHAR(200),
                procedure_group VARCHAR(100),
                treatment_plan VARCHAR(100),
                status VARCHAR(50),
                price DECIMAL(10, 2),
                planned_date DATE,
                start_date DATE,
                completion_date DATE,
                notes TEXT,
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_patient_id (patient_id),
                INDEX idx_doctor_id (doctor_id)
            )
        """,
        'inventory': """
            CREATE TABLE IF NOT EXISTS inventory (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_id VARCHAR(50) UNIQUE,
                category VARCHAR(100),
                name VARCHAR(200),
                sku VARCHAR(50),
                description TEXT,
                unit_of_measure VARCHAR(50),
                size DECIMAL(10, 2),
                quantity_in_stock DECIMAL(10, 2),
                unit_size DECIMAL(10, 2),
                average_purchase_price DECIMAL(10, 2),
                selling_price DECIMAL(10, 2),
                minimum_quantity_warning INT,
                minimum_quantity_critical INT,
                currency VARCHAR(10),
                created_at TIMESTAMP NULL DEFAULT NULL,
                updated_at TIMESTAMP NULL DEFAULT NULL,
                deleted_at TIMESTAMP NULL DEFAULT NULL,
                INDEX idx_source_id (source_id)
            )
        """
    }

    for table_name, create_statement in tables.items():
        cursor.execute(create_statement)
        print(f"🧱 Table '{table_name}' created")

    connection.commit()
    cursor.close()
    print("✅ All tables created successfully!\n")


def truncate_tables(connection):
    """Truncate all tables to remove old data before generating new data"""
    cursor = connection.cursor()

    print("🗑️  Truncating existing data...")

    # List of tables in reverse order to avoid foreign key issues (even though we don't have FKs)
    tables = [
        'payments',
        'invoice_items',
        'invoices',
        'treatments',
        'appointments',
        'patient_relationships',
        'inventory',
        'patients',
        'doctors'
    ]

    try:
        # Disable foreign key checks temporarily (just in case)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            print(f"   ✓ Truncated '{table}'")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        connection.commit()
        cursor.close()
        print("✅ All tables truncated successfully!\n")
    except Error as e:
        print(f"⚠️  Warning during truncation: {e}")
        print("   Continuing anyway...\n")


def random_date_between(start, end):
    """Generate random date between two dates"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def generate_doctors(connection, num_doctors):
    """Generate dummy doctor records"""
    cursor = connection.cursor()
    doctors = []

    print(f"👨‍⚕️ Generating {num_doctors} doctors...")

    for i in range(1, num_doctors + 1):
        specialization = random.choice(SPECIALIZATIONS)

        doctor = {
            'source_id': f'DOC{i:04d}',
            'title': random.choice(['Dr.', 'Prof.', 'Dr.']),
            'first_name': fake.first_name(),
            'father_name': fake.first_name(),
            'last_name': fake.last_name(),
            'specialization': specialization,
            'qualification': random.choice(['DDS', 'DMD', 'BDS, MDS', 'DDS, PhD']),
            'license_number': f'LIC{random.randint(10000, 99999)}',
            'phone': generate_phone_number('lebanon'),  # Doctors mostly have Lebanese numbers
            'phone_alt': generate_phone_number('lebanon') if random.random() > 0.5 else None,
            'email': fake.email(),
            'department': 'Dentistry',
            'consultation_fee': round(random.uniform(50, 150), 2),
            'available_days': 'Mon-Fri',
            'available_hours': '9:00-17:00',
            'created_at': CONFIG['START_DATE'],
            'updated_at': datetime.now()
        }

        doctors.append(doctor)

        query = """
            INSERT INTO doctors (
                source_id, title, first_name, father_name, last_name, specialization,
                qualification, license_number, phone, phone_alt, email, department,
                consultation_fee, available_days, available_hours, created_at, updated_at
            ) VALUES (
                %(source_id)s, %(title)s, %(first_name)s, %(father_name)s, %(last_name)s, %(specialization)s,
                %(qualification)s, %(license_number)s, %(phone)s, %(phone_alt)s, %(email)s, %(department)s,
                %(consultation_fee)s, %(available_days)s, %(available_hours)s, %(created_at)s, %(updated_at)s
            )
        """

        cursor.execute(query, doctor)

    connection.commit()
    cursor.close()
    print(f"✅ {num_doctors} doctors created\n")

    return doctors


def generate_patients(connection, num_patients):
    """Generate dummy patient records"""
    cursor = connection.cursor()
    patients = []

    print(f"👥 Generating {num_patients} patients...")

    for i in range(1, num_patients + 1):
        gender = random.choice(['male', 'female', 'male', 'female', 'male', 'female'])  # 95% male/female, 5% other/unknown
        if random.random() > 0.95:
            gender = random.choice(['other', 'unknown'])

        dob = fake.date_of_birth(minimum_age=1, maximum_age=90)
        created_date = random_date_between(CONFIG['START_DATE'], CONFIG['END_DATE'])

        patient = {
            'source_id': f'PAT{i:06d}',
            'first_name': fake.first_name_male() if gender == 'male' else fake.first_name_female() if gender == 'female' else fake.first_name(),
            'father_name': fake.first_name_male(),
            'last_name': fake.last_name(),
            'mother_name': fake.first_name_female(),
            'id_nb': f'ID{random.randint(100000000, 999999999)}',
            'date_of_birth': dob,
            'gender': gender,
            'marital_status': random.choice(['single', 'married', 'divorced', 'widowed']),
            'nationality': 'Lebanese',
            'phone': generate_phone_number(),
            'phone_alt': generate_phone_number() if random.random() > 0.7 else None,
            'email': fake.email() if random.random() > 0.3 else None,
            'address_line1': fake.street_address(),
            'address_line2': fake.secondary_address() if random.random() > 0.8 else None,
            'city': fake.city(),
            'state': random.choice(['Beirut', 'Mount Lebanon', 'North', 'South', 'Bekaa']),
            'zip_code': fake.zipcode(),
            'country': 'Lebanon',
            'blood_group': random.choice(BLOOD_GROUPS),
            'allergies': ', '.join(random.sample(COMMON_ALLERGIES, random.randint(0, 3))),
            'medical_history': fake.text(max_nb_chars=200) if random.random() > 0.6 else None,
            'created_at': created_date,
            'updated_at': datetime.now()
        }

        patients.append(patient)

        query = """
            INSERT INTO patients (
                source_id, first_name, father_name, last_name, mother_name, id_nb,
                date_of_birth, gender, marital_status, nationality, phone, phone_alt,
                email, address_line1, address_line2, city, state, zip_code, country,
                blood_group, allergies, medical_history, created_at, updated_at
            ) VALUES (
                %(source_id)s, %(first_name)s, %(father_name)s, %(last_name)s, %(mother_name)s, %(id_nb)s,
                %(date_of_birth)s, %(gender)s, %(marital_status)s, %(nationality)s, %(phone)s, %(phone_alt)s,
                %(email)s, %(address_line1)s, %(address_line2)s, %(city)s, %(state)s, %(zip_code)s, %(country)s,
                %(blood_group)s, %(allergies)s, %(medical_history)s, %(created_at)s, %(updated_at)s
            )
        """

        cursor.execute(query, patient)

        if i % 500 == 0:
            print(f"   Created {i} patients...")

    connection.commit()
    cursor.close()
    print(f"✅ {num_patients} patients created\n")

    return patients


def generate_appointments(connection, patients, doctors):
    """Generate dummy appointment records"""
    cursor = connection.cursor()
    appointments = []
    appointment_id = 1

    print(f"📅 Generating appointments...")

    for patient in patients:
        # Each patient gets 2-6 appointments
        num_appointments = random.randint(2, 6)

        patient_created = patient['created_at']

        for _ in range(num_appointments):
            # Random date between patient creation and END_DATE (now includes 2026)
            appointment_date = random_date_between(patient_created, CONFIG['END_DATE'])

            # Random time during work hours
            hour = random.randint(9, 16)
            minute = random.choice([0, 15, 30, 45])
            appointment_time = f"{hour:02d}:{minute:02d}:00"

            # Status based on date
            if appointment_date > datetime.now():
                status = random.choice(['scheduled', 'confirmed', 'pending'])
                missed = False
            else:
                if random.random() < CONFIG['APPOINTMENT_SHOW_RATE']:
                    status = random.choice(['completed', 'completed', 'completed', 'attended', 'checked_in'])
                    missed = False
                else:
                    status = random.choice(['no_show', 'missed', 'cancelled'])
                    missed = True

            doctor = random.choice(doctors)

            appointment = {
                'source_id': f'APT{appointment_id:08d}',
                'patient_id': patient['source_id'],
                'doctor_id': doctor['source_id'],
                'appointment_date': appointment_date.date(),
                'appointment_time': appointment_time,
                'duration': random.choice(['30 min', '45 min', '60 min', '90 min']),
                'duration_minutes': random.choice([30, 45, 60, 90]),
                'revision_number': 0,
                'room': f'Room {random.randint(1, 10)}',
                'status': status,
                'missed': missed,
                'reason_for_visit': random.choice(['Checkup', 'Cleaning', 'Filling', 'Crown', 'Extraction', 'Consultation']),
                'diagnosis': fake.text(max_nb_chars=100) if status == 'completed' and random.random() > 0.5 else None,
                'prescription': fake.text(max_nb_chars=100) if status == 'completed' and random.random() > 0.7 else None,
                'notes': fake.text(max_nb_chars=150) if random.random() > 0.7 else None,
                'created_at': appointment_date,
                'updated_at': datetime.now()
            }

            appointments.append(appointment)

            query = """
                INSERT INTO appointments (
                    source_id, patient_id, doctor_id, appointment_date, appointment_time,
                    duration, duration_minutes, revision_number, room, status, missed,
                    reason_for_visit, diagnosis, prescription, notes, created_at, updated_at
                ) VALUES (
                    %(source_id)s, %(patient_id)s, %(doctor_id)s, %(appointment_date)s, %(appointment_time)s,
                    %(duration)s, %(duration_minutes)s, %(revision_number)s, %(room)s, %(status)s, %(missed)s,
                    %(reason_for_visit)s, %(diagnosis)s, %(prescription)s, %(notes)s, %(created_at)s, %(updated_at)s
                )
            """

            cursor.execute(query, appointment)
            appointment_id += 1

        if appointment_id % 1000 == 0:
            print(f"   Created {appointment_id} appointments...")

    connection.commit()
    cursor.close()
    print(f"✅ {len(appointments)} appointments created\n")

    return appointments


def generate_treatments(connection, appointments, patients, doctors):
    """Generate dummy treatment records"""
    cursor = connection.cursor()
    treatments = []
    treatment_id = 1

    print(f"💉 Generating treatments...")

    completed_appointments = [apt for apt in appointments if apt['status'] == 'completed']

    for appointment in completed_appointments:
        # Each completed appointment gets 1-3 treatments
        num_treatments = random.randint(1, 3)

        for _ in range(num_treatments):
            procedure = random.choice(DENTAL_PROCEDURES)

            treatment = {
                'source_id': f'TRT{treatment_id:08d}',
                'patient_id': appointment['patient_id'],
                'doctor_id': appointment['doctor_id'],
                'tooth_number': str(random.randint(1, 32)) if random.random() > 0.3 else None,
                'procedure_code': procedure[0],
                'procedure_name': procedure[1],
                'procedure_group': procedure[2],
                'treatment_plan': random.choice(['Standard', 'Comprehensive', 'Emergency', 'Cosmetic']),
                'status': 'completed' if random.random() < CONFIG['TREATMENT_COMPLETION_RATE'] else 'in_progress',
                'price': round(procedure[3] * random.uniform(0.9, 1.1), 2),
                'planned_date': appointment['appointment_date'] - timedelta(days=random.randint(1, 30)),
                'start_date': appointment['appointment_date'],
                'completion_date': appointment['appointment_date'] if random.random() < 0.8 else None,
                'notes': fake.text(max_nb_chars=150) if random.random() > 0.7 else None,
                'created_at': appointment['created_at'],
                'updated_at': datetime.now()
            }

            treatments.append(treatment)

            query = """
                INSERT INTO treatments (
                    source_id, patient_id, doctor_id, tooth_number, procedure_code,
                    procedure_name, procedure_group, treatment_plan, status, price,
                    planned_date, start_date, completion_date, notes, created_at, updated_at
                ) VALUES (
                    %(source_id)s, %(patient_id)s, %(doctor_id)s, %(tooth_number)s, %(procedure_code)s,
                    %(procedure_name)s, %(procedure_group)s, %(treatment_plan)s, %(status)s, %(price)s,
                    %(planned_date)s, %(start_date)s, %(completion_date)s, %(notes)s, %(created_at)s, %(updated_at)s
                )
            """

            cursor.execute(query, treatment)
            treatment_id += 1

        if treatment_id % 1000 == 0:
            print(f"   Created {treatment_id} treatments...")

    connection.commit()
    cursor.close()
    print(f"✅ {len(treatments)} treatments created\n")

    return treatments


def generate_invoices_and_payments(connection, appointments, treatments):
    """Generate dummy invoice, invoice item, and payment records"""
    cursor = connection.cursor()
    invoices = []
    invoice_items = []
    payments = []
    invoice_id = 1
    invoice_item_id = 1
    payment_id = 1

    print(f"💰 Generating invoices, items, and payments...")

    # Since treatments don't have appointment_id anymore, we'll create invoices based on appointments
    # and generate treatments within this function or link them by date and patient

    # Group treatments by patient_id and date for easier lookup
    treatments_by_patient_date = {}
    for treatment in treatments:
        key = (treatment['patient_id'], treatment['start_date'])
        if key not in treatments_by_patient_date:
            treatments_by_patient_date[key] = []
        treatments_by_patient_date[key].append(treatment)

    for appointment in appointments:
        if appointment['status'] != 'completed':
            continue

        # Find treatments for this appointment by patient and date
        key = (appointment['patient_id'], appointment['appointment_date'])
        apt_treatments = treatments_by_patient_date.get(key, [])

        if not apt_treatments:
            continue

        # Calculate total
        total_amount = sum(t['price'] for t in apt_treatments)
        subtotal = total_amount

        # Random discount
        discount_type = None
        discount_value = 0
        if random.random() > 0.8:  # 20% chance of discount
            discount_type = random.choice(['Percentage', 'Fixed'])
            if discount_type == 'Percentage':
                discount_value = random.choice([5, 10, 15, 20])
                total_amount = total_amount * (1 - discount_value / 100)
            else:
                discount_value = round(random.uniform(10, 50), 2)
                total_amount = max(0, total_amount - discount_value)

        # Tax (5% VAT for example)
        tax = round(total_amount * 0.05, 2)
        total_amount = round(total_amount + tax, 2)

        # Determine payment status
        rand = random.random()
        if rand < CONFIG['PAYMENT_FULL_RATE']:
            amount_paid = total_amount
            status = 'paid'
        elif rand < CONFIG['PAYMENT_FULL_RATE'] + CONFIG['PAYMENT_PARTIAL_RATE']:
            amount_paid = total_amount * random.uniform(0.3, 0.7)
            status = 'partially_paid'
        else:
            amount_paid = 0
            status = 'unpaid'

        balance_due = total_amount - amount_paid

        invoice = {
            'source_id': f'INV{invoice_id:08d}',
            'invoice_number': f'INV-{invoice_id:08d}',
            'patient_id': appointment['patient_id'],
            'doctor_id': appointment['doctor_id'],
            'appointment_id': appointment['source_id'],
            'invoice_date': appointment['appointment_date'],
            'due_date': appointment['appointment_date'] + timedelta(days=30),
            'status': status,
            'currency': 'USD',
            'subtotal': round(subtotal, 2),
            'discount_type': discount_type,
            'discount_value': round(discount_value, 2) if discount_type else 0,
            'tax': tax,
            'total_amount': round(total_amount, 2),
            'amount_paid': round(amount_paid, 2),
            'balance_due': round(balance_due, 2),
            'notes': fake.text(max_nb_chars=100) if random.random() > 0.8 else None,
            'created_at': appointment['created_at'],
            'updated_at': datetime.now()
        }

        invoices.append(invoice)

        query = """
            INSERT INTO invoices (
                source_id, invoice_number, patient_id, doctor_id, appointment_id, invoice_date, due_date,
                status, currency, subtotal, discount_type, discount_value, tax,
                total_amount, amount_paid, balance_due, notes, created_at, updated_at
            ) VALUES (
                %(source_id)s, %(invoice_number)s, %(patient_id)s, %(doctor_id)s, %(appointment_id)s, %(invoice_date)s, %(due_date)s,
                %(status)s, %(currency)s, %(subtotal)s, %(discount_type)s, %(discount_value)s, %(tax)s,
                %(total_amount)s, %(amount_paid)s, %(balance_due)s, %(notes)s, %(created_at)s, %(updated_at)s
            )
        """

        cursor.execute(query, invoice)
        db_invoice_id = cursor.lastrowid

        # Create invoice items
        for treatment in apt_treatments:
            item = {
                'source_id': f'INVITM{invoice_item_id:08d}',
                'invoice_id': db_invoice_id,
                'invoice_source_id': invoice['source_id'],
                'description': treatment['procedure_name'],
                'unit_price': treatment['price'],
                'quantity': 1,
                'total_amount': treatment['price'],
                'created_at': invoice['created_at'],
                'updated_at': datetime.now()
            }

            invoice_items.append(item)

            query = """
                INSERT INTO invoice_items (
                    source_id, invoice_id, invoice_source_id, description,
                    unit_price, quantity, total_amount, created_at, updated_at
                ) VALUES (
                    %(source_id)s, %(invoice_id)s, %(invoice_source_id)s, %(description)s,
                    %(unit_price)s, %(quantity)s, %(total_amount)s, %(created_at)s, %(updated_at)s
                )
            """

            cursor.execute(query, item)
            invoice_item_id += 1

        # Create payments
        if amount_paid > 0:
            # Randomly split into 1-3 payments
            if status == 'paid':
                num_payments = 1 if random.random() > 0.2 else random.randint(2, 3)
                remaining = amount_paid

                for i in range(num_payments):
                    if i == num_payments - 1:
                        payment_amount = remaining
                    else:
                        payment_amount = remaining * random.uniform(0.3, 0.6)
                        remaining -= payment_amount

                    payment_date = invoice['invoice_date'] + timedelta(days=random.randint(0, 30))

                    payment = {
                        'source_id': f'PAY{payment_id:08d}',
                        'invoice_id': db_invoice_id,
                        'invoice_source_id': invoice['source_id'],
                        'patient_id': appointment['patient_id'],
                        'payment_method': random.choice(PAYMENT_METHODS),
                        'amount': round(payment_amount, 2),
                        'original_amount': round(payment_amount, 2),
                        'currency': 'USD',
                        'reference_number': f'REF{random.randint(100000, 999999)}',
                        'payment_date': payment_date,
                        'notes': fake.text(max_nb_chars=100) if random.random() > 0.8 else None,
                        'created_at': payment_date,
                        'updated_at': datetime.now(),
                        'deleted_at': None
                    }

                    payments.append(payment)

                    query = """
                        INSERT INTO payments (
                            source_id, invoice_id, invoice_source_id, patient_id, payment_method,
                            amount, original_amount, currency, reference_number, payment_date,
                            notes, created_at, updated_at, deleted_at
                        ) VALUES (
                            %(source_id)s, %(invoice_id)s, %(invoice_source_id)s, %(patient_id)s, %(payment_method)s,
                            %(amount)s, %(original_amount)s, %(currency)s, %(reference_number)s, %(payment_date)s,
                            %(notes)s, %(created_at)s, %(updated_at)s, %(deleted_at)s
                        )
                    """

                    cursor.execute(query, payment)
                    payment_id += 1

        invoice_id += 1

        if invoice_id % 500 == 0:
            print(f"   Created {invoice_id} invoices...")

    connection.commit()
    cursor.close()
    print(f"✅ {len(invoices)} invoices, {len(invoice_items)} items, {len(payments)} payments created\n")

    return invoices, invoice_items, payments


def generate_inventory(connection, num_items):
    """Generate dummy inventory records"""
    cursor = connection.cursor()
    inventory = []

    print(f"📦 Generating {num_items} inventory items...")

    for i in range(1, num_items + 1):
        category = random.choice(INVENTORY_CATEGORIES)

        item = {
            'source_id': f'INV{i:06d}',
            'category': category,
            'name': f"{category} Item {i}",
            'sku': f'SKU{random.randint(10000, 99999)}',
            'description': fake.text(max_nb_chars=150),
            'unit_of_measure': random.choice(['Unit', 'Box', 'Pack', 'Bottle', 'Vial']),
            'size': round(random.uniform(1, 100), 2),
            'quantity_in_stock': round(random.uniform(10, 500), 2),
            'unit_size': round(random.uniform(1, 50), 2),
            'average_purchase_price': round(random.uniform(5, 200), 2),
            'selling_price': round(random.uniform(10, 300), 2),
            'minimum_quantity_warning': random.randint(10, 50),
            'minimum_quantity_critical': random.randint(5, 20),
            'currency': 'USD',
            'created_at': random_date_between(CONFIG['START_DATE'], CONFIG['END_DATE']),
            'updated_at': datetime.now(),
            'deleted_at': None
        }

        inventory.append(item)

        query = """
            INSERT INTO inventory (
                source_id, category, name, sku, description, unit_of_measure,
                size, quantity_in_stock, unit_size, average_purchase_price,
                selling_price, minimum_quantity_warning, minimum_quantity_critical,
                currency, created_at, updated_at, deleted_at
            ) VALUES (
                %(source_id)s, %(category)s, %(name)s, %(sku)s, %(description)s, %(unit_of_measure)s,
                %(size)s, %(quantity_in_stock)s, %(unit_size)s, %(average_purchase_price)s,
                %(selling_price)s, %(minimum_quantity_warning)s, %(minimum_quantity_critical)s,
                %(currency)s, %(created_at)s, %(updated_at)s, %(deleted_at)s
            )
        """

        cursor.execute(query, item)

    connection.commit()
    cursor.close()
    print(f"✅ {num_items} inventory items created\n")

    return inventory


def generate_statistics(connection):
    """Print statistics about generated data"""
    cursor = connection.cursor()

    print("\n" + "=" * 60)
    print("📊 DATA GENERATION SUMMARY")
    print("=" * 60)

    tables = ['patients', 'doctors', 'appointments', 'treatments',
              'invoices', 'invoice_items', 'payments', 'inventory']

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table.capitalize():20}: {count:,} records")

    # Additional statistics
    cursor.execute("SELECT COUNT(*) FROM appointments WHERE status = 'completed'")
    completed_appts = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM appointments WHERE missed = TRUE")
    missed_appts = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM appointments WHERE appointment_date > CURDATE()")
    future_appts = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(total_amount) FROM invoices")
    total_revenue = cursor.fetchone()[0] or 0

    cursor.execute("SELECT SUM(amount_paid) FROM invoices")
    total_collected = cursor.fetchone()[0] or 0

    cursor.execute("SELECT SUM(balance_due) FROM invoices")
    total_outstanding = cursor.fetchone()[0] or 0

    print("\n" + "-" * 60)
    print("📈 BUSINESS METRICS")
    print("-" * 60)
    print(f"   Completed Appointments: {completed_appts:,}")
    print(f"   Future Appointments (2026): {future_appts:,}")
    print(f"   Missed Appointments: {missed_appts:,}")
    print(f"   Total Revenue: ${total_revenue:,.2f}")
    print(f"   Total Collected: ${total_collected:,.2f}")
    print(f"   Outstanding Balance: ${total_outstanding:,.2f}")
    if total_revenue > 0:
        print(f"   Collection Rate: {(total_collected / total_revenue * 100):.1f}%")
    print("=" * 60 + "\n")

    cursor.close()


def main():
    """Main function to generate all dummy data"""
    print("\n" + "=" * 60)
    print("🚀 TOOTHPICK EVE - DUMMY DATA GENERATOR")
    print("=" * 60)
    print(f"Target: {CONFIG['NUM_PATIENTS']:,} patients over 7 years")
    print(f"Doctors: {CONFIG['NUM_DOCTORS']} doctors")
    print(f"Period: {CONFIG['START_DATE'].strftime('%Y-%m-%d')} to {CONFIG['END_DATE'].strftime('%Y-%m-%d')}")
    print("=" * 60 + "\n")

    # Connect to database
    connection = create_database_connection()

    if not connection:
        print("❌ Failed to connect to database. Exiting.")
        return

    try:
        # Create tables
        create_tables(connection)

        # Truncate existing data
        truncate_tables(connection)

        # Generate data in proper order
        doctors = generate_doctors(connection, CONFIG['NUM_DOCTORS'])
        patients = generate_patients(connection, CONFIG['NUM_PATIENTS'])
        appointments = generate_appointments(connection, patients, doctors)
        treatments = generate_treatments(connection, appointments, patients, doctors)
        invoices, invoice_items, payments = generate_invoices_and_payments(connection, appointments, treatments)
        inventory = generate_inventory(connection, CONFIG['NUM_INVENTORY_ITEMS'])

        # Print statistics
        generate_statistics(connection)

        print("✅ ALL DUMMY DATA GENERATED SUCCESSFULLY!")
        print(f"📍 Database: {CONFIG['DATABASE_NAME']}")

    except Error as e:
        print(f"\n❌ An error occurred: {e}")

    finally:
        if connection.is_connected():
            connection.close()
            print("🔒 MySQL connection closed\n")


if __name__ == "__main__":
    main()