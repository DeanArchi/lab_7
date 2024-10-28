import psycopg2
from prettytable import PrettyTable

# to run containers: docker-compose start
# to stop containers: docker-compose stop


def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS students (
            student_id SERIAL PRIMARY KEY,
            last_name VARCHAR(50),
            first_name VARCHAR(50),
            middle_name VARCHAR(50),
            address VARCHAR(255),
            phone VARCHAR(15) CHECK (phone ~ '^[0-9]{10}$'),
            course INT CHECK (course BETWEEN 1 AND 4),
            faculty VARCHAR(50) CHECK (faculty IN ('Agrarian management', 'Economics', 'IT')),
            group_name VARCHAR(50),
            is_leader BOOLEAN
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Subjects (
            subject_id SERIAL PRIMARY KEY,
            subject_name VARCHAR(100),
            hours_per_semester INT,
            semesters_count INT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Exams (
            exam_id SERIAL PRIMARY KEY,
            exam_date DATE,
            student_id INT REFERENCES Students(student_id),
            subject_id INT REFERENCES Subjects(subject_id),
            grade INT CHECK (grade BETWEEN 2 AND 5)
        );
    ''')

    conn.commit()
    cursor.close()


def insert_data(conn):
    cursor = conn.cursor()
    cursor.execute("""
        TRUNCATE TABLE students CASCADE;
        TRUNCATE TABLE Subjects CASCADE;
        TRUNCATE TABLE Exams CASCADE;
    """)

    students = [
        (1, 'Іванов', 'Іван', 'Іванович', 'Київ', '1234567890', 2, 'Agrarian management', 'Група 1', True),
        (2, 'Петров', 'Петро', 'Петрович', 'Львів', '0987654321', 1, 'Economics', 'Група 2', False),
        (3, 'Сидоров', 'Сидор', 'Сидорович', 'Одеса', '1122334455', 3, 'IT', 'Група 1', True),
        (4, 'Коваленко', 'Сергій', 'Коваленко', 'Харків', '2233445566', 4, 'Agrarian management', 'Група 3', False),
        (5, 'Мельник', 'Ольга', 'Мельник', 'Запоріжжя', '3344556677', 2, 'Economics', 'Група 1', True),
        (6, 'Ткаченко', 'Марія', 'Ткаченко', 'Дніпро', '4455667788', 1, 'IT', 'Група 2', False),
        (7, 'Левченко', 'Віктор', 'Левченко', 'Кривий Ріг', '5566778899', 3, 'Agrarian management', 'Група 3', True),
        (8, 'Насонов', 'Антон', 'Насонович', 'Миколаїв', '6677889900', 4, 'Economics', 'Група 2', False),
        (9, 'Семенко', 'Семен', 'Семенович', 'Суми', '7788990011', 1, 'IT', 'Група 1', True),
        (10, 'Даниленко', 'Аліна', 'Даниленко', 'Чернівці', '8899001122', 2, 'Agrarian management', 'Група 3', False),
        (11, 'Гнатенко', 'Владислав', 'Гнатенко', 'Тернопіль', '9900112233', 3, 'Economics', 'Група 2', True),
    ]

    # Додавання студентів
    for student in students:
        cursor.execute("""
            INSERT INTO students (student_id, last_name, first_name, middle_name, address, phone, course, faculty, group_name, is_leader) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, student)

    # Додавання предметів
    subjects = [
        (1, 'Mathematics', 30, 4),
        (2, 'Physics', 40, 2),
        (3, 'Programming', 90, 4),
    ]

    for subject in subjects:
        cursor.execute("""
            INSERT INTO Subjects (subject_id, subject_name, hours_per_semester, semesters_count)
            VALUES (%s, %s, %s, %s);
        """, subject)

    # Додавання складань іспитів
    exams = [
        ('2024-06-15', 1, 1, 4),
        ('2024-06-20', 2, 1, 3),
        ('2024-06-25', 3, 2, 5),
        ('2024-06-30', 4, 3, 2),
        ('2024-07-05', 5, 1, 4),
        ('2024-07-10', 6, 2, 3),
        ('2024-07-15', 7, 3, 5),
        ('2024-07-20', 8, 1, 4),
        ('2024-07-25', 9, 2, 2),
        ('2024-07-30', 10, 3, 3),
        ('2024-08-01', 11, 1, 4),
    ]

    for exam in exams:
        cursor.execute(
            "INSERT INTO exams (exam_date, student_id, subject_id, grade) VALUES (%s, %s, %s, %s)",
            exam
        )

    conn.commit()
    cursor.close()


def get_students_leaders(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT last_name, first_name, middle_name
        FROM Students
        WHERE is_leader = TRUE
        ORDER BY last_name ASC;
    """)

    results = cursor.fetchall()
    cursor.close()
    return results


def get_average_grades(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT s.last_name, s.first_name, AVG(e.grade) AS average_grade
        FROM students s
        JOIN exams e ON s.student_id = e.student_id
        GROUP BY s.student_id
        ORDER BY s.last_name ASC;
    """)

    results = cursor.fetchall()
    cursor.close()
    return results


def get_total_hours_per_subject(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT sub.subject_name, SUM(sub.hours_per_semester * sub.semesters_count) AS total_hours
        FROM subjects sub
        GROUP BY sub.subject_id
        ORDER BY sub.subject_name ASC;
    """)

    results = cursor.fetchall()
    cursor.close()
    return results


def get_student_performance_for_subject(conn, subject_id):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT s.last_name, s.first_name, e.grade
        FROM students s
        JOIN exams e ON s.student_id = e.student_id
        WHERE e.subject_id = %s;
    """, (subject_id,))

    results = cursor.fetchall()

    cursor.execute("""
        SELECT subject_name
        FROM Subjects
        WHERE subject_id = %s;
    """, (subject_id,))

    sub_name = cursor.fetchone()
    cursor.close()
    return results, sub_name[0]


def get_student_count_per_faculty(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT faculty, COUNT(*) AS student_count
        FROM students
        GROUP BY faculty
        ORDER BY faculty ASC;
    """)

    results = cursor.fetchall()
    cursor.close()
    return results


def get_grades_for_each_student(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT s.last_name, s.first_name, sub.subject_name AS subject_name, e.grade
        FROM students s
        CROSS JOIN subjects sub
        LEFT JOIN exams e ON s.student_id = e.student_id AND sub.subject_id = e.subject_id
        ORDER BY s.last_name, sub.subject_name;
    """)

    results = cursor.fetchall()
    cursor.close()
    return results


def show_table_structure_and_data(conn, table_name):
    cursor = conn.cursor()

    # Отримати структуру таблиці
    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
    columns = cursor.fetchall()

    # Отримати дані з таблиці
    cursor.execute(f"SELECT * FROM {table_name};")
    data = cursor.fetchall()

    # Створити таблицю для виводу
    x = PrettyTable()
    x.field_names = [col[0] for col in columns]

    for row in data:
        x.add_row(row)

    print(f"Структура таблиці: {table_name}")
    for col in columns:
        print(f"{col[0]}: {col[1]}")

    print(f"\nДані таблиці: {table_name}")
    print(x)

    cursor.close()


try:
    connection = psycopg2.connect(
        dbname='university',
        user='postgres',
        password='password',
        host='localhost',
        port='5433'
    )
    print('Connection established to DB!')

    # create_tables(connection)
    # print('Tables created!')
    #
    # insert_data(connection)
    # print('Data inserted into tables!')

    # Показати структуру та дані всіх таблиць
    show_table_structure_and_data(connection, "students")
    show_table_structure_and_data(connection, "subjects")
    show_table_structure_and_data(connection, "exams")

    # 1. Відобразити всіх студентів, які є старостами, відсортувати прізвища за алфавітом
    leaders = get_students_leaders(connection)
    print('Старости:')
    for last_name, first_name, middle_name in leaders:
        print(f'{last_name} {first_name} {middle_name}')

    # 2. Порахувати середній бал для кожного студента
    average_grades = get_average_grades(connection)
    print('\nСередній бал студентів:')
    for last_name, first_name, avg_grade in average_grades:
        print(f'{last_name} {first_name}: {avg_grade:.2f}')

    # 3. Для кожного предмета порахувати загальну кількість годин, протягом яких він вивчається
    total_hours = get_total_hours_per_subject(connection)
    print('\nЗагальна кількість годин по предметах:')
    for subject_name, hours in total_hours:
        print(f'{subject_name}: {hours} годин')

    # 4. Відобразити успішність студентів по обраному предмету
    subject_performance, subject_name = get_student_performance_for_subject(connection, "1")
    print(f'\nУспішність студентів по предмету {subject_name}:')
    for last_name, first_name, grade in subject_performance:
        print(f'{last_name} {first_name}: {grade}')

    # 5. Порахувати кількість студентів на кожному факультеті
    student_count_per_faculty = get_student_count_per_faculty(connection)
    print('\nКількість студентів на кожному факультеті:')
    for faculty_name, count in student_count_per_faculty:
        print(f'{faculty_name}: {count} студентів')

    # 6. Відобразити оцінки кожного студента по кожному предмету
    grades_per_student = get_grades_for_each_student(connection)
    print('\nОцінки студентів по предметах:')
    for last_name, first_name, subject_name, grade in grades_per_student:
        grade_display = grade if grade is not None else 'No grade'
        print(f'{last_name} {first_name} - {subject_name}: {grade_display}')

except psycopg2.OperationalError as e:
    print(f'Connection error: {e}')
except Exception as e:
    print(f'Error: {e}')
finally:
    connection.close()
