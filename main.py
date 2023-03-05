from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session

from pprint import pprint


def select_one():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_two(discipline_id: int):
    """
    Знайти студента із найвищим середнім балом з певного предмета.
    :return: list[dict]
    """
    result = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return result

def select_three(discipline_id: int):
    """
    Знайти середній бал у групах з певного предмета.
    :return: list[dict]
    """

    result = session.query(Discipline.name,
                           Group.name,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Student)\
        .join(Group) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Group.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .all()
    return result

def select_four():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    :return: list[dict]
    """

    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade) \
        .order_by(desc('avg_grade')) \
        .all()
    return result

def select_five(teacher_id: int):
    """
    Знайти які курси читає певний викладач.
    :return: list[dict]
    """
    result = session.query(Teacher.fullname, Discipline.name) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher) \
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.fullname, Discipline.name) \
        .all()
    return result

def select_six(group_id: int):
    """
    Знайти список студентів у певній групі.
    :return: list[dict]
    """
    result = session.query(Group.name, Student.fullname) \
        .select_from(Grade)\
        .join(Student)\
        .join(Group) \
        .filter(Group.id == group_id) \
        .group_by(Group.name, Student.fullname) \
        .all()
    return result

def select_seven(discipline_id, group_id):
    """
    Знайти оцінки студентів у окремій групі з певного предмета.
    :return: list[dict]
    """
    result = session.query(Discipline.name, Group.name, Student.fullname, Grade.grade) \
        .select_from(Grade)\
        .join(Student)\
        .join(Group) \
        .join(Discipline)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id)) \
        .group_by(Discipline.name, Group.name, Student.fullname, Grade.grade) \
        .all()
    return result

def select_eight(teacher_id):
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return: list[dict]
    """
    result = session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.fullname, Discipline.name) \
        .all()
    return result

def select_nine(student_id):
    """
    Знайти список курсів, які відвідує певний студент.
    :return: list[dict]
    """
    result = session.query(Student.fullname, Discipline.name) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Student)\
        .filter(Student.id == student_id) \
        .group_by(Student.fullname, Discipline.name) \
        .all()
    return result

def select_ten(student_id, teacher_id):
    """
    Список курсів, які певному студенту читає певний викладач.
    :return: list[dict]
    """
    result = session.query(Student.fullname, Teacher.fullname, Discipline.name) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Student)\
        .join(Teacher)\
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .group_by(Student.fullname, Teacher.fullname, Discipline.name)\
        .all()
    return result

def select_eleven(student_id, teacher_id):
    """
    Середній бал, який певний викладач ставить певному студентові.
    :return: list[dict]
    """
    result = session.query(Student.fullname, Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Student)\
        .join(Teacher)\
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .group_by(Student.fullname, Teacher.fullname)\
        .all()
    return result


def select_twelve(discipline_id, group_id):
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    result = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return result


if __name__ == '__main__':
    pprint(select_one())
    pprint(select_two(1))
    pprint(select_three(1))
    pprint(select_four())
    pprint(select_five(1))
    pprint(select_six(1))
    pprint(select_seven(3, 5))
    pprint(select_eight(1))
    pprint(select_nine(1))
    pprint(select_ten(3, 5))
    pprint(select_eleven(3, 5))
    pprint(select_twelve(3, 1))