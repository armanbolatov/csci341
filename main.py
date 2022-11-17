from sqlalchemy import text, create_engine, inspect
import pandas as pd

dbschema = "assignment"
engine = create_engine(
    "postgresql://postgres:Qwerty123@127.0.0.1:5432/postgres",
    connect_args={'options': '-csearch_path={}'.format(dbschema)}
)


def get_query1():
    # List the disease code and the description of diseases that are
    # caused by “bacteria” (pathogen) and were discovered before 1990.
    query = text(
        """
        SELECT D.disease_code, D.description
        FROM "Disease" D JOIN "Discover" E ON D.disease_code = E.disease_code
        WHERE D.pathogen = 'bacteria' AND E.first_enc_date < '1990-01-01'
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)


def get_query2():
    query = text(
        # List the name, surname and degree of doctors
        # who are not specialized in “infectious diseases”.
        """
        SELECT U.name, U.surname, D.degree
        FROM "Users" U INNER JOIN "Doctor" D ON U.email = D.email
                       INNER JOIN "Specialize" S ON U.email = S.email
                       INNER JOIN "DiseaseType" DT ON S.id = DT.id    
        GROUP BY (U.name, U.surname, D.degree)
        EXCEPT
        SELECT U.name, U.surname, D.degree
        FROM "Users" U INNER JOIN "Doctor" D ON U.email = D.email
                       INNER JOIN "Specialize" S ON U.email = S.email
                       INNER JOIN "DiseaseType" DT ON S.id = DT.id    
        WHERE DT.description = 'infectious disease'
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)



def get_query3():
    query = text(
        # List the name, surname and degree of doctors
        # who are specialized in more than 2 disease types.
        """
        SELECT name, surname, degree
        FROM (
            SELECT U.email, U.name, U.surname, D.degree, S.id
            FROM "Users" U INNER JOIN "Doctor" D ON U.email = D.email
                           INNER JOIN "Specialize" S ON U.email = S.email
            ) AS DS
        GROUP BY (name, surname, degree)
        HAVING COUNT(*) > 2
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)


def get_query4():
    # For each country list the cname and average salary of doctors
    # who are specialized in “virology”.
    query = text(
        """
        SELECT U.cname, AVG(U.salary)
        FROM "Doctor" D JOIN "Users" U ON D.email = U.email
        WHERE D.degree = 'virology'
        GROUP BY U.cname
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)

def get_query5():
    # List the departments of public servants who report “covid-19” cases
    # in more than one country and the number of such public servants
    # who work in these departments
    query = text(
        """
        SELECT PS.department, AVG(PSC.num_workers)
        FROM (
            SELECT R.email
            FROM "Record" R
            WHERE R.disease_code = 'covid-19'
            GROUP BY (R.email, R.disease_code)
            HAVING COUNT(*) > 1
        ) AS RC
        INNER JOIN "PublicServant" PS ON RC.email = PS.email
        INNER JOIN (
            SELECT PS.department, COUNT(*) AS num_workers
            FROM "PublicServant" PS
            GROUP BY PS.department
        ) AS PSC
        ON PS.department = PSC.department
        GROUP BY PS.department
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)


def get_query6():
    # Double the salary of public servants
    # who have recorded covid-19 patients more than 3 times.
    query = text(
        """
        UPDATE "Users" U
        SET salary = salary * 2
        FROM (
            SELECT R.email
            FROM "Record" R
            WHERE R.disease_code = 'covid-19'
            GROUP BY (R.email, R.disease_code)
            HAVING COUNT(*) > 3
        ) AS RC
        WHERE U.email = RC.email
        """
    )
    try:
        engine.execute(query)
        print("Query 6 executed successfully")
    except:
        print("Query 6 failed to execute")


def get_query7():
    # Delete the users whose name contain the substring “bek” or “gul”
    query = text(
        """
        DELETE FROM "Users"
        WHERE name LIKE '%bek%' OR name LIKE '%gul%'
        """
    )
    try:
        engine.execute(query)
        print("Query 7 executed successfully")
    except:
        print("Query 7 failed to execute")

def get_query8():
    query = text(
        # Create an index namely “idx pathogen” on the “pathogen” field.
        """
        CREATE INDEX idx_pathogen ON "Disease" (pathogen)
        """
    )
    try:
        engine.execute(query)
        print("Query 8 executed successfully")
    except:
        print("Query 8 failed to execute")


def get_query9():
    query = text(
        # List the email, name, and department of public servants
        # who have created records where the number of patients
        # is between 100000 and 999999
        """
        SELECT U.email, U.name, U.surname, PS.department
        FROM "PublicServant" PS INNER JOIN "Record" R ON PS.email = R.email
                                INNER JOIN "Users" U ON PS.email = U.email
        WHERE R.total_patients >= 100000 AND R.total_patients <= 999999
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)


def get_query10():
    # List the top 5 counties with the highest number of total patients recorded.
    query = text(
        """
        SELECT cname, MAX(total_patients)
        FROM "Record"
        GROUP BY cname
        ORDER BY MAX(total_patients) DESC
        LIMIT 5
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)


def get_query11():
    # Group the diseases by disease type and the total number of patients treated
    query = text(
        """
        SELECT DT.description, SUM(R.total_patients)
        FROM "DiseaseType" DT JOIN "Disease" D ON DT.id = D.id
                              JOIN "Record" R ON R.disease_code = D.disease_code
        GROUP BY DT.description
        """
    )
    df = pd.read_sql(query, engine)
    return print(df)

if __name__ == "__main__":
    # get_query1()
    get_query2()
    # get_query3()
    # get_query4()
    # get_query5()
    # get_query6()
    # get_query7()
    # get_query8()
    # get_query9()
    # get_query10()
    # get_query11()