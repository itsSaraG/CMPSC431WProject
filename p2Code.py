
import psycopg2

# Establish connection to the database
connnection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = connnection.cursor()

dbInfo = {'team': ['home', 'away'], 
    'weather': ['weather_temperature', 'weather_wind_mph', 'weather_humidity'],
    'schedules': ['schedule_season', 'schedule_week', 'schedule_playoff', 'stadium_neutral'],
    'downsinfo': ['date', 'home', 'away', 'first_downs_opp_homeAvg', 'first_downs_homeAvg', 'third_downs_opp_homeAvg', 'third_downs_homeAvg', 'fourth_downs_opp_homeAvg', 'fourth_downs_homeAvg'],
    'redzoneinfo': ['date', 'home', 'away', 'redzone_sucess_opp_homeAvg', 'redzone_rate_opp_homeAvg', 'redzone_sucess_homeAvg', 'redzone_rate_homeAvg'],
    'rushinginfo':['date', 'home', 'away', 'rushing_yards_opp_homeAvg', 'rushing_yards_homeAvg'],
    'passinginfo': ['date', 'home', 'away', 'passing_yards_homeAvg', 'passing_yards_opp_homeAvg', 'pass_completion_rate_opp_homeAvg', 'pass_completion_rate_homeAvg'],
    'otherteamstats': ['date', 'home', 'away', 'total_yards_opp_homeAvg', 'total_yards_homeAvg', 'sacks_opp_homeAvg', 'sacks_homeAvg', 'turnovers_opp_homeAvg', 'turnovers_homeAvg', 'penalties_opp_homeAvg', 'penalties_homeAvg'],
    'gameinformation': ['date', 'home', 'away', 'winner', 'score_home', 'score_away']}

def printCLI():
    print("Welcome to the Database CLI Interface! \n")
    print("Please select an option:")
    print("1. Insert Data")
    print("2. Delete Data")
    print("3. Update Data")
    print("4. Search Data")
    print("5. Aggregate Functions")
    print("6. Sorting")
    print("7. Joins")
    print("8. Grouping")
    print("9. Subqueries")
    print("10. Transactions")
    print("11. Error Handling")
    print("12. Exit")


def mainFunc():
    funcChoice = {
        '1': insertData,
        '2': deleteData,
        '3': updateData,
        '4': searchData,
        '5': aggFunc,
        '6': sortFunc,
        '7': joinFunc,
        '8': groupingFunc,
        '9': subqFunc,
        '10': transactions,
        '11': errorHandling
    }

    numChoice = [str(x) for x in range(1, 12)]

    try:
        while True:
            printCLI()
            user_input = input("\nEnter your choice (1-12):")
            if user_input in numChoice:

                chosenFunction = funcChoice[user_input]()

            elif user_input == '12':
                return
            else:
                print("Invalid input. Please enter a number between 1 and 11.")
    except KeyboardInterrupt:
        return


def insertData():

    

    
    table_name = input("Enter table name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return insertData()
    values = input(f"Enter data to populate table (separated by comma) for the table {table_name} with values corresponding to \n{dbInfo[table_name]}: ").split(",")

    

    valuesString = ""
    for i in range(len(values)):
        valuesString += "%s"
        if i != len(values) - 1:
            valuesString += ", "


    try: 
        query = f"INSERT INTO {table_name} VALUES ({valuesString})"
        cursor.execute(query, values)

        connnection.commit()
        print("Successful Insertion")
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback()

    #cursor.execute(f"SELECT * FROM {table_name}")

    #records = cursor.fetchall()
    #print(records)




def deleteData():

    table_name = input("Enter Table Name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return deleteData()
    condition = input(f"Enter the WHERE condition to delete from any of these columns \n{dbInfo[table_name]}: ")

   
    try: 
        query = f"DELETE FROM {table_name} WHERE ({condition})"    
        cursor.execute(query)
        connnection.commit()
        print("Successful Deletion")
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback()


    #cursor.execute("SELECT * FROM team")
    #records = cursor.fetchall()
    #print(records)




def updateData():

    table_name = input("Enter table name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return updateData()
    setCOND = input(f"SET condition with column name used as one of these \n{dbInfo[table_name]}: ")
    whereCOND = input(f"WHERE condition with column name used as one of these \n{dbInfo[table_name]}: ")

    try: 
        query = f"UPDATE {table_name} SET {setCOND} WHERE {whereCOND}"

        cursor.execute(query)

        connnection.commit()
        print("successful update")
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback()


    #cursor.execute(f"SELECT * FROM {table_name}")
    #records = cursor.fetchall()
    #print(records)


def searchData():
    table_name = input("Enter table name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return searchData()
    condition = input(f"Enter WHERE condition from these columns \n{dbInfo[table_name]} : ")

    try: 
        if condition:
            query = f"SELECT * FROM {table_name} WHERE {condition}"
            cursor.execute(query)
        else: 
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)

        records = cursor.fetchall()
        print("Search results:")
        for row in records:
            print(row)
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback()


def aggFunc():
    table_name = input("Enter table name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return aggFunc()
    column = input(f"Enter column for aggregation from these \n{dbInfo[table_name]} : ")
    operation = input("Enter aggregation operation (SUM, AVG, COUNT, MIN, MAX): ")

    if operation.upper() not in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']:
        print("Invalid operation.")
        return

    try: 
        query = f"SELECT {operation.upper()}({column}) FROM {table_name}"

        cursor.execute(query)

        result = cursor.fetchall()
        print(result)
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback()


def sortFunc():
    table_name = input("Enter table name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return sortFunc()
    
    column = input(f"Enter column to sort by \n{dbInfo[table_name]} : ")
    order = input("Enter sorting order (ASC/DESC): ")

    try:
        query = f"SELECT * FROM {table_name} ORDER BY {column} {order}"

        cursor.execute(query)

        records = cursor.fetchall()
        print("Sorted data:")
        for row in records:
            print(row)
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback()


def joinFunc():
    table1 = input("Enter first table name: ")
    print(['INNER JOIN, LEFT JOIN, RIGHT JOIN, JOIN'])
    join_type = input("Enter the join type?")
    table2 = input("Enter second table name: ")
    condition = input("Enter join condition on what keys to join on: ")

    try: 
        if join_type: 
            query = f"SELECT * FROM {table1} {join_type} {table2} ON {condition}"
            cursor.execute(query)
        else: 
            query = f"SELECT * FROM {table1} JOIN {table2} ON {condition}"

            cursor.execute(query)

        records = cursor.fetchall()
        print("Join result:")
        for row in records:
            print(row)
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback() 


def groupingFunc():
    table_name = input("Enter table name: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return groupingFunc()
    
    aggFunction = input(f"Enter aggregate function name for grouping from the following (SUM, AVG, COUNT, MIN, MAX): ")
    if aggFunction.upper() not in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']:
        print("Invalid operation.")
        return groupingFunc()

    aggColumn = input(f"Enter column for aggregate function, or * if you want to do all for grouping from the following \n{dbInfo[table_name]}: ")
    column = input(f"Enter column name for grouping from the following \n{dbInfo[table_name]}: ")



    try: 
        query = f"SELECT {column}, {aggFunction}({aggColumn}) FROM {table_name} GROUP BY {column}"

        cursor.execute(query)

        records = cursor.fetchall()
        #print(records)

        
       
        for i in records:
            print(i)
        
        
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback() 

def subqFunc():
    table_name = input("Enter table name for the subquery: ")
    if table_name not in dbInfo: 
        print(f"Incorrect table name, choose one from the following: {dbInfo.keys()}")
        return subqFunc()
    subquery_condition = input(f"Enter subquery column for SELECT from the following columns \n{dbInfo[table_name]}: ")
    condition = input(f"Enter query WHERE condition using the following columns as well \n{dbInfo[table_name]}: ")


    try: 
        query = f"SELECT * FROM (SELECT {subquery_condition} FROM {table_name}) WHERE {condition}"

        cursor.execute(query)

        records = cursor.fetchall()
        print("Subquery result:")
        for row in records:
            print(row)
    except Exception as e: 
        print(f"Error, {e}, please fix it")
        connnection.rollback() 


def transactions():
    b = True

    funcsOp = ['UPDATE', 'INSERT', 'DELETE']
    while b: 
        print(funcsOp)
        answer = input("Would you like to perform any function above y or n? ")
        if answer == 'y': 
            funcChoice = input("Which transaction do you want to do: u, i, d? ")
            if funcChoice == 'u':
                table_name = input("Enter table name: ")
                setCOND = input("SET condition: ")
                whereCOND = input("WHERE condition: ")
                query = f"UPDATE {table_name} SET {setCOND} WHERE {whereCOND}"
                cursor.execute(query)

            elif funcChoice == 'i':
                table_name = input("Enter table name: ")
                values = input("Enter data to populate table (separated by comma): ").split(",")

                placeholders = ""
                for i in range(len(values)):
                    placeholders += "%s"
                    if i != len(values) - 1:
                        placeholders += ", "

                query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.execute(query, values)
            elif funcChoice == 'd':
                table = input("Enter Table Name: ")
                condition = input("Enter the Condition to Delete: ")

                query = f"DELETE FROM {table} WHERE ({condition})"

                cursor.execute(query)
            else: 
                return
        else: 
            b = False

    
    
    connnection.commit()
    print("Transaction committed successfully.")


def errorHandling():
    return "Error handling is handled in each of the choices"


if __name__ == "__main__":
    mainFunc()
