import mysql.connector
import csv

# Database connection information
db_config = {
    'host': '192.168.0.49',
    'user': 'tarik',
    'password': 'abc12345',
    'database': 'my_db'
}

connection = mysql.connector.connect(**db_config)

if connection.is_connected():
        print("Database connection is successful.")

        cursor = connection.cursor()

        # Read data from countries.csv and insert into the COUNTRIES table
        with open('/home/tarik/countries.csv', newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip the header row
            for row in reader:
                country = row[0]
                
                cursor.execute("INSERT INTO COUNTRIES (COUNTRY) VALUES (%s)", 
                    (country,))
        
        print("Countries data inserted successfully.")

        
        # Read data from cities.csv and insert into the CITIES table
        with open('/home/tarik/cities.csv', newline='') as file:
            reader = csv.reader(file)
            next(reader)  
            for row in reader:
                country_id = int(row[0])
                city = row[1]
                
                cursor.execute("INSERT INTO CITIES (COUNTRYID, CITY) VALUES 
                      (%s, %s)", (country_id, city))
                
        print("Cities data inserted successfully.")

        
        # Read data from users.csv and insert into the USERS table
        with open('/home/tarik/users.csv', newline='') as file:
            reader = csv.reader(file)
            next(reader)  
            for row in reader:
                USERNAME_ = row[0]
                PASSWORD_ = row[1]
                NAMESURNAME = row[2]
                EMAIL = row[3]
                GENDER = row[4]
                BIRTHDATE = row[5]
                COUNTRYID = int(row[6])
                CITYID = int(row[7])
                CREATEDDATE = row[8]

                cursor.execute("INSERT INTO USERS (USERNAME_, PASSWORD_, 
NAMESURNAME, EMAIL, GENDER, BIRTHDATE, COUNTRYID, CITYID, CREATEDDATE) VALUES 
(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (USERNAME_, PASSWORD_, NAMESURNAME, 
EMAIL, GENDER, BIRTHDATE, COUNTRYID, CITYID, CREATEDDATE))
        
        print("users data inserted successfully.")
        
        connection.commit()         
        print("The data has been successfully added.")


        # connection closed 
        cursor.close()
        connection.close()
        print("Database connection closed.")

else:
     print("Connection failed.")
