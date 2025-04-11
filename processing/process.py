import mysql.connector

def write_file_contents_to_db(file_path, host, user, password, database, table):
    """
    Opens a file, reads its contents, and writes them to a database.

    Args:
        file_path (str): The path to the file to read.
        db_path (str): The path to the database file.
        table_name (str): The name of the table to write to.
    """

    try:

        # Connect to db
        mydb = mysql.connector.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )

        # Create db cursor.
        cursor = mydb.cursor()

        # Open file to analyze.
        with open(file_path, 'r') as file:

            # Set pointer to beginning of file.
            file.seek(0)

            # Set initial variable positions.
            abs_id = ''
            background = ''
            methods = ''
            results = ''
            conclusions = ''

            # Flag to indicate new abstract.
            new_abstract = True

            # Read file line by line.
            for line in file:

                # -- Strip out sections -- #

                # Identify new abstract to analyze.

                # Check for new abstract.  Submit old abstract to db.  Store new abstract id.
                if line[0:3] == '###' or line == "":  # '' indicates end of file (EOF).

                    # -- Submit query to write to db. -- #

                    # Check for first iteration.
                    '''
                        We are submitting the last batch to write to the db.  
                        So if abs_id is blank we have nothing to submit as it's our first iteration, 
                        and we have identified our first abstract. 
                    '''

                    if not abs_id == '':

                        # Escape quotes as for some reason Python doesn't have a mysqli_real_escape_string() function.
                        background = background.replace("\'", "\\'")
                        methods = methods.replace("\'", "\\'")
                        results = results.replace("\'", "\\'")
                        conclusions = conclusions.replace("\'", "\\'")

                        # Build SQL insert string.
                        sql = f"INSERT INTO {database}.{table} (abs_id, background, methods, results, conclusions) VALUES ({abs_id}, '{background}', '{methods}', '{results}', '{conclusions}')"

                        # Run Query
                        try:

                            query_result = cursor.execute(sql)

                            mydb.commit()

                            print(cursor.rowcount, "record inserted.")

                        except Exception as e:

                            print("SQL query returend: " + str(e))
                            print(sql)


                    # If EOF terminate.
                    if line == "":
                        return('End of file reached.')

                    # Reset vars to store values to submit in SQL query.

                    abs_id = ''
                    background = ''
                    methods = ''
                    results = ''
                    conclusions = ''

                    # Store abstract id.
                    abs_id = line[3:]


                # Store background and remove leading whitespaces.
                if line[0:10] == 'BACKGROUND':
                    background += line[10:].lstrip()

                # Store methods.
                if line[0:7] == "METHODS":
                    methods += line[7:].lstrip()

                # Store results
                if line[0:7] == "RESULTS":
                    results += line[7:].lstrip()

                # Store conclusions.
                if line[0:11] == "CONCLUSIONS":
                    conclusions += line[11:].lstrip()

    except Exception as e:

        print(e)


file_path = "/home/danielayer/Desktop/njit/Spring 2025/Deep Learning/project/ds677/data/pubmed-rct-master/PubMed_20k_RCT_numbers_replaced_with_at_sign/dev.txt"

host = 'localhost'

user = 'user'

password = 'enter'

database = 'ds677'

table = 'abstracts'

try:

    results = write_file_contents_to_db(file_path, host, user, password, database, table)

    print(results)

except Exception as e:

    print(e)

