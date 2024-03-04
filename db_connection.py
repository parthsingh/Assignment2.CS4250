#-------------------------------------------------------------------------
# AUTHOR: Parth Singh
# FILENAME: db_connection.py
# SPECIFICATION: SQL Database
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2

def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
        # Establish a connection to the database
    try:
        conn = psycopg2.connect(
            dbname="assignment2",
            user="postgres",
            password="1234",
            # host="your_host",
            port="5432"
        )
        createTables(conn)
        return conn
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None

def createTables(conn):
    cur = conn.cursor()

    sql_create_categories_table = """CREATE TABLE IF NOT EXISTS Categories (
                                         id_cat SERIAL PRIMARY KEY,
                                         name VARCHAR(255) NOT NULL UNIQUE
                                     );"""
    sql_create_documents_table = """CREATE TABLE IF NOT EXISTS Documents (
                                         doc SERIAL PRIMARY KEY,
                                         text TEXT NOT NULL,
                                         title VARCHAR(255) NOT NULL,
                                         num_chars INTEGER NOT NULL,
                                         date DATE NOT NULL,
                                         category_id INTEGER REFERENCES Categories(id_cat)
                                     );"""
    sql_create_terms_table = """CREATE TABLE IF NOT EXISTS Terms (
                                         term VARCHAR(255) PRIMARY KEY,
                                         num_chars INTEGER NOT NULL
                                     );"""
    sql_create_document_terms_table = """CREATE TABLE IF NOT EXISTS Document_Terms (
                                         doc INTEGER REFERENCES Documents(doc),
                                         term VARCHAR(255) REFERENCES Terms(term),
                                         term_count INTEGER NOT NULL,
                                         PRIMARY KEY (doc, term)
                                     );"""

    try:
        cur.execute(sql_create_categories_table)
        cur.execute(sql_create_documents_table)
        cur.execute(sql_create_terms_table)
        cur.execute(sql_create_document_terms_table)
        conn.commit()
        print("Tables created successfully (if they didn't already exist)!")
    except (Exception, psycopg2.Error) as error:
        print("Error creating tables:", error)
              
def createCategory(cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    try:
        cur.execute("INSERT INTO Categories (id_cat, name) VALUES (%s, %s)", (catId, catName))
    except psycopg2.Error as e:
        print("Error creating category:", e)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    cur.execute("SELECT id_cat FROM Categories WHERE name = %s", (docCat,))
    category_id = cur.fetchone()[0]
    num_chars = len(docText.replace(" ", "").replace(".", "").replace(",", ""))

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    cur.execute("INSERT INTO Documents (doc, text, title, num_chars, date, category_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (docId, docText, docTitle, num_chars, docDate, category_id))
    
    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    terms = set(docText.lower().replace(".", "").replace(",", "").split())
    for term in terms:
        cur.execute("SELECT * FROM Terms WHERE term = %s", (term,))
        existing_term = cur.fetchone()
        if not existing_term:
            num_chars = len(term)
            cur.execute("INSERT INTO Terms (term, num_chars) VALUES (%s, %s)", (term, num_chars))
        cur.execute("INSERT INTO Document_Terms (doc, term, term_count) VALUES (%s, %s, %s)",
                    (docId, term, docText.lower().count(term)))

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    cur.execute("DELETE FROM Document_Terms WHERE doc = %s", (docId,))
    # 2 Delete the document from the database
    # --> add your Python code here
    cur.execute("DELETE FROM Documents WHERE doc = %s", (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    try:
        deleteDocument(cur, docId)
        createDocument(cur, docId, docText, docTitle, docDate, docCat)
    except psycopg2.Error as e:
        print("Error updating document:", e)
    # 2 Create the document with the same id
    # --> add your Python code here

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = {}
    try:
        cur.execute("SELECT term, title, term_count FROM Document_Terms JOIN Documents ON Document_Terms.doc = Documents.doc")
        rows = cur.fetchall()
        for row in rows:
            term = row[0]
            title = row[1]
            count = row[2]
            if term not in index:
                index[term] = f"{title}:{count}"
            else:
                index[term] += f",{title}:{count}"
    except psycopg2.Error as e:
        print("Error retrieving index:", e)
    return index