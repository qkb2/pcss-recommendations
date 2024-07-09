import sqlite3

conn = sqlite3.connect('recom_db')

# Query all records from the publications_citations table
cursor = conn.execute("SELECT * FROM publications_citations")

# Fetch all results from the cursor
rows = cursor.fetchall()

# Iterate through the rows and print them
for row in rows:
    num = row[-1]
    c = int.from_bytes(num, 'little')
    print(c)
    print(row)

# Close the connection
conn.close()