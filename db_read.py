import sqlite3

conn = sqlite3.connect('recom_db3.db')

# Query all records from the publications_citations table
cursor = conn.execute("SELECT * FROM publications_citations")

# Fetch all results from the cursor
rows = cursor.fetchall()

# Iterate through the rows and print them
for idx, row in enumerate(rows):
    num = row[-1]
    c = int.from_bytes(num, 'little')
    print(c)
    print(row)
    if idx > 100:
        break

cursor = conn.execute("SELECT COUNT(*) FROM publications_citations")
row = cursor.fetchone()
print(f"Total rows in connected table: {row}")

# Close the connection
conn.close()

# output_file = 'output.txt'
# with open(output_file, 'a') as f:
#     f.write(f"Total rows in connected table: {row}\n")