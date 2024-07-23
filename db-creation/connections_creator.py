import sqlite3

conn = sqlite3.connect('recom_db2.db')
cursor = conn.cursor()

doi_max_len = 50
author_max_len = 50

cursor.execute(f'''
CREATE TABLE IF NOT EXISTS connections (
    doi1 VARCHAR({doi_max_len}),
    citations1 INT,
    doi2 VARCHAR({doi_max_len}),
    citations2 INT,
    PRIMARY KEY(doi1, doi2)
)
''')

# Populate connections table
cursor.execute('''
INSERT INTO connections (doi1, citations1, doi2, citations2)
SELECT 
    pc1.doi AS doi1, 
    pc1.citations AS citations1, 
    pc2.doi AS doi2, 
    pc2.citations AS citations2
FROM 
    publications_citations pc1
JOIN 
    authored a1 ON pc1.doi = a1.doi
JOIN 
    authored a2 ON a1.author = a2.author
JOIN 
    publications_citations pc2 ON a2.doi = pc2.doi
WHERE 
    pc1.doi <> pc2.doi
''')

conn.commit()

cursor = conn.execute("SELECT COUNT(*) FROM connections")
row = cursor.fetchone()
print(f"Total rows in connections (edges) table: {row[0]}")

# Close the connection
conn.close()

output_file = 'output.txt'
with open(output_file, 'a') as f:
    f.write(f"Total rows in connections (edges) table: {row[0]}")