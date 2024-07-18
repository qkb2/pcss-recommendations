import sqlite3

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('example.db')
cursor = conn.cursor()

# Define the maximum length for DOI and author fields
doi_max_len = 50
author_max_len = 50

# Create publications_citations table
cursor.execute('''
CREATE TABLE IF NOT EXISTS publications_citations AS
SELECT 
    publications.title, 
    publications.doi, 
    citations.omid, 
    citations.citations
FROM 
    publications
INNER JOIN 
    citations
ON 
    publications.doi = citations.doi
''')

# Create authored table
cursor.execute(f'''
CREATE TABLE IF NOT EXISTS authored (
    doi VARCHAR({doi_max_len}), 
    author VARCHAR({author_max_len}), 
    PRIMARY KEY(doi, author)
)
''')

# Create connections table
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

# Commit the transaction
conn.commit()

# Close the connection
conn.close()
