import sqlite3

conn = sqlite3.connect('recom_db2')

conn.execute(f"""
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
""")

# Commit changes and close the connection
conn.commit()
conn.close()