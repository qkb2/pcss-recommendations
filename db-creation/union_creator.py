import sqlite3

conn = sqlite3.connect("recom_db2.db")

conn.execute("""
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

conn.commit()

cursor = conn.execute("SELECT COUNT(*) FROM publications_citations")
row = cursor.fetchone()
print(f"Total rows in connected table: {row[0]}")

# Close the connection
conn.close()

output_file = "output.txt"
with open(output_file, "a") as f:
    f.write(f"Total rows in connected table: {row[0]}\n")
