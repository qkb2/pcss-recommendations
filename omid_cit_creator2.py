import pandas as pd
import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('recom_db')
cursor = conn.cursor()

doi_max_len = 50
title_max_len = 50
omid_max_len = 50
citations_max_len = 10
author_max_len = 50

conn.execute(f"""
CREATE TABLE IF NOT EXISTS citations (
    omid VARCHAR({omid_max_len}) PRIMARY KEY, 
    citations INT({citations_max_len}),
    doi VARCHAR({doi_max_len})
)
""")

# Function to update the omid for DOI and count successful updates
def update_doi_for_omid(chunk):
    update_count = 0
    line_count = 0
    for index, row in chunk.iterrows():
        line_count += 1
        omid = row['omid']
        omid = omid[4:]
        omid = int(omid)
        # print(omid)
        identifier = row['id']
        # print(identifier)
        if 'doi:' in identifier:
            doi = ''
            ids = identifier.split(' ')
            for id in ids:
                if id.startswith('doi:'):
                    doi = id[4:]
                    break
            cursor.execute('''
                UPDATE citations
                SET doi = ?
                WHERE omid = ?
            ''', (doi, str(omid)))
            if cursor.rowcount == 1:
                update_count += 1
    conn.commit()
    return update_count, line_count

# Function to update the citations for OMID and count successful updates
def update_citations_for_omid(chunk):
    update_count = 0
    line_count = 0
    for index, row in chunk.iterrows():
        line_count += 1
        omid = row['omid']
        citations = row['citations']
        cursor.execute('''
            INSERT INTO citations (citations, omid)
            VALUES (?, ?)
        ''', (citations, str(omid)))
        if cursor.rowcount == 1:
            update_count += 1
    conn.commit()
    return update_count, line_count

# Define chunk size
chunk_size = 10000

db_omid_size = 100_000
db_cit_size = 100_000
lines_read_limit_omid = 10_000_000
lines_read_limit_cit = 10_000_000
stop_on_db_size = True

# Initialize counters
total_omid_updates = 0
total_citations_updates = 0
total_omid_lines = 0
total_citations_lines = 0
lines_read_omid = 0
lines_read_cit = 0

# Read the second CSV file in chunks and update citations for OMID
for chunk in pd.read_csv('dblp/citations.csv', chunksize=chunk_size):
    updated, read = update_citations_for_omid(chunk)
    total_citations_updates += updated
    lines_read_cit += read
    print(total_citations_updates)
    if stop_on_db_size and (total_citations_updates >= db_cit_size or total_citations_lines >= lines_read_limit_cit):
        break
    
# Read the first CSV file in chunks and update OMID for DOI
for chunk in pd.read_csv('dblp/omid.csv', chunksize=chunk_size):
    updated, read = update_doi_for_omid(chunk)
    total_omid_updates += updated
    lines_read_omid += read
    print(total_omid_updates)
    if stop_on_db_size and (total_omid_updates >= db_omid_size or total_omid_lines >= lines_read_limit_omid):
        break

# Close the connection
conn.close()

# Print the update counts
print(f"Total OMID updates: {total_omid_updates}")
print(f"Total citations updates: {total_citations_updates}")
print(f"Total OMID lines: {lines_read_omid}")
print(f"Total citations lines: {lines_read_cit}")

output_file = 'output.txt'
with open(output_file, 'w') as f:
    f.write(f"Total OMID updates: {total_omid_updates}\n")
    f.write(f"Total citations updates: {total_citations_updates}\n")
    f.write(f"Total OMID lines: {lines_read_omid}\n")
    f.write(f"Total citations lines: {lines_read_cit}\n")