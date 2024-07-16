import pandas as pd
import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('recom_db2')
cursor = conn.cursor()

doi_max_len = 50
title_max_len = 50
omid_max_len = 50
citations_max_len = 10
author_max_len = 50

conn.execute("DROP TABLE citations")

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
    multi_dois = 0
    for index, row in chunk.iterrows():
        line_count += 1
        omid = row['omid']
        omid = omid[4:]
        omid = int(omid)
        # print(omid)
        identifier = row['id']
        # print(identifier)
        if 'doi:' in identifier:
            doi = None
            ids = identifier.split(' ')
            for id in ids:
                if id.startswith('doi:'):
                    if doi is None:
                        doi = id[4:]
                    else:
                        multi_dois += 1
            cursor.execute('''
                UPDATE citations
                SET doi = ? 
                WHERE omid = ?
            ''', (doi, str(omid)))
            if cursor.rowcount == 1:
                update_count += 1
    conn.commit()
    return update_count, line_count, multi_dois

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

# Initialize params
chunk_size = 10000
db_omid_size = 10_000
db_cit_size = 10_000
lines_read_limit_omid = 10_000
lines_read_limit_cit = 10_000
stop_on_db_size = True

# Initialize counters
omid_updates_counter = 0
citations_updates_counter = 0
omid_line_reads_counter = 0
citations_line_reads_counter = 0
multi_doi_counter = 0

for chunk in pd.read_csv('dblp/citations.csv', chunksize=chunk_size):
    updated, read = update_citations_for_omid(chunk)
    citations_updates_counter += updated
    citations_line_reads_counter += read
    print(citations_updates_counter)
    if stop_on_db_size and (
        citations_updates_counter >= db_cit_size or citations_line_reads_counter >= lines_read_limit_cit):
        break
    
for chunk in pd.read_csv('dblp/omid.csv', chunksize=chunk_size):
    updated, read, multi_dois = update_doi_for_omid(chunk)
    omid_updates_counter += updated
    omid_line_reads_counter += read
    multi_doi_counter += multi_dois
    print(omid_updates_counter)
    if stop_on_db_size and (
        omid_updates_counter >= db_omid_size or omid_line_reads_counter >= lines_read_limit_omid):
        break

# Close the connection
conn.close()

# Print the update counts
print(f"Total OMID updates: {omid_updates_counter}")
print(f"Total citations updates: {citations_updates_counter}")
print(f"Total OMID lines: {omid_line_reads_counter}")
print(f"Total citations lines: {citations_line_reads_counter}")
print(f"Total instances of multiple DOIs: {multi_doi_counter}")
# Multiple DOIs counter counts how many DOIs there are over the limit of 1 DOI per OMID, e.g. for
# OMID 123 with DOIs doi:1234 doi:12345 doi:3210 isbn:123456 there would be +2 multiple DOIs (12345 and 3210)

output_file = 'output.txt'
with open(output_file, 'a') as f:
    f.write(f"Total OMID updates: {omid_updates_counter}\n")
    f.write(f"Total citations updates: {citations_updates_counter}\n")
    f.write(f"Total OMID lines: {omid_line_reads_counter}\n")
    f.write(f"Total citations lines: {citations_line_reads_counter}\n")
    f.write(f"Total instances of multiple DOIs: {multi_doi_counter}\n")