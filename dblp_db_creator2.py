import sqlite3
from lxml import etree as ET

filename = "dblp/dblp.xml"
dtd_name = "dblp/dblp.dtd"
main_db_name = "recom_db"

conn = sqlite3.connect(main_db_name)

doi_max_len = 50
title_max_len = 50
omid_max_len = 50
citations_max_len = 10
author_max_len = 50

conn.execute(f"""
CREATE TABLE IF NOT EXISTS publications (
    title VARCHAR({title_max_len}), 
    doi VARCHAR({doi_max_len}) PRIMARY KEY
)
""")

conn.execute(f"""
CREATE TABLE IF NOT EXISTS authored (
    doi VARCHAR({doi_max_len}), 
    author VARCHAR({author_max_len}), 
    PRIMARY KEY(doi, author)
)
""")

# conn.execute(f"""
# CREATE TABLE IF NOT EXISTS omids (
#     doi VARCHAR({doi_max_len}) UNIQUE, 
#     omid VARCHAR({omid_max_len}) PRIMARY KEY,
#     FOREIGN KEY(doi) REFERENCES publications(doi)
# )
# """)



dblp_record_types_for_publications = ('article',)

# Read DTD
dtd = ET.DTD(dtd_name)

# Get an iterable
context = ET.iterparse(filename, events=('start', 'end'), tag=dblp_record_types_for_publications, load_dtd=True, resolve_entities=True)

max_doi_len_found = 0
skipped_len_counter = 0
skipped_no_data_counter = 0
skipped_bad_doi = 0
processed_articles_counter = 0
updated_counter = 0

db_size = 1_000_000
stop_on_db_size = True

for event, elem in context:
    if stop_on_db_size and updated_counter >= db_size:
        break
    if elem.tag in dblp_record_types_for_publications and event == 'end':
        processed_articles_counter += 1
        pub_title = elem.findtext('title')
        if not pub_title:
            skipped_no_data_counter += 1
            elem.clear()
            continue

        pub_authors = [author.text for author in elem.findall('author') if author.text]

        ee_doi = elem.findtext('ee')
        if not ee_doi:
            skipped_no_data_counter += 1
            elem.clear()
            continue
        
        pub_doi = ee_doi[16:]
        
        if not pub_doi.startswith('10'):
            skipped_bad_doi += 1
            elem.clear()
            continue
        
        print(pub_doi)
        
        if len(pub_doi) > max_doi_len_found:
            max_doi_len_found = len(pub_doi)
            
        if len(pub_doi) > doi_max_len:
            skipped_len_counter += 1
            elem.clear()
            continue

        pub_title_sql_str = pub_title.replace("'", "''")
        pub_author_sql_strs = [author.replace("'", "''") for author in pub_authors]

        conn.execute("""
        INSERT OR IGNORE INTO publications (title, doi) 
        VALUES (?, ?)
        """, (pub_title_sql_str, pub_doi))

        for author in pub_author_sql_strs:
            conn.execute("""
            INSERT OR IGNORE INTO authored (doi, author) 
            VALUES (?, ?)
            """, (pub_doi, author))

        updated_counter += 1
        # print("No. of records parsed: {}".format(n_records_parsed))

        # Clear the processed element to free memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

conn.commit()
conn.close()

print(f"Total succesful updates: {updated_counter}")
print(f"Total no-data skips: {skipped_no_data_counter}")
print(f"Total DOI to long skips: {skipped_len_counter}")
print(f"Total bad DOI skips: {skipped_bad_doi}")
print(f"Total articles processed: {processed_articles_counter}")
