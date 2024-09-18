import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Function to read and convert byte data from citations.citations column
def read_byte_as_int(byte_data):
    # Assuming the bytes are stored as little-endian (adjust as needed)
    return int.from_bytes(byte_data, byteorder="little")

# Connect to the SQLite database
conn = sqlite3.connect("recom_db3.db")

# Load the first few rows from citations and convert the 'citations' column from byte to int
citations_df = pd.read_sql_query("SELECT omid, citations, doi FROM citations LIMIT 5", conn)
citations_df['citations'] = citations_df['citations'].apply(lambda x: read_byte_as_int(x))

# Load the first few rows from publications and authored
publications_df = pd.read_sql_query("SELECT * FROM publications LIMIT 5", conn)
authored_df = pd.read_sql_query("SELECT * FROM authored LIMIT 5", conn)

# Close the database connection
conn.close()

# Function to truncate long text fields (e.g., titles) for visualization
def truncate_text(text, max_len=30):
    if len(text) > max_len:
        return text[:max_len] + '...'
    return text

# Apply truncation to the 'title' column in the publications DataFrame
publications_df['title'] = publications_df['title'].apply(lambda x: truncate_text(x, max_len=30))

# Create a function to visualize tables using matplotlib
def visualize_table(df, title, ax):
    ax.axis('off')  # Turn off the axis
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.2)  # Scale the table size
    ax.set_title(title)

# Set up the plot
fig, axes = plt.subplots(3, 1, figsize=(10, 15))

# Visualize each table
visualize_table(citations_df, 'Citations Table', axes[0])
visualize_table(publications_df, 'Publications Table (Truncated Titles)', axes[1])
visualize_table(authored_df, 'Authored Table', axes[2])

# Adjust layout and display the plot
plt.tight_layout()
plt.show()
