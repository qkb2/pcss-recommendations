import sqlite3
import random

class Article:
    def __init__(self, doi, citations, title) -> None:
        self.doi = doi
        self.citations = citations
        self.title = title
      
# TODO: implement subgraph  
class SurfGraph:
    def __init__(self, user_context, conn: sqlite3.Connection) -> None:
        self.graph = dict()
        self.user_context = user_context
        self.conn = conn
        self.article_list = dict()
        
    def add_article(self, article: Article):
        pass
    
    def add_author_edge(self, author: str):
        pass
    
    # initialize subgraph to surf thru up to some level d, such that
    # going from an article in context to any vertex not in user context
    # requires at most d jumps
    def initialize_graph(self, depth: int):
        pass
    
    def random_surf_iter(iter_limit: int, d=0.85):
        pass

def create_subgraph():
    pass

# TODO: change to work for subgraph - rewrite as method
def random_surfer_iter(
    iter_limit: int, conn: sqlite3.Connection, user_context, N: int, d=0.85):
    curr_article = user_context[-1]
    for _ in range(iter_limit):
        doi = curr_article.doi
        conn.execute(
        f'''
        UPDATE surfers
        SET score = score + 1
        WHERE doi = ?
        ''', (doi,)
        )
        p = random.uniform(0,1)
        if p > d:
            # jump (find from context)
            best_article = user_context[0]
            for article in user_context:
                if article.citations > best_article.citations:
                    best_article = article
            curr_article = best_article
        else:
            query = """
            SELECT DISTINCT p.title, p.citations, p.doi
            FROM publications_citations p
            INNER JOIN authored a1 ON p.doi = a1.doi
            WHERE a1.author IN (
                SELECT a2.author
                FROM authored a2
                WHERE a2.doi = ?
            )
            AND p.doi <> ?
            ORDER BY p.citations DESC
            """
            
            cursor = conn.execute(query, (doi, doi))
            row = cursor.fetchone()
            
            if row is not None:
                nbr_article = Article(title=row[0], doi=row[2], citations=int.from_bytes(row[1], byteorder='little'))
                curr_article = nbr_article
            
def pagerank_surfer(
    iter_limit_pr: int, conn: sqlite3.Connection, user_context, iter_limit: int, how_many: int):
    
    user_context_dois = [c.doi for c in user_context]
    
    surf_graph = create_subgraph()
    
    for _ in range(iter_limit_pr):
        random_surfer_iter(iter_limit=iter_limit, conn=conn, user_context=user_context, graph=surf_graph)
    
    dois_to_recom = []
            
    return dois_to_recom
            

def init(article: Article, db_name):            
    conn = sqlite3.connect(db_name)

    user_context = [article]
    dois_to_recom = pagerank_surfer(iter_limit=300, iter_limit_pr=2, conn=conn, user_context=user_context, how_many=10)
    print(dois_to_recom)
    conn.close()