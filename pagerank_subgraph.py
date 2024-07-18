import sqlite3
import random

class Article:
    def __init__(self, doi, citations, title) -> None:
        self.doi = doi
        self.citations = citations
        self.title = title
        self.rank = 0
      
class SurfGraph:
    def __init__(self, user_context, conn: sqlite3.Connection, depth: int) -> None:
        self.graph = dict()
        self.user_context = user_context
        self.conn = conn
        self.article_list = dict()
        self.depth = depth
        self.new_articles = []
        
    def add_article(self, article: Article):
        self.graph[article.doi] = []
        self.article_list[article.doi] = article
        
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
            
        cursor = self.conn.execute(query, (article.doi, article.doi))
        
        rows = cursor.fetchall()
        
        # add all author edges
        if rows is not None:
            for row in rows:
                nbr_article = Article(
                    title=row[0], doi=row[2], citations=int.from_bytes(row[1], byteorder='little'))
                self.graph[article.doi].append(nbr_article)
                if nbr_article.doi not in self.article_list:
                    self.new_articles.append(nbr_article)
        
    
    # initialize subgraph to surf thru up to some level d, such that
    # going from an article in context to any vertex not in user context
    # requires at most d jumps
    def initialize_subgraph(self):
        for article in self.user_context:
            self.add_article(article)
        
        for _ in range(self.depth):
            articles_in_this_iter = self.new_articles.copy()
            self.new_articles.clear()
            for article in articles_in_this_iter:
                self.add_article(article)
    
    def random_surf_iter(self, iter_limit: int, d=0.85):
            curr_article = self.user_context[-1]
            for _ in range(iter_limit):
                # update rank
                doi = curr_article.doi
                self.article_list[doi].rank += 1
                
                p = random.uniform(0,1)
                if p > d:
                    # jump (find from context)
                    best_article = self.user_context[0]
                    for article in self.user_context:
                        if article.citations > best_article.citations:
                            best_article = article
                    curr_article = best_article
                else:
                    # TODO: change to work on graph - jump to neighbor of current article with most citations
                    
                    if row is not None:
                        nbr_article = Article(title=row[0], doi=row[2], citations=int.from_bytes(row[1], byteorder='little'))
                        curr_article = nbr_article
    
    def get_best_results(self, amount: int):
        pass
            
def pagerank_surfer(
    iter_limit_pr: int, conn: sqlite3.Connection, user_context, iter_limit: int, how_many_results: int, sg_depth: int):
        
    surf_graph = SurfGraph(user_context, conn, sg_depth)
    surf_graph.initialize_subgraph()
    
    for _ in range(iter_limit_pr):
        surf_graph.random_surf_iter(iter_limit)
    
    return surf_graph.get_best_results(how_many_results)
            

def init(article: Article, db_name):            
    conn = sqlite3.connect(db_name)

    user_context = [article]
    dois_to_recom = pagerank_surfer(
        iter_limit=300, iter_limit_pr=2, conn=conn, user_context=user_context, how_many_results=10, sg_depth=5)
    print(dois_to_recom)
    conn.close()