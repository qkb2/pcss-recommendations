import sqlite3
import random

class Article:
    def __init__(self, doi, citations, title) -> None:
        self.doi = doi
        self.citations = citations
        self.title = title
        self.rank = 0
      
# code is still too slow
class SurfGraph:
    def __init__(self, user_context, conn: sqlite3.Connection, depth: int, vertex_limit: int) -> None:
        self.graph = dict()
        self.user_context = user_context
        self.conn = conn
        self.article_dict = dict()
        self.depth = depth
        self.new_articles = []
        self.vertex_limit = vertex_limit
        
    def add_article(self, article: Article):
        self.graph[article.doi] = []
        self.article_dict[article.doi] = article
        
        query_small = """
            SELECT a1.doi
            FROM authored a1
            WHERE a1.author IN (
                SELECT a2.author
                FROM authored a2
                WHERE a2.doi = ? 
            )
        """
            
        cursor = self.conn.execute(query_small, (article.doi,))
        
        rows = cursor.fetchall()
        
        # add all author edges
        if rows is not None:
            for row in rows:
                if row[0] not in self.article_dict:
                    cursor_inner = self.conn.execute(
                        "SELECT title, citations FROM publications_citations WHERE doi = ?", (row[0],))
                    row_inner = cursor_inner.fetchone()
                    if row_inner is not None:
                        nbr_article = Article(
                            title=row_inner[0], doi=row[0], citations=int.from_bytes(row_inner[1], byteorder='little'))
                        self.graph[article.doi].append(nbr_article)
                        self.new_articles.append(nbr_article)
                        self.article_dict[row[0]] = nbr_article
                else:
                    nbr_article = self.article_dict[row[0]]
                    self.graph[article.doi].append(nbr_article)
        
    
    # initialize subgraph to surf thru up to some level d, such that
    # going from an article in context to any vertex not in user context
    # requires at most d jumps
    def initialize_subgraph(self):
        article_counter = 0
        for article in self.user_context:
            self.add_article(article)
            article_counter += 1
            print(f"Article no. {article_counter} added")
        
        is_at_limit = False
        for _ in range(self.depth):
            articles_in_this_iter = self.new_articles.copy()
            self.new_articles.clear()
            if is_at_limit:
                break
            for article in articles_in_this_iter:
                self.add_article(article)
                article_counter += 1
                print(f"Article no. {article_counter} added")
                if article_counter > self.vertex_limit:
                    is_at_limit = True
                    break
                
        for key in self.graph:
            neighbors = self.graph[key]
            neighbors.sort(key=lambda x: x.citations, reverse=True)
    
    def random_surf_iter(self, iter_limit: int, d=0.85):
            curr_article = self.user_context[-1]
            for _ in range(iter_limit):
                # update rank
                doi = curr_article.doi
                self.article_dict[doi].rank += 1
                
                p = random.uniform(0,1)
                if p > d:
                    # jump (find from context)
                    best_article = self.user_context[0]
                    for article in self.user_context:
                        if article.citations > best_article.citations:
                            best_article = article
                    curr_article = best_article
                else:
                    # jump to neighbor of current article with most citations                   
                    neighbors = self.graph[doi]
                    best_neighbor = neighbors[0]
                    curr_article = best_neighbor
    
    def get_best_results(self, amount: int):
        article_list = list(self.article_dict.values())
        article_list.sort(key=lambda x: x.rank, reverse=True)
        return article_list[:amount]
            
def pagerank_surfer(
    iter_limit_pr: int, conn: sqlite3.Connection, user_context, 
    iter_limit: int, how_many_results: int, sg_depth: int, vertex_limit: int):
        
    surf_graph = SurfGraph(user_context, conn, sg_depth, vertex_limit)
    surf_graph.initialize_subgraph()
    
    for _ in range(iter_limit_pr):
        surf_graph.random_surf_iter(iter_limit)
    
    return surf_graph.get_best_results(how_many_results)
            

def init(article: Article, db_name):            
    conn = sqlite3.connect(db_name)

    user_context = [article]
    dois_to_recom = pagerank_surfer(
        iter_limit=300, iter_limit_pr=2, conn=conn, user_context=user_context, how_many_results=10, sg_depth=2, vertex_limit=20)
    print(dois_to_recom)
    conn.close()