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
    """
    Class fields:
        graph: adjacency list, stored as a dict from doi (str) to list of articles (Article)
        user_context: list of articles (Article)
        conn: sqlite3 Connection
        article_dict: dict from doi (str) to Article (used for counting surfs)
        depth: assuming some vertex vj not in user context it must be true that
        for some vertex vi in user context the shortest path from vi to vj satisfying
        len(d) <= depth
        new_articles: list of articles (Articles) to add in next iteration
        vertex_limit: max. no. of vertices (Article) that can be added to the graph
    """

    def __init__(
        self, user_context, conn: sqlite3.Connection, depth: int, vertex_limit: int
    ) -> None:
        """SurfGraph constructor.

        Args:
            user_context (_type_): _description_
            conn (sqlite3.Connection): _description_
            depth (int): _description_
            vertex_limit (int): _description_
        """
        self.graph = dict()
        self.user_context = user_context
        self.conn = conn
        self.article_dict = dict()
        self.depth = depth
        self.new_articles = []
        self.vertex_limit = vertex_limit

    def add_article(self, article: Article):
        """
        Adds article to graph - articles adjacent to the article to add 
        will be added to a list of neighbors of article in self.graph
        
        Args:
            article: Article - article to be added
        """
        self.graph[article.doi] = []
        self.article_dict[article.doi] = article

        query_redundant = """
            SELECT doi2, citations2
            FROM connections
            WHERE doi1 = ?
        """

        cursor = self.conn.execute(query_redundant, (article.doi,))

        rows = cursor.fetchall()

        # add all author edges
        if rows is not None:
            for row in rows:
                # if DOI doesn't exist add new article to dictionaries
                if row[0] not in self.graph:
                    nbr_article = Article(
                        title=None,
                        doi=row[0],
                        citations=int.from_bytes(row[1], byteorder="little"),
                    )
                    self.graph[article.doi].append(nbr_article)
                    self.new_articles.append(nbr_article)
                    self.article_dict[row[0]] = nbr_article
                else:
                    nbr_article = self.article_dict[row[0]]
                    self.graph[article.doi].append(nbr_article)

    def initialize_subgraph(self):
        """
        Initializes the subgraph to surf thru up to some level d,
        such that going from an article in context to any vertex
        not in user context requires at most d jumps.
        """
        article_counter = 0
        for article in self.user_context:
            self.add_article(article)
            article_counter += 1
            print(f"Article no. {article_counter} added")

        is_at_limit = False
        
        # outer loop guarantees the d jumps assumption
        for _ in range(self.depth):
            # copy so that the new_articles list can be cleared and used in the inner loop
            articles_in_this_iter = self.new_articles.copy()
            self.new_articles.clear()
            if is_at_limit:
                break
            
            # inner loop adds new articles and enforces vertex_limit assumption
            for article in articles_in_this_iter:
                self.add_article(article)
                article_counter += 1
                print(f"Article no. {article_counter} added")
                if self.vertex_limit is not None and article_counter > self.vertex_limit:
                    is_at_limit = True
                    break

        # sort keys for faster max-edge choosing in surfing phases
        for key in self.graph:
            neighbors = self.graph[key]
            neighbors.sort(key=lambda x: x.citations, reverse=True)

    def random_surf_iter(self, iter_limit: int, d=0.85):
        """Surfs over the graph up to iter_limit times, i.e.
        there is up to iter_limit jumps/surfs in this iteration.

        Args:
            iter_limit (int): how many times the surfer can move on a graph or jump to non-related edge in user context.
            d (float, optional): probability for surfer to go from neighbor to neighbor instead of jumping (1-d). Defaults to 0.85.
        """
        curr_article = self.user_context[-1]
        for _ in range(iter_limit):
            # update rank
            doi = curr_article.doi
            self.article_dict[doi].rank += 1

            p = random.uniform(0, 1)
            if p > d:
                # jump (find from context)
                best_article = self.user_context[0]
                for article in self.user_context:
                    if article.citations > best_article.citations:
                        best_article = article
                curr_article = best_article
            else:
                # jump to neighbor of current article with most citations
                neighbors = self.graph.get(doi, None)
                # if neighbor is not in the graph then it's meaningless to try to jump to them
                # (this can happen if we enforce vertex_limit and some vertices don't get added etc.)
                if neighbors:
                    best_neighbor = None
                    for neighbor in neighbors:
                        # check if this neighbor exists in graph
                        # first existent neighbor is the best because of previous sorting
                        if self.graph.get(neighbor.doi, None):
                            best_neighbor = neighbor
                            break
                    curr_article = best_neighbor
                    # handling situation where the was no best neighbor at all
                    # (I think it shouldn't really happen? But it doesn't change that much)
                    if best_neighbor is None:
                        curr_article = self.user_context[0]

    def get_best_results(self, amount: int):
        """Get a list of articles (Article) with the best score from PageRank surfing.

        Args:
            amount (int): how many top articles to include in a list

        Returns:
            _type_ (list<Article>): list of articles to recommend
        """
        article_list = list(self.article_dict.values())
        article_list.sort(key=lambda x: x.rank, reverse=True)
        to_name_list = article_list[:amount]
        for article in to_name_list:
            if article.title is None:
                cursor = self.conn.execute(
                    "SELECT title FROM publications_citations WHERE doi = ?", (article.doi,))
                row = cursor.fetchone()
                title = row[0]
                article.title = title
        return to_name_list


def pagerank_surfer(
    iter_limit_pr: int,
    conn: sqlite3.Connection,
    user_context,
    iter_limit: int,
    how_many_results: int,
    sg_depth: int,
    vertex_limit: int,
):
    """Helper method running PageRank surfer algorithm iter_limit_pr times, each iteration
    having iter_limit graph jumps/moves.

    Args:
        iter_limit_pr (int): how many times to perform a graph surfing method
        conn (sqlite3.Connection): db connection
        user_context (list<Article>): list of articles in user context
        iter_limit (int): how many times can a surfer move/jump in one iteration
        how_many_results (int): how many articles to return as recommendations
        sg_depth (int): graph depth for subgraph initializer
        vertex_limit (int): max. amount of vertices in graph for subgraph initializer

    Returns:
        _type_ (list<Article>): list of articles to recommend
    """
    surf_graph = SurfGraph(user_context, conn, sg_depth, vertex_limit)
    surf_graph.initialize_subgraph()

    for _ in range(iter_limit_pr):
        surf_graph.random_surf_iter(iter_limit)

    return surf_graph.get_best_results(how_many_results)


def init(article: Article, db_name):
    """For simple testing. Large values of sg_depth not recommended.

    Args:
        article (Article): article in user context
        db_name (_type_): sqlite db to connect to
    """
    conn = sqlite3.connect(db_name)

    user_context = [article]
    articles_to_recom = pagerank_surfer(
        iter_limit=300,
        iter_limit_pr=2,
        conn=conn,
        user_context=user_context,
        how_many_results=10,
        sg_depth=2,
        vertex_limit=1000,
    )
    
    for article in articles_to_recom:
        print(article.title)
    conn.close()
