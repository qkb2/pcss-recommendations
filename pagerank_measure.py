import time
from pagerank import init, Article

t0 = time.perf_counter_ns()
init(Article(doi='10.1089/10665270050081478', title='A Greedy Algorithm for Aligning DNA Sequences.', citations=3866), 'recom_db2')
t1 = time.perf_counter_ns()
t = t1 - t0
print(f'Time taken: {t/1000000000}s')