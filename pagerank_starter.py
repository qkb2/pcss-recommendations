import time
from pagerank_subgraph import init, Article

context = [
    Article(
        doi="10.1089/10665270050081478",
        title="A Greedy Algorithm for Aligning DNA Sequences.",
        citations=3866,
    )
]
t0 = time.perf_counter_ns()
init(
    context,
    "recom_db3.db",
)
t1 = time.perf_counter_ns()
t = t1 - t0
for a in context:
    print(a.title)
print(f"Time taken: {t/1000000000}s")

context = [
    Article(
        doi="10.1093/bioinformatics/btn604",
        title="CleaveLand: a pipeline for using degradome data to find cleaved small RNA targets.",
        citations=521,
    )
]
t0 = time.perf_counter_ns()
init(
    context,
    "recom_db3.db",
)
t1 = time.perf_counter_ns()
t = t1 - t0
for a in context:
    print(a.title)
print(f"Time taken: {t/1000000000}s")

context = [
    Article(
        doi="10.1089/10665270050081478",
        title="A Greedy Algorithm for Aligning DNA Sequences.",
        citations=3866,
    ),
    Article(
        doi="10.1093/bioinformatics/btn604",
        title="CleaveLand: a pipeline for using degradome data to find cleaved small RNA targets.",
        citations=521,
    ),
]
t0 = time.perf_counter_ns()
init(
    context,
    "recom_db3.db",
)
t1 = time.perf_counter_ns()
t = t1 - t0
for a in context:
    print(a.title)
print(f"Time taken: {t/1000000000}s")

context = [
    Article(
        doi="10.1186/1471-2105-5-113",
        title="MUSCLE: a multiple sequence alignment method with reduced time and space complexity.",
        citations=6819,
    )
]
t0 = time.perf_counter_ns()
init(
    context,
    "recom_db3.db",
)
t1 = time.perf_counter_ns()
t = t1 - t0
for a in context:
    print(a.title)
print(f"Time taken: {t/1000000000}s")

context = [
    Article(
        doi="10.1007/s00422-006-0139-8",
        title="Neuronal firing rates account for distractor effects on mnemonic accuracy in a visuo-spatial working memory task.",
        citations=17,
    )
]
t0 = time.perf_counter_ns()
init(
    context,
    "recom_db3.db",
)
t1 = time.perf_counter_ns()
t = t1 - t0
for a in context:
    print(a.title)
print(f"Time taken: {t/1000000000}s")
