import arxiv
import time

client = arxiv.Client(page_size=10)

start = time.time()
# Search for the 10 most recent articles matching the keyword "quantum."
search = arxiv.Search(
  query="cat:cs.AI",
  max_results=20,
  sort_by=arxiv.SortCriterion.SubmittedDate,
  sort_order=arxiv.SortOrder.Descending,
)

results = list(client.results(search))

print("length", len(results))
print("cost time", time.time() - start)











