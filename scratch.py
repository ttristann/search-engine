# from multiprocessing import Manager

# def director(query, inverted_index, docId_dict, num_processes=4):
#     """
#     Director function that spawns processes to handle subqueries.
#     """
#     # Step 1: Tokenize the query
#     search_query = SearchQuery(query)
#     search_query.tokenize_query()
#     query_tokens = search_query.get_query_tokens()

#     # Step 2: Split the query into subqueries
#     subqueries = split_query(query_tokens, num_processes)

#     # Step 3: Split the index for each process
#     index_parts = [{} for _ in range(num_processes)]
#     for term, postings in inverted_index.items():
#         index_parts[hash(term) % num_processes][term] = postings

#     # Step 4: Set up multiprocessing
#     manager = Manager()
#     result_dict = manager.dict()  # Shared dictionary for storing results
#     processes = []

#     # Step 5: Spawn worker processes
#     for i in range(num_processes):
#         process = Process(
#             target=process_subquery,
#             args=(subqueries[i], index_parts[i], docId_dict, result_dict, i)
#         )
#         processes.append(process)
#         process.start()

#     # Step 6: Wait for all processes to finish
#     for process in processes:
#         process.join()

#     # Step 7: Merge results from all processes
#     merged_results = {}
#     for process_id, results in result_dict.items():
#         for docID, score in results.items():
#             merged_results[docID] = merged_results.get(docID, 0) + score

#     # Step 8: Sort results by score
#     sorted_results = sorted(merged_results.items(), key=lambda x: x[1], reverse=True)

#     return sorted_results
