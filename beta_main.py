import multiprocessing as mp
import time
import random

def process_query(data, query_type, query_param):

    if query_type == "positional":
        return handle_positional_query(data, query_param)
    elif query_type == "circular":
        return handle_circular_query(data, query_param)
    else:
      return "Unknown query type"


def handle_positional_query(data, query_param):
    if isinstance(query_param, int):
        try:
            return data[query_param]
        except IndexError:
             return "Out of range"
    elif isinstance(query_param, tuple) and len(query_param) == 2:
      start, end = query_param
      try:
        return data[start:end]
      except IndexError:
        return "Out of range"
    else:
        return "Wrong Format"
    
def handle_circular_query(data, query_param):
    data_len = len(data)
    if isinstance(query_param, int):
        if data_len == 0 : return "No Data"
        index = query_param % data_len
        return data[index]
    elif isinstance(query_param, tuple) and len(query_param) == 2:
        if data_len == 0 : return "No Data"
        start, end = query_param
        start_index = start % data_len
        end_index = end % data_len
        if start_index < end_index:
            return data[start_index:end_index]
        else: # jika index berakhir sebelum index mulai, ambil dari start_index sampai akhir, dan dari awal sampai end_index
            return data[start_index:] + data[:end_index]
    else:
        return "Wrong Format"


def generate_queries(data_len, min_queries=3, max_queries=7):
    num_queries = random.randint(min_queries, max_queries)  # Jumlah kueri random
    queries = []

    for _ in range(num_queries):
        query_type = random.choice(["positional", "circular"]) # jenis kueri random

        if query_type == "positional":
             # variasi positional (integer or tuple)
            if random.random() < 0.5:  # 50% kemungkinan integer
                query_param = random.randint(0, 2*data_len) # bisa melebihi panjang data
            else: # sisanya tuple
                start = random.randint(0, 2*data_len)
                end = random.randint(start, 2*data_len)
                query_param = (start, end)
        else: # query_type == "circular"
            if random.random() < 0.5:  # 50% kemungkinan integer
               query_param = random.randint(0, 2*data_len)
            else: # sisanya tuple
                start = random.randint(0, 2*data_len)
                end = random.randint(start, 2*data_len)
                query_param = (start, end)

        queries.append((query_type, query_param))
    return queries

def run_queries_parallel(data, queries):
    with mp.Pool(processes=mp.cpu_count()) as pool:
      results = pool.starmap(process_query, [(data, q_type, q_param) for q_type, q_param in queries])
    return results


if __name__ == "__main__":
    # Contoh Data (bisa berupa list, string, atau lainnya)
    data_lengths = [1, 5, 10, 20, 100, 500, 1000] # contoh panjang data
    for data_len in data_lengths:
      data = list(range(1, data_len+1)) # daftar angka dari 1 sampai data_len
      print("\n========================================================")
      print(f"Data dengan panjang {data_len}: {data}")
      print("--------------------------------------------------------")
      
      # Generate Kueri secara otomatis
      min_queries = data_len//10 if data_len > 10 else 3
      max_queries=data_len//5 if data_len > 5 else 7
      
      # Query processing
      min_queries = max(1, min(min_queries, max_queries))
      max_queries = max(1, max(min_queries, max_queries))


      queries = generate_queries(len(data), min_queries=min_queries, max_queries=max_queries)
    
      print("Menjalankan Kueri Paralel...")
      start_time = time.time()
      results = run_queries_parallel(data, queries)
      end_time = time.time()

      print("\nHasil Kueri:")
      for i, (q, result) in enumerate(zip(queries, results)):
          print(f"Kueri {i+1}: {q}, Hasil: {result}")
      print(f"\nWaktu Eksekusi: {end_time-start_time:.4f} detik")
