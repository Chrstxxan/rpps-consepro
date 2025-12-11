# core/parallel_runner.py
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_discovery_parallel(sites, crawl_func, workers=4):
    """
    Roda múltiplos crawl_site() em paralelo.
    - sites: lista vinda do seu app.py (cada item é um dict com name, url, uf, etc)
    - crawl_func: referência para discovery.crawl_site
    - workers: quantos sites rodar simultaneamente
    """
    results = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(crawl_func, site["url"]): site
            for site in sites
        }

        for fut in as_completed(future_map):
            site = future_map[fut]
            try:
                links = fut.result()
            except Exception as e:
                print(f"[PARALLEL][ERRO] {site['name']}: {e}")
                links = []

            results[site["name"]] = links

    return results

# ===============================================================
# PIPELINE STREAMING — Discovery em paralelo entregando resultados
# assim que cada site termina (não bloqueia tudo antes).
# ===============================================================
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_discovery_streaming(rpps_sites, discovery_fn, workers=4):
    """
    Executa discovery em paralelo, mas vai liberando cada resultado
    assim que fica pronto — streaming.
    """
    executor = ThreadPoolExecutor(max_workers=workers)

    futures = {
        executor.submit(discovery_fn, site["url"]): site
        for site in rpps_sites
    }

    for future in as_completed(futures):
        site = futures[future]
        try:
            links = future.result()
        except Exception as e:
            print(f"[DISCOVERY][ERRO] {site['name']}: {e}")
            links = []
        yield site, links
