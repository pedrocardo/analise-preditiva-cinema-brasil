import pandas as pd
from functions import search_tmdb_for_movie_countries_genres_runtime, search_tmdb_for_movie_credits_number
from multiprocessing import Pool, cpu_count

if __name__ == "__main__":
    CPUS_NUMBER: int = cpu_count()

    ids: pd.DataFrame = pd.read_csv("data/movies_ids.csv", usecols=["ancine_id", "tmdb_id"], index_col="ancine_id", dtype=str)

    with Pool(CPUS_NUMBER) as pool:
        ids["tmdb_credits_number"] = pool.map(search_tmdb_for_movie_credits_number, ids["tmdb_id"])

    with Pool(CPUS_NUMBER) as pool:
        ids["tmdb_countries_genres_runtime"] = pool.map(search_tmdb_for_movie_countries_genres_runtime, ids["tmdb_id"])

    ids["tmdb_countries"] = ids["tmdb_countries_genres_runtime"].apply(lambda dictionary: dictionary.get("countries"))

    ids["tmdb_genres"] = ids["tmdb_countries_genres_runtime"].apply(lambda dictionary: dictionary.get("genres"))

    ids["tmdb_runtime"] = ids["tmdb_countries_genres_runtime"].apply(lambda dictionary: dictionary.get("runtime"))

    ids["tmdb_credits_number"] = ids["tmdb_credits_number"].astype(pd.Int32Dtype())

    ids["tmdb_genres"] = ids["tmdb_genres"].str.lower()

    ids["tmdb_runtime"] = ids["tmdb_runtime"].astype(pd.Int16Dtype())

    ids: pd.DataFrame = ids.drop(columns=["tmdb_id", "tmdb_countries_genres_runtime"])

    ids: pd.DataFrame = ids.reindex(columns=ids.columns.sort_values())

    ids.to_csv("data/movies_details_tmdb.csv")
