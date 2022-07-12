import pandas as pd
from functions import search_imdb_for_movie_countries_genres_runtime, search_imdb_for_movie_credits_number
from multiprocessing import Pool

if __name__ == "__main__":
    ids: pd.DataFrame = pd.read_csv("data/movies_ids.csv", usecols=["ancine_id", "imdb_id"], index_col="ancine_id", dtype=str)

    with Pool(3) as pool:
        ids["imdb_credits_number"] = pool.map(search_imdb_for_movie_credits_number, ids["imdb_id"])

    with Pool(3) as pool:
        ids["imdb_countries_genres_runtime"] = pool.map(search_imdb_for_movie_countries_genres_runtime, ids["imdb_id"])

    ids["imdb_countries"] = ids["imdb_countries_genres_runtime"].apply(lambda dictionary: dictionary.get("countries"))

    ids["imdb_genres"] = ids["imdb_countries_genres_runtime"].apply(lambda dictionary: dictionary.get("genres"))

    ids["imdb_runtime"] = ids["imdb_countries_genres_runtime"].apply(lambda dictionary: dictionary.get("runtime"))

    ids["imdb_credits_number"] = ids["imdb_credits_number"].astype(pd.Int32Dtype())

    ids["imdb_genres"] = ids["imdb_genres"].str.replace("Sci-Fi", "Science Fiction")

    ids["imdb_genres"] = ids["imdb_genres"].str.lower()

    ids["imdb_runtime"] = ids["imdb_runtime"].astype(pd.Int16Dtype())

    ids: pd.DataFrame = ids.drop(columns=["imdb_id", "imdb_countries_genres_runtime"])

    ids: pd.DataFrame = ids.reindex(columns=ids.columns.sort_values())

    ids.to_csv("data/movies_details_imdb.csv")
