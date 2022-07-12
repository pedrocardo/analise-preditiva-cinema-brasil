import pandas as pd
from functions import (
    its_same_director,
    sanitize_director_name,
    sanitize_movie_title,
    search_tmdb_for_movie_director,
    search_tmdb_for_movie_imdb_id,
    search_tmdb_for_movie_tmdb_id,
)
from multiprocessing import cpu_count, Pool

if __name__ == "__main__":
    CPUS_NUMBER: int = cpu_count()

    directors_brazil: pd.DataFrame = pd.read_csv(
        "data_raw/ancine_directors_brazil.csv",
        sep=";",
        header=0,
        names=["ancine_id", "ancine_director"],
        usecols=[0, 2],
        dtype=str,
        encoding="mbcs",
    )

    directors_foreign: pd.DataFrame = pd.read_csv(
        "data_raw/ancine_directors_foreign.csv",
        sep=";",
        header=0,
        names=["ancine_id", "ancine_director"],
        usecols=[0, 2],
        dtype=str,
        encoding="mbcs",
    )

    movies: pd.DataFrame = pd.read_excel(
        "data_raw/ancine_movies.xlsx",
        header=2,
        names=["ancine_release_year", "ancine_title", "ancine_id", "ancine_release_date", "ancine_public"],
        index_col="ancine_id",
        usecols=[0, 1, 2, 6, 11],
        dtype={
            "ancine_release_year": pd.Int16Dtype(),
            "ancine_title": str,
            "ancine_id": str,
            "ancine_public": pd.Int32Dtype(),
        },
        skipfooter=24,
        na_values=["Sem CPB", "Sem ROE", "ND"],
        thousands=".",
    )

    movies: pd.DataFrame = movies[movies.index.notna()].copy()

    movies: pd.DataFrame = movies.dropna().query("ancine_release_date < Timestamp('2020-01-31')")

    movies["ancine_title"] = movies["ancine_title"].apply(sanitize_movie_title)

    directors: pd.DataFrame = pd.concat([directors_brazil, directors_foreign])

    directors["ancine_id"] = directors["ancine_id"].str.strip()

    directors: pd.DataFrame = directors.groupby("ancine_id").aggregate(" ".join)

    directors["ancine_director"] = directors["ancine_director"].apply(sanitize_director_name)

    movies: pd.DataFrame = movies.join(directors, "ancine_id")

    titles_and_years: pd.DataFrame = movies.loc[:, ["ancine_title", "ancine_release_year"]].copy()

    titles_and_years["ancine_release_year"] = titles_and_years["ancine_release_year"].astype(str)

    with Pool(CPUS_NUMBER) as pool:
        movies["tmdb_id"] = pool.starmap(search_tmdb_for_movie_tmdb_id, titles_and_years.values)

    mask: pd.Series = movies["tmdb_id"].isna()

    titles_and_years: pd.DataFrame = movies.loc[mask, ["ancine_title", "ancine_release_year"]].copy()

    titles_and_years["ancine_release_year"] -= 1

    titles_and_years["ancine_release_year"] = titles_and_years["ancine_release_year"].astype(str)

    with Pool(CPUS_NUMBER) as pool:
        movies.loc[mask, "tmdb_id"] = pool.starmap(search_tmdb_for_movie_tmdb_id, titles_and_years.values)

    with Pool(CPUS_NUMBER) as pool:
        movies["tmdb_director"] = pool.map(search_tmdb_for_movie_director, movies["tmdb_id"])

    movies["tmdb_director"] = movies["tmdb_director"].apply(sanitize_director_name)

    movies["same_director"] = movies[["ancine_director", "tmdb_director"]].apply(
        lambda directors: its_same_director(directors[0], directors[1]),
        axis="columns",
        raw=True,
    )

    movies.loc[~movies["same_director"], "tmdb_id"] = None

    manually_searched_tmdb_ids: pd.DataFrame = pd.read_csv("data_manually/manually_searched_tmdb_ids.csv", index_col="ancine_id", dtype=str)

    movies: pd.DataFrame = movies.fillna(manually_searched_tmdb_ids)

    with Pool(CPUS_NUMBER) as pool:
        movies["imdb_id"] = pool.map(search_tmdb_for_movie_imdb_id, movies["tmdb_id"])

    manually_searched_imdb_ids: pd.DataFrame = pd.read_csv("data_manually/manually_searched_imdb_ids.csv", index_col="ancine_id", dtype=str)

    movies: pd.DataFrame = movies.fillna(manually_searched_imdb_ids)

    movies[["tmdb_id", "imdb_id"]].to_csv("data/movies_ids.csv")
