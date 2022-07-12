import pandas as pd

if __name__ == "__main__":
    movies: pd.DataFrame = pd.read_excel(
        "data_raw/ancine_movies.xlsx",
        header=2,
        names=["ancine_id", "ancine_release_date", "ancine_public"],
        index_col="ancine_id",
        usecols=[2, 6, 11],
        dtype={
            "ancine_release_year": pd.Int16Dtype(),
            "ancine_id": str,
            "ancine_public": pd.Int32Dtype(),
        },
        skipfooter=24,
        na_values=["Sem CPB", "Sem ROE", "ND"],
        thousands=".",
    )

    details_classind: pd.DataFrame = pd.read_csv(
        "data/movies_details_classind.csv",
        index_col="ancine_id",
        dtype=str,
    )

    details_distributors: pd.DataFrame = pd.read_csv(
        "data/movies_details_distributors.csv",
        index_col="ancine_id",
        dtype={
            "distributor_origin": str,
            "distributor_movies_shown": pd.Int16Dtype(),
        },
    )

    details_imdb: pd.DataFrame = pd.read_csv(
        "data/movies_details_imdb.csv",
        index_col="ancine_id",
        dtype={
            "imdb_countries": str,
            "imdb_credits_number": pd.Int32Dtype(),
            "imdb_genres": str,
            "imdb_runtime": pd.Int16Dtype(),
        },
    )

    details_tmdb: pd.DataFrame = pd.read_csv(
        "data/movies_details_tmdb.csv",
        index_col="ancine_id",
        dtype={
            "tmdb_countries": str,
            "tmdb_credits_number": pd.Int32Dtype(),
            "tmdb_genres": str,
            "tmdb_runtime": pd.Int16Dtype(),
        },
    )

    movies: pd.DataFrame = movies[movies.index.notna()]

    movies: pd.DataFrame = movies.dropna().query("ancine_release_date < Timestamp('2020-01-31')")

    movies: pd.DataFrame = movies.join(details_classind)

    movies: pd.DataFrame = movies.join(details_distributors)

    movies: pd.DataFrame = movies.join(details_imdb)

    movies: pd.DataFrame = movies.join(details_tmdb)

    movies["release_month"] = movies["ancine_release_date"].dt.month

    movies["credits_number"] = movies["imdb_credits_number"].copy()

    mask = movies["credits_number"] < movies["tmdb_credits_number"]

    movies.loc[mask, "credits_number"] = movies.loc[mask, "tmdb_credits_number"].copy()

    movies["runtime"] = movies["imdb_runtime"].copy()

    mask = movies["runtime"].isna()

    movies.loc[mask, "runtime"] = movies.loc[mask, "tmdb_runtime"]

    def unite_semicolon_strings(string_1: str, string_2: str) -> str | None:
        if isinstance(string_1, str) and isinstance(string_2, str):
            string_1: str = string_1.split(";")
            string_2: str = string_2.split(";")
            strings: list[str] = sorted(set(string_1 + string_2))
            return ";".join(strings)
        else:
            return string_1 or string_2

    movies["countries"] = movies[["imdb_countries", "tmdb_countries"]].apply(
        lambda countries: unite_semicolon_strings(countries[0], countries[1]),
        axis="columns",
        raw=True,
    )

    movies["genres"] = movies[["imdb_genres", "tmdb_genres"]].apply(
        lambda genres: unite_semicolon_strings(genres[0], genres[1]),
        axis="columns",
        raw=True,
    )

    movies: pd.DataFrame = movies.rename(
        columns={
            "ancine_public": "public",
            "classind_rating": "rating",
        }
    )

    movies: pd.DataFrame = movies.drop(
        columns=[
            "ancine_release_date",
            "imdb_countries",
            "imdb_credits_number",
            "imdb_genres",
            "imdb_runtime",
            "tmdb_countries",
            "tmdb_credits_number",
            "tmdb_genres",
            "tmdb_runtime",
        ]
    )

    movies: pd.DataFrame = movies.reindex(columns=movies.columns.sort_values())

    movies: pd.DataFrame = movies.dropna()

    movies.to_excel("movies_details.xlsx")
