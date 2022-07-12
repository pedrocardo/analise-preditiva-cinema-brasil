import pandas as pd
import re
from functions import sanitize_movie_title, sanitize_director_name, its_same_director

if __name__ == "__main__":
    classind: pd.DataFrame = pd.read_csv(
        "data_raw/classind.csv",
        header=None,
        names=["classind_id", "classind_title", "classind_director", "category", "market"],
        usecols=[1, 2, 7, 11, 13],
        dtype=str,
    )

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

    movies: pd.DataFrame = movies.join(directors, on="ancine_id")

    classind["classind_title"] = classind["classind_title"].apply(sanitize_movie_title)

    classind["classind_director"] = classind["classind_director"].apply(sanitize_director_name)

    classind["category"] = classind["category"].str.strip()

    classind["market"] = classind["market"].str.strip()

    categories: list[str] = ["Curta Metragem", "Documentário", "Filme", "Longa Metragem e Trailer", "Longa Metragem", "Média Metragem"]

    classind: pd.DataFrame = classind.query("category in @categories and market == 'Cinema'")

    classind: pd.DataFrame = classind.drop_duplicates("classind_id")

    classind: pd.DataFrame = classind.join(
        movies.reset_index().set_index("ancine_title"),
        on="classind_title",
        how="inner",
    )

    classind["same_director"] = classind[["classind_director", "ancine_director"]].apply(
        lambda directors: its_same_director(directors[0], directors[1]),
        axis="columns",
        raw=True,
    )

    classind: pd.DataFrame = classind.loc[classind["same_director"], ["classind_id", "ancine_id"]].copy()

    classind: pd.DataFrame = classind.drop_duplicates("ancine_id", keep=False).set_index("ancine_id")

    movies: pd.DataFrame = movies.join(classind, on="ancine_id")

    manually_searched_classind_ids: pd.DataFrame = pd.read_csv(
        "data_manually/manually_searched_classind_ids.csv",
        index_col="ancine_id",
        dtype=str,
    )

    movies: pd.DataFrame = movies.fillna(manually_searched_classind_ids)

    classind: pd.DataFrame = pd.read_csv(
        "data_raw/classind.csv",
        header=None,
        index_col="classind_id",
        usecols=[1, 16],
        names=["classind_id", "classind_rating"],
        dtype={
            "classind_id": str,
            "classind_rating": str,
        },
    )

    classind: pd.DataFrame = classind[~classind.index.duplicated(keep="last")]

    movies: pd.DataFrame = movies.join(classind, "classind_id")

    def sanitize_classind_rating(rating: str) -> str | None:
        if not isinstance(rating, str):
            return None
        rating_match: re.Match | None = re.search(r"[0-9]{2}", rating)
        rating: str | None = rating_match.group() if rating_match else None
        return rating if rating else "all ages"

    movies["classind_rating"] = movies["classind_rating"].apply(sanitize_classind_rating)

    movies["classind_rating"].to_csv("data/movies_details_classind.csv")
