import pandas as pd

if __name__ == "__main__":
    distributors: pd.DataFrame = pd.read_csv(
        "data_raw/ancine_distributors.csv",
        sep=";",
        header=2,
        names=["year", "name", "distributor_origin", "distributor_movies_exhibited"],
        usecols=[0, 1, 3, 8],
        dtype={
            "year": pd.Int16Dtype(),
            "name": str,
            "distributor_origin": str,
            "distributor_movies_exhibited": pd.Int16Dtype(),
        },
        engine="python",
        skipfooter=18,
        thousands=".",
        encoding="mbcs",
    )

    movies: pd.DataFrame = pd.read_excel(
        "data_raw/ancine_movies.xlsx",
        header=2,
        names=["ancine_release_year", "ancine_id", "ancine_release_date", "ancine_distributor", "ancine_public"],
        usecols=[0, 2, 6, 7, 11],
        index_col="ancine_id",
        dtype={
            "ancine_release_year": pd.Int16Dtype(),
            "ancine_id": str,
            "ancine_distributor": str,
            "ancine_public": pd.Int32Dtype(),
        },
        skipfooter=24,
        na_values=["Sem CPB", "Sem ROE", "ND"],
        thousands=".",
    )

    movies: pd.DataFrame = movies[movies.index.notna()].copy()

    movies: pd.DataFrame = movies.dropna().query("ancine_release_date < Timestamp('2020-01-31')")

    movies["ancine_distributor"] = movies["ancine_distributor"].str.lower()

    distributors["name"] = distributors["name"].str.lower()

    distributors["distributor_movies_exhibited"] = distributors["distributor_movies_exhibited"].groupby(distributors["name"]).cumsum()

    old_origins: list[str] = ["Distribuição Nacional", "Distribuição Internacional", "Codistribuição Internacional-Nacional"]

    new_origins: list[str] = ["national", "international", "collaboration"]

    distributors["distributor_origin"] = distributors["distributor_origin"].replace(old_origins, new_origins)

    distributors: pd.DataFrame = distributors.set_index(["year", "name"])

    movies: pd.DataFrame = movies.join(distributors, on=["ancine_release_year", "ancine_distributor"])

    movies[["distributor_movies_exhibited", "distributor_origin"]].to_csv("data/movies_details_distributors.csv")
