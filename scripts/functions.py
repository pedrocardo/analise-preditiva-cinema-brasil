import bs4
import json
import os
import pandas as pd
import re
import requests
import unidecode
from dotenv import load_dotenv

load_dotenv()


def sanitize_director_name(name: str) -> str | None:
    if not isinstance(name, str):
        return None
    name: str = name.lower()
    name: str = unidecode.unidecode(name)
    name: str = re.sub(r"[^a-z]", " ", name)
    name: str = re.sub(r"\b[a-z]{1,2}\b", "", name)
    name: str = re.sub(r"\s{2,}", " ", name)
    name: str = name.strip()
    return name if name else None


def sanitize_movie_title(title: str) -> str | None:
    if not isinstance(title, str):
        return None
    title: str = title.lower()
    title: str = unidecode.unidecode(title)
    title: str = re.sub(r"[^a-z0-9]", " ", title)
    title: str = re.sub(r"\s{2,}", " ", title)
    title: str = title.strip()
    return title if title else None


def its_same_director(director_1: str, director_2: str) -> bool:
    if not (isinstance(director_1, str) and isinstance(director_2, str)):
        return False
    names_1: set[str] = set(director_1.split(" "))
    names_2: set[str] = set(director_2.split(" "))
    return not names_1.isdisjoint(names_2)


def get_tmdb_search_movie(title: str, year: str) -> pd.DataFrame:
    if not (isinstance(title, str) and isinstance(year, str)):
        return pd.DataFrame(None)
    URL: str = "https://api.themoviedb.org/3/search/movie"
    PARAMETERS: dict = {
        "api_key": os.getenv("TMDB_API_KEY"),
        "query": title,
        "region": "BR",
        "year": year,
    }
    response: requests.Response = requests.get(URL, PARAMETERS)
    is_json: bool = "application/json" in response.headers["Content-Type"]
    response_body: dict = response.json() if is_json else dict()
    movies_found: list[dict] | None = response_body.get("results")
    return pd.DataFrame(movies_found)


def get_tmdb_movie_details(tmdb_id: str) -> dict:
    if not isinstance(tmdb_id, str):
        return dict()
    URL: str = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    PARAMETERS: dict = {
        "api_key": os.getenv("TMDB_API_KEY"),
    }
    response: requests.Response = requests.get(URL, PARAMETERS)
    is_json: bool = "application/json" in response.headers["Content-Type"]
    return response.json() if is_json else dict()


def get_tmdb_movie_external_ids(tmdb_id: str) -> dict:
    if not isinstance(tmdb_id, str):
        return dict()
    URL: str = f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids"
    PARAMETERS: dict = {
        "api_key": os.getenv("TMDB_API_KEY"),
    }
    response: requests.Response = requests.get(URL, PARAMETERS)
    is_json: bool = "application/json" in response.headers["Content-Type"]
    return response.json() if is_json else dict()


def get_tmdb_movie_credits(tmdb_id: str) -> pd.DataFrame:
    if not isinstance(tmdb_id, str):
        return pd.DataFrame(None)
    URL: str = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
    PARAMETERS: dict = {
        "api_key": os.getenv("TMDB_API_KEY"),
    }
    response: requests.Response = requests.get(URL, PARAMETERS)
    is_json: bool = "application/json" in response.headers["Content-Type"]
    response_content: dict = response.json() if is_json else dict()
    cast: pd.DataFrame = pd.DataFrame(response_content.get("cast"))
    crew: pd.DataFrame = pd.DataFrame(response_content.get("crew"))
    return pd.concat([cast, crew])


def get_imdb_page_movie(imdb_id: str) -> str:
    if not isinstance(imdb_id, str):
        return str()
    URL: str = f"https://www.imdb.com/title/{imdb_id}"
    response: requests.Response = requests.get(URL)
    response.raise_for_status()
    is_html: bool = "text/html" in response.headers["Content-Type"]
    return response.text if is_html else str()


def get_imdb_page_movie_credits(imdb_id: str) -> str:
    if not isinstance(imdb_id, str):
        return str()
    URL: str = f"https://www.imdb.com/title/{imdb_id}/fullcredits"
    response: requests.Response = requests.get(URL)
    response.raise_for_status()
    is_html: bool = "text/html" in response.headers["Content-Type"]
    return response.text if is_html else str()


def filter_movie_by_year(movies: pd.DataFrame, year: str) -> pd.Series:
    if movies.empty or "release_date" not in movies.columns:
        return pd.Series(None, dtype=object)
    mask: pd.Series = movies["release_date"].str.contains(year, regex=False)
    if mask.any():
        return movies.loc[mask.idxmax()]
    else:
        return pd.Series(None, dtype=object)


def filter_credits_by_job(crew: pd.DataFrame, job: str) -> pd.DataFrame:
    if crew.empty or "job" not in crew.columns:
        return pd.DataFrame(None)
    mask: pd.Series = crew["job"] == job
    if mask.any():
        return crew.loc[mask]
    else:
        return pd.DataFrame(None)


def extract_tmdb_movie_countries(movie_details: dict) -> str | None:
    if not isinstance(movie_details, dict):
        return None
    countries: list[dict] | None = movie_details.get("production_countries")
    countries_codes: list[str] | None = [country.get("iso_3166_1") for country in countries] if countries else None
    return ";".join(countries_codes) if countries_codes else None


def extract_tmdb_movie_genres(movie_details: dict) -> str | None:
    if not isinstance(movie_details, dict):
        return None
    genres: list[dict] | None = movie_details.get("genres")
    genres_names: list[str] = [genre.get("name") for genre in genres] if genres else None
    return ";".join(genres_names) if genres_names else None


def extract_tmdb_movie_runtime(movie_details: dict) -> int | None:
    if not isinstance(movie_details, dict):
        return None
    runtime: int | None = movie_details.get("runtime")
    return runtime if runtime else None


def extract_imdb_movie_countries(movie_page: bs4.BeautifulSoup) -> str | None:
    countries: list[str] = re.findall(r"country_of_origin=[A-Z]{2}", str(movie_page))
    countries_codes: list[str] | None = [country[-2:] for country in countries] if countries else None
    return ";".join(countries_codes) if countries_codes else None


def extract_imdb_movie_genres(movie_page: bs4.BeautifulSoup) -> str | None:
    script: bs4.Tag | None = movie_page.find("script", {"type": "application/ld+json"})
    script_content: dict = json.loads(script.text) if script else dict()
    genres: list[str] | None = script_content.get("genre")
    return ";".join(genres) if genres else None


def extract_imdb_movie_runtime(movie_page: bs4.BeautifulSoup) -> int | None:
    script: bs4.Tag | None = movie_page.find("script", {"type": "application/ld+json"})
    script_content: dict = json.loads(script.text) if script else dict()
    runtime: str | None = script_content.get("duration")
    return int(pd.Timedelta(runtime).seconds / 60) if runtime else None


def extract_imdb_movie_credits_number(credits_page: bs4.BeautifulSoup) -> int | None:
    if not isinstance(credits_page, bs4.BeautifulSoup):
        return None
    crew: set = set(credits_page.find_all("td", {"class": "name"}))
    cast: set = set(credits_page.find_all("td", {"class": "primary_photo"}))
    credits_number: int = len(cast) + len(crew)
    return credits_number if credits_number else None


def search_tmdb_for_movie_tmdb_id(title: str, year: str) -> str | None:
    movies_found: pd.DataFrame = get_tmdb_search_movie(title, year)
    movie: pd.Series = filter_movie_by_year(movies_found, year)
    id: int | None = movie.get("id")
    return str(id) if id else None


def search_tmdb_for_movie_director(tmdb_id: str) -> str | None:
    credits: pd.DataFrame = get_tmdb_movie_credits(tmdb_id)
    director: pd.DataFrame = filter_credits_by_job(credits, "Director")
    director_name: pd.Series | None = director.get("name")
    if isinstance(director_name, pd.Series) and not director_name.empty:
        return director_name.to_string()


def search_tmdb_for_movie_imdb_id(tmdb_id: str) -> str | None:
    external_ids: dict = get_tmdb_movie_external_ids(tmdb_id)
    imdb_id: str | None = external_ids.get("imdb_id")
    return imdb_id if imdb_id else None


def search_tmdb_for_movie_credits_number(tmdb_id: str) -> int | None:
    credits: pd.DataFrame = get_tmdb_movie_credits(tmdb_id)
    credits_id: pd.Series | None = credits.get("id")
    if isinstance(credits_id, pd.Series):
        return len(credits_id.unique())
    else:
        return None


def search_tmdb_for_movie_countries_genres_runtime(tmdb_id: str) -> dict:
    movie_details: dict = get_tmdb_movie_details(tmdb_id)
    return {
        "countries": extract_tmdb_movie_countries(movie_details),
        "genres": extract_tmdb_movie_genres(movie_details),
        "runtime": extract_tmdb_movie_runtime(movie_details),
    }


def search_imdb_for_movie_countries_genres_runtime(imdb_id: str) -> dict:
    movie_page: str = get_imdb_page_movie(imdb_id)
    soup: bs4.BeautifulSoup = bs4.BeautifulSoup(movie_page, "lxml")
    return {
        "countries": extract_imdb_movie_countries(soup),
        "genres": extract_imdb_movie_genres(soup),
        "runtime": extract_imdb_movie_runtime(soup),
    }


def search_imdb_for_movie_credits_number(imdb_id: str) -> int | None:
    movie_credits_page: str = get_imdb_page_movie_credits(imdb_id)
    soup: bs4.BeautifulSoup = bs4.BeautifulSoup(movie_credits_page, "lxml")
    return extract_imdb_movie_credits_number(soup)
