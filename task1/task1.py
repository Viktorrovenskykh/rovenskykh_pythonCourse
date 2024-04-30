import requests
from datetime import datetime, timedelta
import csv

class MovieDataTool:
    def __init__(self, num_pages):
        self.num_pages = num_pages
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIzMTI3NGFmYTRlNTUyMjRjYzRlN2Q0NmNlMTNkOTZjOSIsInN1YiI6IjVkNmZhMWZmNzdjMDFmMDAxMDU5NzQ4OSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.lbpgyXlOXwrbY0mUmP-zQpNAMCw_h-oaudAJB6Cn5c8"
        }
        self.genres = self._fetch_genres()
        self.movies_data = self._fetch_movies_data()

    def _fetch_genres(self):
        url = f"{self.base_url}/genre/movie/list?language=en"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()["genres"]
        else:
            print("Error fetching genre data:", response.status_code)
            return []

    def _fetch_movies_data(self):
        movies_data = []
        for page in range(1, self.num_pages + 1):
            url = f"{self.base_url}/discover/movie?include_adult=false&include_video=false&sort_by=popularity.desc&page={page}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                movies_data.extend(response.json()["results"])
            else:
                print(f"Error fetching movie data on page {page}:", response.status_code)
        return movies_data

    def fetch_data_from_desired_pages(self):
        return self.movies_data

    def get_all_data(self):
        return self.movies_data

    def get_movies_by_indexes(self, start, end, step):
        return self.movies_data[start:end:step]

    def get_most_popular_title(self):
        return max(self.movies_data, key=lambda movie: movie["popularity"])["title"]

    def get_movies_by_description_keywords(self, keywords):
        return [movie["title"] for movie in self.movies_data if any(keyword.lower() in movie.get("overview", "").lower() for keyword in keywords)]

    def get_unique_genres(self):
        return {genre["name"] for genre in self.genres}

    def delete_movies_by_genre(self, genre):
        self.movies_data = [movie for movie in self.movies_data if genre not in [g["name"] for g in movie.get("genres", [])]]

    def get_most_popular_genres(self):
        genre_counts = {}
        for movie in self.movies_data:
            for genre in movie.get("genres", []):
                genre_counts[genre["name"]] = genre_counts.get(genre["name"], 0) + 1
        return genre_counts

    def group_movies_by_common_genres(self):
        groups = {}
        for movie in self.movies_data:
            for genre in movie.get("genres", []):
                if genre["name"] not in groups:
                    groups[genre["name"]] = []
                groups[genre["name"]].append(movie["title"])
        return groups

    def return_initial_data_with_replaced_genre_id(self):
        copy_data = self.movies_data.copy()
        for movie in copy_data:
            if movie.get("genres"):
                movie["genres"][0]["id"] = 22
        return self.movies_data, copy_data

    def get_structures_with_fields(self):
        structures = []
        for movie in self.movies_data:
            structure = {
                "Title": movie["title"],
                "Popularity": round(movie["popularity"], 1),
                "Score": int(movie["vote_average"]),
                "Last_day_in_cinema": datetime.strptime(movie["release_date"], "%Y-%m-%d") + timedelta(weeks=10, days=3)
            }
            structures.append(structure)
        return sorted(structures, key=lambda x: (x["Score"], x["Popularity"]), reverse=True)

    def write_to_csv(self, data, file_path):
        keys = data[0].keys()
        with open(file_path, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)


tool = MovieDataTool(num_pages=5)
data = tool.get_structures_with_fields()
tool.write_to_csv(data, "movie_data.csv")
