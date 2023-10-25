import itertools
import json


class GenreMapper:
    def __init__(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as json_file:
            self._genre_mapping: dict[str, set[str]] = json.load(json_file)

    def map_many_genres(self, genres: list[str]) -> list[str]:
        result = []
        for genre in genres:
            result.append(self.map_single_genre(genre))

        result = list(set(itertools.chain(*result)))
        if len(result) != 1 and 'unknown' in result:
            result.remove('unknown')

        return result

    def map_single_genre(
            self,
            single_genre: str,
            before_mapping: dict[str, set[str]] | None = None,
            after_mapping: dict[str, list[str]] | None = None,
    ) -> list[str]:
        """
        Maps a single genre to its corresponding mapped genres based on provided mappings.

        Args:
            single_genre (str): The genre to be mapped.
            before_mapping (Optional[dict[str, set[str]]]): A dictionary containing mappings
                before the general part of mapping. The keys represent original genres, and
                the values are sets of mapped genres. (Default: None)
            after_mapping (Optional[dict[str, list[str]]]): A dictionary containing mappings
                after the general part of mapping. The keys represent general mapped genres,
                and the values are lists of more specific mapped genres. (Default: None)

        Returns:
            list[str]: A list of mapped genres corresponding to the input genre.

        Example:
            >>> mapper = GenreMapper("../../data/all_genre_map.json")
            >>> mapped_genres = mapper.map_single_genre('punk rock')
            >>> print(mapped_genres)
            ['punk', 'rock']
        """

        result_genres = {single_genre.lower()}
        if before_mapping:
            result_genres = before_mapping.get(single_genre, result_genres)

        # General part of mapping.
        result_genres = [self._genre_mapping.get(genre, ['unknown']) for genre in result_genres]
        result_genres = set(itertools.chain(*result_genres))

        if after_mapping:
            result_genres = [after_mapping.get(genre, ['unknown']) for genre in result_genres]
            result_genres = set(itertools.chain(*result_genres))

        if len(result_genres) != 1 and 'unknown' in result_genres:
            result_genres.remove('unknown')

        return list(result_genres)
