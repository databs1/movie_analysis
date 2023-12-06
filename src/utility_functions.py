import json
import pandas as pd
import unicodedata as ud
import os
import numpy as np
import re


def identify_unprocessed_rows(string_series: pd.Series):
    """_summary_

    Args:
        string_series (pd.Series): _description_

    Returns:
        _type_: _description_
    """
    stdout_unprocessed = {}
    # find which line fails the JSON encryption
    for line in range(string_series.shape[0]):
        try:
            json.loads(string_series[line])
        except Exception as e:
            stdout_unprocessed[line] = e
    return stdout_unprocessed


def is_latin(uchr):
    latin_letters = {}
    try:
        return latin_letters[uchr]
    except KeyError:
        return latin_letters.setdefault(uchr, "LATIN" in ud.name(uchr))


def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def dump_corpus_in_chunks(
    corpus: pd.Series, ids: pd.Series, file_path: str, chunks: int
):
    """_summary_

    Args:
        corpus (pd.Series): _description_
        ids (pd.Series): _description_
        file_path (str): _description_
        chunks (int): _description_

    Returns:
        _type_: _description_
    """
    base_name, file_extension = os.path.splitext(file_path)
    chunk_range = np.arange(0, corpus.shape[0], corpus.shape[0] // chunks)

    n_chunk = 0
    for chunk_start, chunk_end in zip(chunk_range[:-1], chunk_range[1:]):
        chunked_corpus = corpus.iloc[chunk_start:chunk_end].values
        chunked_index = ids.iloc[chunk_start:chunk_end].values

        # Add line numbers to each line in the chunk
        lines_with_numbers = [
            f"{idx}: {line}" for idx, line in zip(chunked_index, chunked_corpus)
        ]
        chunk_file_path = f"{base_name}_{n_chunk}{file_extension}"
        n_chunk += 1

        if not os.path.exists(chunk_file_path):
            with open(chunk_file_path, "w") as file:
                file.write("\n".join(lines_with_numbers) + "\n")
        else:
            return "Files already exist!"


def remove_apostrophes(string_series: pd.Series):
    """
    There is a normalization rule in which the JSON was stringified, allowing us to apply a pattern matching index on it:
    {'id': 15101, 'name': "based on children's book"}

    With this function yields:
    {'id': 15101, 'name': 'based on childrens book'}
    This double irregular double quote allows us to find easily the apostrophes and fix the malformed json so json.loads
    can safely load the data.

    Args:
        string_series (pd.Series): Stringified JSON 

    Returns:
        string_series (pd.Series)
    """

    string_series = (
        string_series.replace("\"'", '"')
        .replace("'\"", '"')
        .replace("\\xa0", " ")
        .replace("\\x9", " ")
    )  # normalize the string
    name_pattern = re.compile(r"\"(.*?)\"")
    matches = name_pattern.findall(string_series)

    if matches:
        for old_name_value in matches:
            updated_name_value = old_name_value.replace("'", "")
            string_series = string_series.replace(old_name_value, updated_name_value)
            normalized_string = string_series

        return normalized_string.replace("'", '"')
    else:
        return string_series.replace("'", '"')
