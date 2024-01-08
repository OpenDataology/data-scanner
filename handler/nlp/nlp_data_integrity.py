import json

from deepchecks.nlp.checks import TextPropertyOutliers, PropertyLabelCorrelation, SpecialCharacters, \
    UnderAnnotatedMetaDataSegments, UnderAnnotatedPropertySegments, UnknownTokens, TextDuplicates, ConflictingLabels
from deepchecks.nlp import TextData
import pandas as pd
from deepchecks.nlp.datasets.classification import tweet_emotion
from util.tools import load_file_path


def load_nlp_text_data(raw_text, label=None, task_type=None, metadata=None, embeddings=None, properties=None,
                       categorical_metadata=None, calculate_builtin_properties=False):
    text_data = TextData(raw_text=raw_text, label=label, task_type=task_type, metadata=metadata, embeddings=embeddings,
                         properties=properties, categorical_metadata=categorical_metadata)
    if calculate_builtin_properties:
        text_data.calculate_builtin_properties()
    return text_data


def text_property_outliers(text_data):
    check = TextPropertyOutliers()
    result = check.run(text_data)
    file_path, file_name = load_file_path('text_property_outliers')
    result.save_as_html(file=file_path)
    return file_name


def property_label_correlation(text_data):
    result = PropertyLabelCorrelation().run(text_data)
    file_path, file_name = load_file_path('property_label_correlation')
    result.save_as_html(file=file_path)
    return file_name


def special_characters(text_data):
    check = SpecialCharacters()
    result = check.run(text_data)
    file_path, file_name = load_file_path('special_characters')
    result.save_as_html(file=file_path)
    return file_name


def under_annotated_metadata_segments(text_data):
    # need dataset set_metadata
    check = UnderAnnotatedMetaDataSegments(segment_minimum_size_ratio=0.07)
    result = check.run(text_data)
    file_path, file_name = load_file_path('under_annotated_metadata_segments')
    result.save_as_html(file=file_path)
    return file_name


def under_annotated_property_segments(text_data):
    # Check was unable to find under annotated segments. This is expected if your data is well annotated. If this is
    # not the case, try increasing n_samples or supply more properties
    check = UnderAnnotatedPropertySegments(segment_minimum_size_ratio=0.04)
    result = check.run(text_data)
    file_path, file_name = load_file_path('under_annotated_property_segments')
    result.save_as_html(file=file_path)
    return file_name


def unknown_token(text_data):
    check = UnknownTokens()
    result = check.run(text_data)
    file_path, file_name = load_file_path('unknown_token')
    result.save_as_html(file=file_path)
    return file_name


def text_data_duplicates(dataset):
    result = TextDuplicates(
        ignore_case=False,
        remove_punctuation=False,
        normalize_unicode=False,
        remove_stopwords=False,
        ignore_whitespace=False
    ).run(dataset)
    file_path, file_name = load_file_path('text_data_duplicates')
    result.save_as_html(file=file_path)
    return file_name


def conflicting_labels(dataset):
    result = ConflictingLabels(
        ignore_case=False,
        remove_punctuation=False,
        normalize_unicode=False,
        remove_stopwords=False,
        ignore_whitespace=False
    ).run(dataset)
    file_path, file_name = load_file_path('conflicting_labels')
    result.save_as_html(file=file_path)
    return file_name


def nlp_data_check(raw_text=None, task_type=None, metadata=None, embeddings=None, properties=None,
                   categorical_metadata=None, calculate_builtin_properties=False):
    if raw_text is None:
        dataset = tweet_emotion.load_data(as_train_test=False, include_embeddings=True)
    else:
        dataset = load_nlp_text_data(raw_text=raw_text['text'], label=raw_text['label'],
                                     task_type=task_type,
                                     metadata=metadata, embeddings=embeddings, properties=properties,
                                     categorical_metadata=categorical_metadata)
    html_urls = [text_data_duplicates(dataset), property_label_correlation(dataset), special_characters(dataset),
                 unknown_token(dataset), text_data_duplicates(dataset), conflicting_labels(dataset)]
    return html_urls


if __name__ == '__main__':
    # nlp_data_check()
    dataset = pd.read_csv('../../tweet_emotion.csv')
    properties = pd.read_csv('../../tweet_emotion_properties.csv')
    metadata = pd.read_csv('../../tweet_emotion_metadata.csv')
    CAT_METADATA = ['gender', 'user_region']
    tweet_text_data = load_nlp_text_data(raw_text=dataset['text'], label=dataset['label'],
                                         task_type='text_classification',
                                         metadata=metadata, embeddings=None, properties=properties,
                                         categorical_metadata=CAT_METADATA)
    # a = text_property_outliers(tweet_text_data)
    # b = property_label_correlation(tweet_text_data)

    # print(special_characters(tweet_text_data))
    # # print(under_annotated_metadata_segments(tweet_text_data))
    # # print(under_annotated_property_segments(tweet_text_data))
    # print(unknown_token(tweet_text_data))
    # print(text_data_duplicates(tweet_text_data))
    # print(conflicting_labels(tweet_text_data))
