import pandas as pd
from deepchecks.tabular import datasets
from deepchecks.tabular import Dataset
from deepchecks.tabular.suites import data_integrity

from util.tools import load_file_path
from pydantic import BaseModel
from typing import Union


class TabularBody(BaseModel):
    tabular_path: str
    cat_features: str = None
    datetime_name: str = None
    label: str = None


class TabularWebHookBody(BaseModel):
    action_name: str
    branch_id: str
    commit_message: str
    commit_metadata: Union[TabularBody, None] = None
    committer: str
    event_time: str
    event_type: str
    hook_id: str
    repository_id: str
    source_ref: str


def check_tabular_data(data_df, cat_features=None, datetime_name=None, label=None):
    ds = Dataset(data_df, cat_features=cat_features, datetime_name=datetime_name, label=label)
    integ_suite = data_integrity()
    suite_result = integ_suite.run(ds)
    file_path, file_name = load_file_path('tabular_check_')
    suite_result.save_as_html(file_path)
    return file_name


if __name__ == '__main__':
    # load data
    # data = datasets.regression.avocado.load_data(data_format='DataFrame', as_train_test=False)
    data = pd.read_csv('avocado.csv')
    ds = Dataset(data, cat_features=['type'], datetime_name='Date', label='AveragePrice')
    # Run Suite:
    integ_suite = data_integrity()
    suite_result = integ_suite.run(ds)
    # Note: the result can be saved as html using suite_result.save_as_html()
    # or exported to json using suite_result.to_json()
    suite_result.save_as_html("tabular.html")
