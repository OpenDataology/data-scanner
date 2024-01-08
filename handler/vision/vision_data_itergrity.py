import os
from deepchecks.vision import classification_dataset_from_directory
from deepchecks.vision.suites import train_test_validation

from util.tools import load_file_path
from pydantic import BaseModel
from typing import Union


class VisionBody(BaseModel):
    path: str


class VisionWebHookBody(BaseModel):
    action_name: str
    branch_id: str
    commit_message: str
    commit_metadata: Union[VisionBody, None] = None
    committer: str
    event_time: str
    event_type: str
    hook_id: str
    repository_id: str
    source_ref: str


def check_vision_data(lakefs_url):
    cmd = './shell_tool/lakectl fs download -r {0} ./tmp_data'.format(lakefs_url)
    if os.system(cmd) != 0:
        return "download file error"
    train_ds, test_ds = classification_dataset_from_directory(
        root='./tmp_data/', object_type='VisionData', image_extension='jpg')
    suite = train_test_validation()
    result = suite.run(train_ds, test_ds)
    file_path, file_name = load_file_path('image_check_')
    result.save_as_html(file_path)
    os.system('rm -rf ./tmp_data')
    return file_name


if __name__ == '__main__':
    # url = 'https://figshare.com/ndownloader/files/34912884'
    # urllib.request.urlretrieve(url, 'EuroSAT_data.zip')
    #
    # with zipfile.ZipFile('EuroSAT_data.zip', 'r') as zip_ref:
    #     zip_ref.extractall('EuroSAT')
    # train_ds, test_ds = classification_dataset_from_directory(
    #     root='./EuroSAT/euroSAT/', object_type='VisionData', image_extension='jpg')
    # suite = train_test_validation()
    # result = suite.run(train_ds, test_ds)
    # result.save_as_html('output.html')
    # check_vision_data('lakefs://euro-sat/main/data/')
    os.system('rm -rf ./tmp_data')
