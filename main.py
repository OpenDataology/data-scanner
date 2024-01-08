import io
import os
import random
import time

import pandas as pd
import uvicorn
from fastapi import FastAPI
from typing import Union
import boto3
import hashlib
from pydantic import BaseModel

from handler.checksum_handler import checksum_handle
from handler.nlp.nlp_data_integrity import nlp_data_check

# lakefs_url = 'http://lakefs-service:8000'
from handler.plot_text_duplicates import dc_handle
from handler.tabular.tabula_data_itergrity import TabularWebHookBody, check_tabular_data
from handler.vision.vision_data_itergrity import VisionWebHookBody, check_vision_data

lakefs_url = 'http://www.opendataology.com:30910'
app = FastAPI()
s3 = boto3.client('s3',
                  endpoint_url=lakefs_url,
                  aws_access_key_id='AKIAJAJLUWEUICR7742Q',
                  aws_secret_access_key='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')


class CheckSumBody(BaseModel):
    path: str = None
    deepcheck_type: str = None
    deepcheck_path: str = None
    raw_text: str = None
    task_type: str = None
    metadata: str = None
    properties: str = None
    categorical_metadata: str = None
    tabular_path: str = None


class WebHookBody(BaseModel):
    action_name: str
    branch_id: str
    commit_message: str
    commit_metadata: Union[CheckSumBody, None] = None
    committer: str
    event_time: str
    event_type: str
    hook_id: str
    repository_id: str
    source_ref: str


def check_commit_metadata_for_checksum(web_hook_body):
    if web_hook_body is None \
            or web_hook_body.commit_metadata is None \
            or web_hook_body.commit_metadata.path is None:
        raise BaseException("webhook 参数不合法")
    pass


def check_commit_metadata_for_deepcheck(web_hook_body):
    if web_hook_body is None \
            or web_hook_body.commit_metadata is None \
            or web_hook_body.commit_metadata.deepcheck_type is None \
            or web_hook_body.commit_metadata.deepcheck_path is None:
        raise BaseException("webhook 参数不合法")
    pass


@app.post("/checksum")
def checksum(web_hook_body: WebHookBody):
    try:
        check_commit_metadata_for_checksum(web_hook_body)
        data = s3.get_object(Bucket=web_hook_body.repository_id,
                             Key=web_hook_body.branch_id + web_hook_body.commit_metadata.path)
        streaming = data['Body']
        sha1obj = hashlib.sha1()
        sha1obj.update(streaming._raw_stream.data)
        hash1 = sha1obj.hexdigest()
        # 额外处理
        checksum_handle(hash=hash1, hook_context=web_hook_body)
        return {"hash": hash1}
    except BaseException as e:
        print(e)
        return {"data": web_hook_body}


@app.post("/deepcheck/test")
def deepcheck(web_hook_body: WebHookBody):
    try:
        check_commit_metadata_for_deepcheck(web_hook_body)
        deepcheck_path = web_hook_body.commit_metadata.deepcheck_path
        deepcheck_type = web_hook_body.commit_metadata.deepcheck_type
        # data = s3.get_object(Bucket=web_hook_body.repository_id,
        #                      Key=web_hook_body.branch_id + web_hook_body.commit_metadata.path)
        data = s3.get_object(Bucket=web_hook_body.repository_id,
                             Key=web_hook_body.branch_id + deepcheck_path)
        streaming = data['Body']
        text = streaming.read().decode('utf-8')
        # if deepcheck_type == "0":
        #     file_url = ptpo.dc_handle(text)
        # else:
        file_url_list = dc_handle(text)
        return {
            "status": "success",
            "result_link": file_url_list
        }
    except BaseException as e:
        print(e)
        return {"status": "fail"}


def load_df(data):
    file_name = './' + str(int(time.time())) + str(random.randint(0, 100)) + '.csv'
    while os.path.exists(file_name):
        file_name = './' + str(int(time.time())) + str(random.randint(0, 100)) + '.csv'
    body = data['Body']
    with io.FileIO(file_name, 'w') as f:
        for i in body:
            f.write(i)
    df = pd.read_csv(file_name)
    os.remove(file_name)
    return df


@app.post("/deepcheck/nlp")
def deepcheck_nlp(web_hook_body: WebHookBody):
    try:
        # check_commit_metadata_for_checksum(web_hook_body)
        data = s3.get_object(Bucket=web_hook_body.repository_id,
                             Key=web_hook_body.branch_id + web_hook_body.commit_metadata.raw_text)
        df_rawtext = load_df(data)
        data = s3.get_object(Bucket=web_hook_body.repository_id,
                             Key=web_hook_body.branch_id + web_hook_body.commit_metadata.metadata)
        df_meta = load_df(data)
        data = s3.get_object(Bucket=web_hook_body.repository_id,
                             Key=web_hook_body.branch_id + web_hook_body.commit_metadata.properties)
        df_properties = load_df(data)
        cm = web_hook_body.commit_metadata.categorical_metadata.split(sep=",")
        file_url_list = nlp_data_check(raw_text=df_rawtext, task_type=web_hook_body.commit_metadata.task_type,
                                       metadata=df_meta, properties=df_properties,
                                       categorical_metadata=cm)
        return {
            "status": "success",
            "result_link": file_url_list
        }
    except BaseException as e:
        print(e)
        return {"status": "fail"}


@app.post("/deepcheck/tabular")
def deepcheck_nlp(body: TabularWebHookBody):
    try:
        data = s3.get_object(Bucket=body.repository_id,
                             Key=body.branch_id + body.commit_metadata.tabular_path)
        df = load_df(data)
        report_path = check_tabular_data(df, cat_features=body.commit_metadata.cat_features.split(sep=","),
                                         datetime_name=body.commit_metadata.datetime_name,
                                         label=body.commit_metadata.label)
        return report_path
    except BaseException as e:
        print(e)
        return {"status": "fail"}


@app.post("/deepcheck/vision")
def deepcheck_nlp(body: VisionWebHookBody):
    try:
        report_path = check_vision_data(
            'lakefs://{0}/{1}{2}'.format(body.repository_id, body.branch_id, body.commit_metadata.path))
        if 'error' in report_path:
            return {"status": report_path}
        return report_path
    except BaseException as e:
        print(e)
        return {"status": "fail"}


@app.get("/health")
def health():
    return {"data": "success"}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
