import time
import os
import threading

from config.server_config import LakefsConnectEntity

SHA256_HASH_FILE_NAME = "CHECKSUM.SHA256"
lakefsConnectEntity = LakefsConnectEntity()


def checksum_handle(hash=None, hook_context=None):
    # 创建线程并传递参数
    thread = threading.Thread(target=checksum_file_flow, kwargs={"sha256_hash": hash, "hook_context": hook_context})
    thread.start()
    return


def generate_file(content=None, file_path=None):
    """
    根据传入的内容生成文件
    :param content:
    :return:
    """
    # 检查目录是否存在，如果不存在则创建目录
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # 打开文件并写入内容
    with open(file_path, "w") as file:
        file.write(content)


def load_file_path():
    """
    获取文件生成路径
    :return:
    """
    # 获取当前工作目录
    current_dir = os.getcwd()
    # 文件路径
    return os.path.join(current_dir, "generate", str(threading.get_ident()), str(int(time.time())),
                        SHA256_HASH_FILE_NAME)


def load_checksum_file_of_lakefs_path(cacl_hash_file_path):
    """
    获取lakefs上传文件路径
    :param cacl_hash_file_path:
    :return:
    """
    result = ""
    if cacl_hash_file_path is None or cacl_hash_file_path == "":
        return result

    if "/" not in cacl_hash_file_path:
        return result

    cacl_hash_file_path_sq = cacl_hash_file_path.split("/")
    sq_len = len(cacl_hash_file_path_sq)
    for i in range(sq_len):
        if cacl_hash_file_path_sq[i] == "":
            continue
        if i == sq_len - 1:
            break
        result += (cacl_hash_file_path_sq[i] + "/")
    return result


def upload_file(file_path, hook_context):
    """
    上传文件到s3上
    :param file_path:
    :return:
    """
    # cacl_hash_file_path = hook_context.get("commit_metadata").get("path")
    cacl_hash_file_path = hook_context.commit_metadata.path
    checksum_file_of_lakefs_path = load_checksum_file_of_lakefs_path(cacl_hash_file_path)
    with open(file_path, 'rb') as f:
        lakefsConnectEntity.client.objects.upload_object(repository=hook_context.repository_id,
                                                         branch=hook_context.branch_id,
                                                         path=checksum_file_of_lakefs_path + SHA256_HASH_FILE_NAME,
                                                         content=f)


def delete_file(file_path):
    """
    删除生成的文件
    :param file_path:
    :return:
    """
    # 检查文件是否存在
    if os.path.exists(file_path):
        # 删除文件
        os.remove(file_path)
        print("文件已删除")
    else:
        print("文件不存在")


def checksum_file_flow(sha256_hash=None, hook_context=None):
    """
    完整性检查文件工作流
    :param msg:
    :return:
    """
    if sha256_hash is None or hook_context is None:
        return

    file_path = load_file_path()

    # 生成文件
    generate_file(content=sha256_hash, file_path=file_path)

    # 上传文件到数据湖系统
    upload_file(file_path=file_path, hook_context=hook_context)

    # 删除文件
    delete_file(file_path=file_path)


if __name__ == '__main__':
    lakefsConnectEntity = LakefsConnectEntity()

    hook_context = {"event_type": "post-commit", "event_time": "2023-06-12T07:18:15Z",
                    "action_name": "check wpxwebhook", "hook_id": "checksum",
                    "repository_id": "the-stack-dedup", "branch_id": "main",
                    "source_ref": "61ef1f90b9b10813502f9c04c1309ec01e774596531c5d1cb9607afa9520eead",
                    "commit_id": "61ef1f90b9b10813502f9c04c1309ec01e774596531c5d1cb9607afa9520eead",
                    "commit_message": "checksum", "committer": "admin",
                    "commit_metadata": {"path": "/abap/data-00000-of-00001.parquet"}}

    # WebHookBody(**hook_context)
    # print(hook_context.get("repository_id"))

    checksum_handle("123", hook_context=hook_context)
