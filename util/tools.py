import time


def load_file_path(flag=""):
    """
    获取文件生成路径
    :return:
    """

    # 获取当前工作目录
    file_name = flag + '_' + str(int(time.time())) + '.html'

    current_dir = '/data/' + file_name
    # current_dir = './' + file_name
    # 文件路径
    RESULT_FILE_DOMAIN = "http://119.8.116.2:9258/"
    return current_dir, RESULT_FILE_DOMAIN + file_name
