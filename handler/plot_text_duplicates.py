# -*- coding: utf-8 -*-
"""
.. _nlp__data_duplicates:

Text Data Duplicates
********************

This notebook provides an overview for using and understanding the text data duplicates check:

**Structure:**

* `Why check for text data duplicates? <#why-check-for-text-data-duplicates>`__
* `Create TextData <#create-textdata>`__
* `Run the Check <#run-the-check>`__
* `Define a Condition <#define-a-condition>`__

Why check for text data duplicates?
===================================

The ``TextDuplicates`` check finds multiple instances of identical or nearly identical (see
`text normalization <#with-text-normalization>`__) samples in the
Dataset. Duplicate samples increase the weight the model gives to those samples.
If these duplicates are there intentionally (e.g. as a result of intentional
oversampling, or due to the dataset's nature it has identical-looking samples)
this may be valid, however if this is a hidden issue we're not expecting to occur,
it may be an indicator for a problem in the data pipeline that requires attention.

Create TextData
===============

Let's create a simple dataset with some duplicate and similar text samples.
"""

from deepchecks.nlp.checks import TextDuplicates, SpecialCharacters
from deepchecks.nlp import TextData

# 打开文件
from handler.plot_text_property_outliers import load_file_path


def dc_handle(data=None):
    texts = data.split("\n")

    dataset = TextData(texts)

    # 检查重复的
    check = TextDuplicates()
    check.add_condition_ratio_less_or_equal(0)
    result = check.run(dataset)
    # result.show(show_additional_outputs=False)
    # result.save_as_html()
    file_path, file_url = load_file_path(flag="textDuplicates")
    result.save_as_html(file=file_path)

    # 检查多特殊字符的
    special_characters_check = SpecialCharacters()
    special_characters_check_result = special_characters_check.run(dataset=dataset)
    special_characters_file_path, special_characters_file_url = load_file_path(flag="specialCharacters")
    special_characters_check_result.save_as_html(file=special_characters_file_path)
    print("文件目录结果：", file_url, special_characters_file_url)
    return [file_url, special_characters_file_url]
