from string import Template
from datetime import datetime as dt
from datetime import timedelta
import sys
import subprocess
import os
import re
import argparse
import pandas as pd
import toml

ABSPATH = os.path.dirname(os.path.abspath(__file__))

if __name__=="__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="对外统一接口")
    parser.add_argument('--config', default=None, dest='config', type=str, help='配置参数信息')
    parser.add_argument('--the_date', default=None, dest='the_date', type=str, help='需要处理的the_date分区')
    parser.add_argument('--file_no', default=None, dest='file_no', type=str, help='需要处理的file_no分区')
    args = parser.parse_args()
    print('数据抽取任务解析接受到如下参数 config:{0} the_date:{1} file_no:{2}'.format(args.config, args.the_date, args.file_no))
    # 解析配置信息
    with open(args.config, 'r') as f:
        config_dict = toml.load(f)
    the_date = args.the_date
    file_no = args.file_no
    # 调用接口
    result = subprocess.call(["sh", os.path.join(ABSPATH,'./data_extractor.sh'), '--is_test', '1', '--the_date', the_date, '--file_no', file_no, \
                                                                                 '--source_table', config_dict.get('source_table'), \
                                                                                 '--target_table', config_dict.get('target_table')])
    # 返回结果
    if result != 0:
        raise ValueError("任务执行失败")
