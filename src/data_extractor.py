import argparse
import json
import os
import re
import subprocess
from datetime import datetime as dt
from datetime import timedelta
from functools import partial
from string import Template

import pandas as pd
from pyrsistent import T
from regex import R
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import sys,toml
sys.path.append('..')
from lib.spark_base_connector import BaseSparkConnector

# 配置文件信息
os.environ['PYSPARK_PYTHON'] = "/usr/local/python3.7.4/bin/python3"
ABSPATH = os.path.dirname(os.path.abspath(__file__))

# 读取规则原始文件
rule_info_df = pd.read_csv(os.path.join(ABSPATH, "../config/rule_classifier_info.txt"), sep="\t")
rule_info_df = rule_info_df[rule_info_df["is_active"]==1]

# 读取规则映射文件
with open(r"../config/rule_map.toml", "r", encoding='utf-8') as f:
    rule_map = toml.load(f)

# 把读取结果输出一下，检查是否存在问题
print(rule_map)
print(rule_info_df.head(10))
print(rule_info_df)

# 规则拆解成字典
def rule_info_seperator(rule_info_df):
    rule_info_dict = {}
    level_1_rule_id_list = rule_info_df[(rule_info_df['rule_level']==1) & (rule_info_df['is_active']==1)]['rule_id'].values.tolist()
    level_2_rule_id_list = rule_info_df[(rule_info_df['rule_level']==2) & (rule_info_df['is_active']==1)]['rule_id'].values.tolist()
    rule_info_df_id = rule_info_df.set_index('rule_id')
    for rule_id in level_1_rule_id_list+level_2_rule_id_list:
        rule_info_dict[rule_id] = {
                "level_1_forward_rule_content":rule_info_df_id.loc[rule_id]['level_1_forward_rule_content'],
                "level_2_forward_rule_content":rule_info_df_id.loc[rule_id]['level_2_forward_rule_content'],
                "level_3_forward_rule_content":rule_info_df_id.loc[rule_id]['level_3_forward_rule_content'],
                "level_1_backward_rule_content":rule_info_df_id.loc[rule_id]['level_1_backward_rule_content'],
                "class_label_ch":rule_info_df_id.loc[rule_id]['class_label_ch'],
                "class_label_en":rule_info_df_id.loc[rule_id]['class_label_en'],
                "domain_label_ch":rule_info_df_id.loc[rule_id]['domain_label_ch'],
                "domain_label_en":rule_info_df_id.loc[rule_id]['domain_label_en'],
                "rule_level":rule_info_df_id.loc[rule_id]['rule_level'],
                "upper_id":rule_info_df_id.loc[rule_id]['upper_id']
        }
    return rule_info_dict, level_1_rule_id_list, level_2_rule_id_list

# 对文本进行标注工作
def rule_labeler(msg, rule_info_dict, level_1_rule_id_list, level_2_rule_id_list):
    label_list = []
    # 先进行level_1的预测
    for rule_id in level_1_rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)
        if re.search(rule_info_part.get("level_1_forward_rule_content"),msg) and re.search(rule_info_part.get("level_2_forward_rule_content"),msg) and re.search(rule_info_part.get("level_3_forward_rule_content"),msg):
            if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                label_list.append(rule_info_part.get("class_label_en"))
    # 在进行level_2的预测
    for rule_id in level_2_rule_id_list:
        rule_info_part = rule_info_dict.get(rule_id)
        upper_rule_id = rule_info_part.get("upper_id")
        upper_class_label_en = rule_info_dict.get(upper_rule_id).get("class_label_en")
        # print('当前id:{} 上级id:{}'.format(rule_id,upper_rule_id))
        if upper_class_label_en not in label_list:
            continue
        else:
            # print('命中{}'.format(upper_class_label_en))
            if re.search(rule_info_part.get("level_1_forward_rule_content"),msg) and re.search(rule_info_part.get("level_2_forward_rule_content"),msg) and re.search(rule_info_part.get("level_3_forward_rule_content"),msg):
                if rule_info_part.get("level_1_backward_rule_content") == '.' or re.search(rule_info_part.get("level_1_backward_rule_content"),msg) is None:
                    label_list.append(rule_info_part.get("class_label_en"))
    return ",".join(set(",".join(label_list).split(",")))

rule_info_dict, level_1_rule_id_list, level_2_rule_id_list = rule_info_seperator(rule_info_df)

rule_labeler_udf = udf(lambda x: rule_labeler(msg=x, rule_info_dict=rule_info_dict, level_1_rule_id_list=level_1_rule_id_list, level_2_rule_id_list=level_2_rule_id_list), returnType=StringType())

# 构建分时间段函数 输入字段为event_time
def time_slot(x):
    if x[11:] >= "18:00:01":
        return "t4"
    elif x[11:] >= "12:00:01":
        return "t3"
    elif x[11:] >= "06:00:01":
        return "t2"
    else:
        return "t1"

time_slot_udf = udf(lambda x: time_slot(x), returnType=StringType())

# 构建日期分段函数 输入字段为the_date
def date_slot(x):
    if dt.strptime(x,"%Y-%m-%d").weekday() >= 5:
        return "1"
    else:
        return "0"

date_slot_udf = udf(lambda x: date_slot(x), returnType=StringType())

class DataExtractor(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """初始化
        初始化spark

        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 设置副本数为2
        self.spark.sql("set dfs.replication=3")

    def run_test(self, source_table, target_table):
        # 读取全量数据
        data = self.read_all(source_table=source_table)
        # 进行提取
        data = data.select("mobile_id",
                           "event_time",
                           "msg",
                           time_slot_udf("event_time").alias("time_slot"),
                           date_slot_udf("the_date").alias("date_slot"),
                           rule_labeler_udf("msg").alias("rule_hit"),
                           "the_date",
                           "file_no")
        data.repartition(1000).filter("rule_hit != ''").write.format("hive").saveAsTable(target_table, mode="overwrite")

    def run(self, source_table, target_table, the_date, file_no):
        """
        执行目标数据获取任务

        Args:
            source_table: 必填参数，上游数据表;  str
            target_table: 必填参数，目标数据表;  str
            the_date: 必填参数，待处理分区;  str
            file_no: 必填参数，待处理分区;  str
        """
        # 读取分区数据
        data = self.read_partition(source_table=source_table, the_date=the_date, file_no=file_no)
        # 进行提取
        data = data.select("mobile_id",
                           time_slot_udf("event_time").alias("time_slot"),
                           date_slot_udf("the_date").alias("date_slot"),
                           rule_labeler_udf("msg").alias("rule_hit"),
                           "the_date",
                           "file_no")
        data = data.filter("rule_hit != ''")
        # 数据条数大小来设置一下分区数据
        data.cache()
        cnt = data.count()
        repartition = cnt//16000000
        # 写入数据
        data.createOrReplaceTempView("tmp_table")
        _sql = "insert overwrite table {} partition(the_date,file_no) select * from tmp_table distribute by pmod(hash(1000*rand(1)), {})".format(target_table, repartition)
        self.logger.info("将要执行如下sql进行数据插入:")
        self.logger.info(_sql)
        self.spark.sql(_sql)

if __name__=="__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="数据抽取模块")
    parser.add_argument("--app_name", default="rule_classify_extractor", dest="app_name", type=str, help="spark任务名称")
    parser.add_argument("--source_table", default=None, dest="source_table", type=str, help="上游数据表")
    parser.add_argument("--target_table", default=None, dest="target_table", type=str, help="目标数据表")
    parser.add_argument("--the_date", default=None, dest="the_date", type=str, help="需要处理的the_date分区")
    parser.add_argument("--file_no", default=None, dest="file_no", type=str, help="需要处理的file_no分区")
    parser.add_argument("--is_test", default=0, dest="is_test", type=int, help="是否为测试")
    args = parser.parse_args()
    # 初始化
    data_extractor = DataExtractor(app_name=args.app_name)
    # 是否为测试
    if args.is_test == 1:
        data_extractor.run_test(source_table=args.source_table, target_table=args.target_table)
    # 如果不是测试 那么就需要先检查参数是否已经给出
    else:
        data_extractor.run(source_table=args.source_table, target_table=args.target_table, the_date=args.the_date, file_no=args.file_no)
    # 结束
    data_extractor.stop()
