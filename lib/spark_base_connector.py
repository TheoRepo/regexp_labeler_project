import logging
import os
import re
import subprocess
import time
from datetime import datetime as dt
from functools import partial
from pathlib import Path
from string import Template

import pandas as pd
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 获取目标数据
source_sql = Template('''
select
    row_key,
    mobile_id,
    event_time,
    app_name,
    suspected_app_name,
    msg,
    main_call_no,
    abnormal_label,
    hashcode,
    the_date,
    file_no
from ${source_table}
where 1=1
and ${the_date_condition}
and ${file_no_condition}
''')

class BaseSparkConnector(object):
    def __init__(self, app_name, log_level=None):
        """ 基础Spark连接类
            封装了部分常用功能进来组成基类，方便在后续继承此类构建自己的Spark任务代码类;

            Args:
                app_name: 必填参数，用于标记Spark任务名称;  str
                log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 配置文件信息 由于经常使用动态分区功能 在这里默认打开
        self.app_name = app_name
        conf = SparkConf().setMaster("yarn").setAppName(self.app_name)
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        conf.set("hive.exec.max.dynamic.partitions", 10000)
        # 初始化spark
        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        self.sqlContext = SQLContext(self.sc)
        # 设置日志等级
        if log_level:
            if log_level not in ["WARN","INFO","DEBUG","ERROR"]:
                raise ValueError("detect unexpected log_level: {0}".format(log_level))
            self.sc.setLogLevel(log_level)
        self.logger = self.init_logger()

    def init_logger(self):
        """ 初始化logger
        """
        logger = logging.getLogger(self.app_name)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def stop(self):
        """ 退出spark
        """
        self.sc.stop()
        self.spark.stop()

    def _run_cmd(self, command, print_log=True, raise_err=False):
        """ 执行命令行命令

            Args:
                command: 必填参数，需要执行的命令;  str
                pring_log: 选填参数，是否打印返回结果; bool
                raise_err: 选填参数，是否在cmd跑失败的时候抛出异常; bool

            Returns:
                执行命令结果; subprocess.CompletedProcess类
        """
        res = subprocess.run(command, shell=True)
        if print_log:
            self.logger.info(res)
        if raise_err and res.returncode != 0:
            raise ValueError("shell命令执行失败")
        return res

    def read_partition(self, source_table, the_date, file_no):
        """从指定hive表的指定分区读取数据

            Args:
                source_table: 必填参数，待读取分区的表; str
                the_date: 必填参数，待读取分区; str
                file_no: 选填参数，待读取分区; str
        """
        self.logger.info('将要执行如下sql进行分区数据获取:')
        if the_date == 'customer':
            self.logger.info('-- 客户测试模式，只通过file_no精确匹配')
            _sql = source_sql.substitute(source_table=source_table,the_date_condition=" 1=1 ".format(the_date), file_no_condition="file_no = '{}'".format(file_no))
        else:
            if file_no == "all":
                self.logger.info('-- 因为file_no为all,所以启用批量模式,the_date为正则匹配')
                _sql = source_sql.substitute(source_table=source_table, the_date_condition="the_date regexp '{}'".format(the_date), file_no_condition=" 1=1 ")
            else:
                self.logger.info('-- 因为file_no不为all,所以启用普通单批次模式')
                _sql = source_sql.substitute(source_table=source_table,the_date_condition="the_date = '{}'".format(the_date), file_no_condition="file_no regexp '{}'".format(file_no))
        self.logger.info(_sql)
        return self.spark.sql(_sql)

    def read_all(self, source_table):
        """从指定hive表的指定全量数据

            Args:
                source_table: 必填参数，待读取分区的表; str
        """
        self.logger.info('将要执行如下sql进行全量数据获取:')
        _sql = source_sql.substitute(source_table=source_table, the_date_condition="1=1 ", file_no_condition="1=1 ")
        self.logger.info(_sql)
        return self.spark.sql(_sql)

    def read_file_no(self, source_table, file_no):
        """从指定hive表的指定分区读取数据

            Args:
                source_table: 必填参数，待读取分区的表; str
                the_date: 必填参数，待读取分区; str
                file_no: 选填参数，待读取分区; str
        """
        self.logger.info('将要执行如下sql进行分区数据获取:')
        _sql = source_sql.substitute(source_table=source_table, the_date_condition=" the_date regexp '.' ", file_no_condition="file_no = '{}'".format(file_no))
        self.logger.info(_sql)
        return self.spark.sql(_sql)
