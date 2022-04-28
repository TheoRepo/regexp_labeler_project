from ast import arg
import toml
import json
import os
from string import Template
import argparse
from dateutil.relativedelta import relativedelta
import datetime
import sys
sys.path.append('..')
from lib.spark_base_connector import BaseSparkConnector

ABSPATH = os.path.dirname(os.path.abspath(__file__))
ABSPATH_F = os.path.dirname(ABSPATH)

sql_tmp = Template('''
insert into table ${write_table} partition(the_date,file_no)
select /*+ BROADCAST (b) */ mobile_id,time_slot,date_slot,rule_hit,the_date,'${file_no}' as file_no from ${read_table} a
left semi join (select mobile_id from ${id_back_date_table}  
where back_date > '${month}' and date_sub(back_date,220) < '${month}' and nvl(mobile_id, '') != ''
    and pt regexp '${file_no}') b
on a.mobile_id = b.mobile_id
distribute by pmod(hash(1000*rand(1)), 40) 
''')

start_end_sql = Template('''
select min(date_sub(back_date, 220)) as start_date,
    max(back_date) as end_date
from ${id_back_date_table}
where nvl(mobile_id, '') != ''
    and pt regexp '${pt}'
''')



class DataCleaner(BaseSparkConnector):
    def __init__(self, app_name, log_level=None):
        """
        数据清洗模块
        Args:
            app_name: 必填参数，用于标记Spark任务名称;  str
            log_level: 选填参数，用于标记Spark任务的日志等级，只可以为WARN、INFO、DEBUG、ERROR其中一种;  str
        """
        # 初始化spark
        super().__init__(app_name=app_name, log_level=log_level)
        # 加载完毕
        self.logger.info('数据清洗模块初始化完成')

    def run(self, master_table, customer_table, file_no, id_back_date_table):
        start_end_df = self.spark.sql(start_end_sql.substitute(
            pt=file_no, id_back_date_table=id_back_date_table)).toJSON().collect()
        _start_end = json.loads(start_end_df[0])
        _start = _start_end['start_date']
        _end = _start_end['end_date']
        start = datetime.datetime.strptime(_start, '%Y-%m-%d')
        end = datetime.datetime.strptime(_end, '%Y-%m-%d')
        _the_date = start
        the_dates = []
        while _the_date <= end:
            month = _the_date.strftime("%Y-%m")
            the_dates.append(month)
            _the_date = _the_date + relativedelta(months=+1)
        print(the_dates)
        self.spark.sql("ALTER TABLE {0} DROP IF EXISTS PARTITION (file_no='{1}')".format(
            customer_table, file_no))
        for m in the_dates:
            read_table = "(select * from {0} where the_date regexp '{1}')".format(
                master_table, m)
            _sql = sql_tmp.substitute(write_table=customer_table, file_no=file_no,
                                      read_table=read_table, month=m, id_back_date_table=id_back_date_table)
            print(_sql)
            self.spark.sql(_sql)


if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser(description="建表sql生成")
    parser.add_argument('--file_no', default='master',
                        dest='file_no', type=str, help='file_no')
    parser.add_argument('--index', default=-1,
                        dest='index', type=int, help='第几张表')
    parser.add_argument('--id_table', default='customer_test.customer_sample_id',
                        type=str, help='id 回溯表', dest='id_table')
    args = parser.parse_args()
    file_no = args.file_no

    customer = toml.load(open(os.path.join(
        ABSPATH_F, 'config', 'config_customer.toml'), 'r'))
    master = toml.load(
        open(os.path.join(ABSPATH_F, 'config', 'config.toml'), 'r'))
    master_table = master['target_table']
    customer_table = customer['target_table']

    dr = DataCleaner(app_name="" + '_MasterToCustomer')
    dr.run(master_table, customer_table, file_no, args.id_table)
