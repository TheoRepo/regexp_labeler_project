#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import argparse
import toml
sys.path.append('..')

# 定义参数
parser = argparse.ArgumentParser(description="生成SQL表")
parser.add_argument('--config', default=None, dest='config', type=str, help='配置信息')
parser.add_argument('--output', default=None, dest='output', type=str, help='输出结果')
args = parser.parse_args()
print('数据抽取任务解析接受到如下参数 config:{0} output:{1}'.format(args.config, args.output))
# 解析配置信息
with open(args.config, 'r', encoding='utf-8') as f:
    config_dict = toml.load(f)

sql = """
CREATE TABLE IF NOT EXISTS `{}` (
    `mobile_id` String COMMENT '手机号映射id',
    `time_slot` String COMMENT '收信时间段',
    `date_slot` String COMMENT '收信是否为周末',
    `rule_hit` String COMMENT '命中规则分类类目情况，多个用逗号分隔'
)comment '文本规则分类命中情况表'
partitioned by (
    the_date string comment '分区1',
    file_no string comment '分区2'
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orcfile;
""".format(config_dict.get('target_table'))

with open(args.output, 'w', encoding='utf-8') as f:
    f.write(sql)
f.close()
