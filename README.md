<center><b><font size=20>规则分类结构化项目</font></b></center>

## 项目说明
+ 项目描述
    + 本项目实现了原始数据进行规则分类的工作
+ 业务方
    + 智慧金融一部
+ 数据更新周期
    + 随上游结果实时更新
+ 实时性要求
    + 上游数据更新后2个小时内完成

## 数据流拓扑图及数据⾎缘
[config:source_table(数据源底层表)] -->> [config:target_table(规则分类结构化目标表)]

## 库表说明
+ 输入库表说明：预处理加工结果表
+ 输出库表说明：规则分类结构化目标表（字段说明详见建表语句）

## 部署说明
+ 项目结构
```angular2
nlp_structure_rule_classify
│
├─config # 配置信息备份模块,包含核心路径配置、分类映射内容配置等
│      config.toml # 生产环境配置项
│      config_dev.toml # 开发环境配置项
│
├─lib # 结构化依赖的模块,包含一些辅助函数与包
│      spark_base_connector.py
│      __init__.py
│
├─src # 结构化执行的模块,主要逻辑为输出每条记录命中的规则情况
│        data_extractor.py
│        data_extractor.sh
│        export_sql_table.py
│        extractor.py
│        __init__.py
│create_table.sh # 生成Hive建表语句
│run.sh # 执行规则分类结构化主程序
│README.md # 说明文档
```
+ 依赖版本
```angular2
python 3.7.4
    pyspark 3.2.0
    toml 0.10.2
```
+ 部署流程
    + 基于生产环境情况修改config.toml文件
    + 基于config.toml文件执行如下命令生成建表语句并完成表创建
    ```
    sh create_table.sh --config ./config/config_dev.toml --output create_table.sql
    ```
    + 基于生产账号和资源设置调整./src/data_extractor.sh
    + 执行如下命令完成跑数工作
    ```
    # file_no为all,启用批量模式,the_date为正则匹配
    # file_no不为all如merge_20200304_23807_L0,启用单批次模式,the_date为具体日期
    sh run.sh --config ./config/config.toml --the_date 2020-03-04 --file_no merge_20200304_23807_L0
    sh run.sh --config ./config/config_dev.toml --the_date 2021-11-11 --file_no merge_20211111_0123_L0
    ```
+ 注意事项
    + 如果shell脚本在win下压缩后存在doc与unix不兼容问题,在Linux下将run.sh与data_extractor.sh格式转换,进入命令行操作模式:set ff=unix


## 测试说明
+ **第一步核验规则的正确性**
    + 因为SQL的正则规则和python的正则规则存在出入，所以需要对规则的正确性进行核验；
    + 因为测试流程的目标表，使用pyspark的SaveAsTable命令进行创建，无需写额外的建表语句

    1. 首先从采样表截取一天的数据写入新表(因为测试流程读的是全量表，数据量太大，运行时间太长了，目前需要验证正则的正确性，最好用小批量的数据，几秒钟完成计算)
    ```sql
    create table if not exists nlp_dev.qianyu_20220427_ds_txt_final_sample like preprocess.ds_txt_final_sample;
    insert overwrite table nlp_dev.qianyu_20220427_ds_txt_final_sample partition(the_date, file_no)
    select * from preprocess.ds_txt_final_sample where the_date = '2021-11-11' and file_no= 'merge_20211111_0123_L0';
    ```

    2. 修改配置文件`config_dev.toml`
    ```toml
    source_table = "nlp_dev.qianyu_20220427_ds_txt_final_sample" # 数据源抽样表
    target_table = "nlp_dev.rule_txt_classify_epidemicGroup_test" # 规则分类结构化目标表
    ```
    [TOML格式说明](https://zh.wikipedia.org/wiki/TOML)

    3. 修改代码`extractor.py`
    ```python
    # 调用接口
    result = subprocess.call(["sh", os.path.join(ABSPATH,'./data_extractor.sh'), '--is_test', '1', '--the_date', the_date, '--file_no', file_no, \
                                                                                 '--source_table', config_dict.get('source_table'), \
                                                                                 '--target_table', config_dict.get('target_table')])    
    ```

    4. 跑数（测试过程中，目标表读取的是全量，--the_date --file_no 这两配置参数必须有，但是内容可以随意，因为代码运行过程中不使用）
    ```bash
    sh run.sh --config ./config/config_dev.toml --the_date 2021-11-11 --file_no merge_20211111_0123_L0
    ```

    5. 核验结果数据
    ```sql
    select msg,rule_hit from nlp_dev.rule_txt_classify_epidemicGroup_test limit 1000;
    ```

    6. 给旧表改个名字
    ```sql
    alter table nlp_dev.rule_txt_classify_epidemicGroup_test rename to nlp_dev.rule_txt_classify_test;
    ```

    7. 规则修改
    因为只改动了`rule_classifier_info.txt`，如果代码报错，问题一定出在`rule_classifier_info.txt`
    解决的办法就是，逐条测试规则

    使用一定正确的规则，测试流程的正确性
    ```text
    id	rule_id	level_1_forward_rule_content	level_2_forward_rule_content	level_3_forward_rule_content	level_1_backward_rule_content	class_label_ch	class_label_en	domain_label_ch	domain_label_en	rule_level	upper_id	is_active	ext_info	create_time	update_time
    2	R0101	隔离	.	.	.	隔离	isolation	涉疫	invovle_epidemic	1	-1	1		27/4/2022 10:20:00	27/4/2022 10:20:00
    ```

    确保流程正确的前提下，反复修改出正确的规则
    ```text
    id	rule_id	level_1_forward_rule_content	level_2_forward_rule_content	level_3_forward_rule_content	level_1_backward_rule_content	class_label_ch	class_label_en	domain_label_ch	domain_label_en	rule_level	upper_id	is_active	ext_info	create_time	update_time
    2	R0101	[您|你].*隔离.*[您|你]	【[\u4e00-\u9fa5_a-zA-Z0-9]*?居委会[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?社区[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?居民委员会[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?街道办事处[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?管委会[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?防控组[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?公安局[\u4e00-\u9fa5_a-zA-Z0-9]*?】|【[\u4e00-\u9fa5_a-zA-Z0-9]*?村委会[\u4e00-\u9fa5_a-zA-Z0-9]*?】	隔离[时期]间|隔离.{0,5}[天日]|期满|转码|医.{0,3}上门|上报体温|[天|日]算起|防控重点人员|报备|主动.{0,8}报告|马上.{0,8}报到|生活必需品|加.{0,3}微信|加.{0,3}好友|离开住所|不.{0,3}外出|不.{0,3}出门	办公.{0,2}系统|政务.{0,2}平台|如|家属|朋友|家人|反映.{0,8}问题|温馨提醒|解除隔离|工业园区	被社区要求隔离或者正在隔离的人	isolation	涉疫	invovle_epidemic	1	-1	1		27/4/2022 10:20:30	27/4/2022 10:20:30
    3	R0102	[您|你].*属于	黄码|红码|时空伴随者	.	交警|机动车|骗|老师|家长|孕妇|误判|学校|工单|司机|滴滴出行|如有不配合者|反应.{0,12}问题|诈|骗|公文|文件	健康码变色但未明确需要隔离	discolored_heath_code	涉疫	invovle_epidemic	1	-1	1		27/4/2022 10:20:00	27/4/2022 10:20:00
    4	R0103	新冠病毒感染|新冠.{0,2}阳性|核酸.{0,2}阳性|密切接触者|密接	申报|报备|隔离	[您|你].*疾控.*[您|你]	提[示|醒]|老师|家长|班主任|工单|公文|OA系统|亲友	新冠的密接并且被要求报备或者隔离的人	close_contacts	涉疫	invovle_epidemic	1	-1	1		27/4/2022 10:20:10	27/4/2022 10:20:10
    ```

+ **第二步，跑全量的sample表，将结果进行数据核验**
    1. 删除测试表
    ```sql
    drop table if exists nlp_dev.rule_txt_classify_epidemicGroup_test;
    ```

    2. 修改配置文件
    config_dev.toml
    ```toml
    source_table = "preprocess.ds_txt_final_sample" # 数据源抽样表
    target_table = "nlp_dev.rule_txt_classify_epidemicGroup_test" # 规则分类结构化目标表
    ```

    3. 用正确的规则跑全量的采样（sample）表
    ```bash
    sh run.sh --config ./config/config_dev.toml --the_date 2021-11-11 --file_no merge_20211111_0123_L0
    ```