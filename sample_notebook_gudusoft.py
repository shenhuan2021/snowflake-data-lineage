# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

# COMMAND ----------

start_date=dbutils.widgets.get("start_date")
end_date=dbutils.widgets.get("end_date")

print("start date is : "+start_date)
print("end date is : "+end_date)

# COMMAND ----------

import sys 
from datetime import datetime, timedelta 
from pyspark.sql import SparkSession 
from pyspark.sql import Row 
from pyspark.sql.functions import from_json,expr,from_unixtime,from_utc_timestamp,to_date 
from pyspark.sql.types import StringType,StructField,StructType,LongType,MapType,IntegerType,DateType,TimestampType,DatetimeConverter,DateConverter 
spark = SparkSession.builder.appName("rev_mvmt_base_tables")\
  .config('spark.sql.parquet.writeLegacyFormat', 'true')\
  .config("mapreduce.fileoutputcommitter.algorithm.version", "2")\
  .config('spark.sql.session.timeZone', 'IST')\
  .config('spark.sql.legacy.timeParserPolicy','LEGACY')\
  .config("spark.sql.sources.partitionOverwriteMode","dynamic")\
  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")\
  .config('spark.sql.parquet.compression.codec', 'gzip')\
  .config('spark.sql.vectorized.execution.enabled','false')\
  .enableHiveSupport()\
  .getOrCreate()

# COMMAND ----------

spark.sql("""msck repair table nikesh.rev_mvmt_base_pt_ipt_st_report;""")

# COMMAND ----------

spark.sql("""msck repair table nikesh.rev_mvmt_base_bonus_report;""")

# COMMAND ----------

spark.sql("""msck repair table nikesh.rev_mvmt_base_ft_ipt_st_report;""")

# COMMAND ----------

param1=start_date
param2=end_date
delta = timedelta(days=1)
v_start_date = datetime.strptime(param1, '%Y-%m-%d').date()
v_end_date = datetime.strptime(param2, '%Y-%m-%d').date()
basePath='s3path/player_transaction/'
paths=[]

while (( v_start_date <= v_end_date )):
        tableDate = v_start_date.strftime("%Y%m%d")
        monthYear = v_start_date.strftime("%b") + "-" + v_start_date.strftime("%y")
        paths.append(f"s3path/partition_column_date={v_start_date}")
        v_start_date=v_start_date + delta


player_transaction=spark.read.option("basePath",basePath).parquet(*paths)
player_transaction.createOrReplaceTempView("player_transaction")

# COMMAND ----------

param1=start_date
param2=end_date
delta = timedelta(days=1)
v_start_date = datetime.strptime(param1, '%Y-%m-%d').date()
v_end_date = datetime.strptime(param2, '%Y-%m-%d').date()
basePath='s3path/in_play_transaction/'
paths=[]

while (( v_start_date <= v_end_date )):
        tableDate = v_start_date.strftime("%Y%m%d")
        monthYear = v_start_date.strftime("%b") + "-" + v_start_date.strftime("%y")
        paths.append(f"s3path/partition_column_date={v_start_date}")
        v_start_date=v_start_date + delta


in_play_transaction=spark.read.option("basePath",basePath).parquet(*paths)
in_play_transaction.createOrReplaceTempView("in_play_transaction")

# COMMAND ----------

param1=start_date
param2=end_date
delta = timedelta(days=1)
v_start_date = datetime.strptime(param1, '%Y-%m-%d').date()
v_end_date = datetime.strptime(param2, '%Y-%m-%d').date()
basePath='s3path/system_transaction/'
paths=[]

while (( v_start_date <= v_end_date )):
        tableDate = v_start_date.strftime("%Y%m%d")
        monthYear = v_start_date.strftime("%b") + "-" + v_start_date.strftime("%y")
        paths.append(f"s3path/partition_column_date={v_start_date}")
        v_start_date=v_start_date + delta


system_transaction=spark.read.option("basePath",basePath).parquet(*paths)
system_transaction.createOrReplaceTempView("system_transaction")

# COMMAND ----------


txn_type=spark.read.parquet("s3path/txn_type")
txn_type.createOrReplaceTempView("txn_type")

# COMMAND ----------

param1=start_date
param2=end_date
delta = timedelta(days=1)
v_start_date = datetime.strptime(param1, '%Y-%m-%d').date()
v_end_date = datetime.strptime(param2, '%Y-%m-%d').date()
basePath='s3basepath/lb_in_play_transaction/'
paths=[]

while (( v_start_date <= v_end_date )):
        tableDate = v_start_date.strftime("%Y%m%d")
        monthYear = v_start_date.strftime("%b") + "-" + v_start_date.strftime("%y")
        paths.append(f"s3path/partition_column_date={v_start_date}")
        v_start_date=v_start_date + delta


lb_in_play_transaction=spark.read.option("basePath",basePath).parquet(*paths)
lb_in_play_transaction.createOrReplaceTempView("lb_in_play_transaction")

# COMMAND ----------

spark.sql("""drop table if exists nikesh.mtt_points_in_play_transaction ; """)
spark.sql("""CREATE TABLE nikesh.mtt_points_in_play_transaction(
  id bigint, 
  txn_id bigint, 
  user_id bigint, 
  amount decimal(16,3), 
  update_time timestamp, 
  trnmnt_id bigint, 
  updated_bal decimal(16,3), 
  credit_debit string, 
  txn_type_id smallint, 
  meta_data varchar(255))
using parquet
PARTITIONED BY ( 
  update_date date)
LOCATION
  's3path/mtt_points_in_play_transaction';""")

# COMMAND ----------

spark.sql("""msck repair table nikesh.mtt_points_in_play_transaction;""")

# COMMAND ----------


user_preference=spark.read.parquet("s3path/user_preference)
user_preference.createOrReplaceTempView("user_preference")
  

# COMMAND ----------


bonus_promotions=spark.read.parquet("s3path/bonus_promotions/")
bonus_promotions.createOrReplaceTempView("bonus_promotions")
  
  

# COMMAND ----------


spark.sql("""drop table if exists nikesh.rev_mvmt_base_monthly_pt_ipt_st;""")
spark.sql("""create table nikesh.rev_mvmt_base_monthly_pt_ipt_st as 
SELECT 
r_date,
user_id,
gt_txn_id,
credit_debit,
txn_head,
table_name,
sum(amt) as amount
from

(
select pt.update_date as r_date,
pt.user_id as user_id,
pt.txn_type_id as gt_txn_id,
cast(credit_debit as int) as credit_debit,
txn_head as txn_head,
'pt' as table_name,
sum(pt.amount) as amt
from
player_transaction pt,
txn_type tt
where  pt.txn_type_id = tt.id
and pt.update_date between '{0}' and '{1}'

group by 
pt.update_date,pt.user_id,pt.txn_type_id,credit_debit,txn_head,'pt' 

union all

select 
ipt.update_date as r_date,
ipt.user_id as user_id,
ipt.txn_type_id as gt_txn_id,
cast(credit_debit as int) as credit_debit,
txn_head as txn_head,
'ipt' as table_name,
sum(ipt.amount) as amt
from
in_play_transaction ipt,
txn_type tt
where  ipt.txn_type_id = tt.id
and ipt.update_date between '{0}' and '{1}'
group by 
ipt.update_date,ipt.user_id,ipt.txn_type_id,credit_debit,txn_head,'ipt' 

union all

select 
st.update_date as r_date,
st.user_id as user_id,
st.txn_type_id as gt_txn_id,
cast(credit_debit as int) as credit_debit,
txn_head as txn_head,
'st' as table_name,
sum(st.amount) as amt
from
system_transaction st,
txn_type tt
where  st.txn_type_id = tt.id
and st.update_date between '{0}' and '{1}'
group by 
st.update_date,st.user_id,st.txn_type_id,credit_debit,txn_head,'st'

union all

select 
lb_ipt.update_date as r_date,
lb_ipt.user_id as user_id,
lb_ipt.txn_type_id as gt_txn_id,
cast(credit_debit as int) as credit_debit,
txn_head as txn_head,
'lb_ipt' as table_name,
sum(lb_ipt.amount) as amt
from
lb_in_play_transaction lb_ipt,
txn_type tt
where lb_ipt.txn_type_id = tt.id
and lb_ipt.update_date between '{0}' and '{1}'

group by 
lb_ipt.update_date,lb_ipt.user_id,lb_ipt.txn_type_id,credit_debit,txn_head,'lb_ipt' 

union all

select 
mtt_ipt.update_date as r_date,
mtt_ipt.user_id as user_id,
mtt_ipt.txn_type_id as gt_txn_id,
cast(credit_debit as int) as credit_debit,
txn_head as txn_head,
'points_mtt_ipt' as table_name,
sum(mtt_ipt.amount) as amt
from
nikesh.mtt_points_in_play_transaction mtt_ipt,
txn_type tt
where mtt_ipt.txn_type_id = tt.id
and mtt_ipt.update_date between '{0}' and '{1}'
group by 
mtt_ipt.update_date,mtt_ipt.user_id,mtt_ipt.txn_type_id,credit_debit,txn_head,'points_mtt_ipt'

) pt_ipt_st

group by 
r_date,
user_id,
gt_txn_id,
credit_debit,
txn_head,
table_name;""".format(start_date,end_date))



# COMMAND ----------



spark.sql(""" insert overwrite table nikesh.rev_mvmt_base_pt_ipt_st_report partition(r_date) 
select user_id,
gt_txn_id,
credit_debit,
txn_head,
table_name,
amount,
r_date
from nikesh.rev_mvmt_base_monthly_pt_ipt_st
where r_date between '{0}' and '{1}'; """.format(start_date,end_date))



# COMMAND ----------


spark.sql("""drop table if exists nikesh.rev_mvmt_bonus_amount_monthly;""")
spark.sql("""create table nikesh.rev_mvmt_bonus_amount_monthly as
select pt.update_date as report_date,
pt.user_id,
bb.promo_code , 
bb.promo_type,
sum(case when pt.txn_type_id = 28 then coalesce(pt.amount,0) end) as bonus_amount_rc,
sum(case when pt.txn_type_id = 206 then coalesce(pt.amount,0) end) as bonus_amount_ft
from 
player_transaction pt,
txn_type tt,
user_preference up,
(select distinct promo_code,promo_type
from bonus_promotions) bb
where  pt.txn_type_id = tt.id
and pt.user_id = up.user_id
and pt.update_date between '{0}' and '{1}'
and bb.promo_code = substr(pt.meta_data,1,(length(pt.meta_data) - length(substring_index(pt.meta_data,'_',-1))) - 1)
and pt.txn_type_id in (28,206)
group by pt.update_date,
pt.user_id,
bb.promo_code, 
bb.promo_type;""".format(start_date,end_date))

# COMMAND ----------

spark.sql(""" insert overwrite table nikesh.rev_mvmt_base_bonus_report partition(r_date) 
select user_id,
promo_code,
promo_type,
bonus_amount_rc,
bonus_amount_ft,
report_date 
from nikesh.rev_mvmt_bonus_amount_monthly
where report_date between '{0}' and '{1}';""".format(start_date,end_date))

# COMMAND ----------
