# sri Gurubhyonamaha


import pandas 
import matplotlib.pyplot as plt
import numpy as np
import copy
#from sklearn import preprocessing
#from sklearn.preprocessing import MinMaxScaler
import datetime as DT
import dateutil.relativedelta


df_pcar = spark.read.option("header","true").csv('\\\\vanguard\\entdfs\\vgifs01\\EnterpriseAdvice\\ADA & AI\\Data Sets\\IIG\\pcar\\PAR_20180801.csv').select(acxiom_cols)

df_beh_cluster = spark.read.option('header','true').csv('Rig_cluser_201806_new.csv')

df_beh_cluster.createOrReplaceTempView('att_seg')
df_beh_cluster = spark.sql("""SELECT PO_ID FROM att_seg""")

poids = [row.po_id for row in df_beh_cluster.collect()]
df_beh_cluster.head()


# In[8]:


df_pcar1 = df_pcar.where(~(df_pcar['PO_ID']).isin(poids))
df_dc_only.head()

df_pcar2 = df_pcar1.select('po_id',col('part_bal').alias('vista_bal'),col('web_reg_fl').alias('web_reg_fl_now')\
,'gengr_cd',col('AGE').alias('best_age'),col('MRTL_STATUS_CD').alias('mrtl_status_in_hhld_8609'),\
col('VGI_MF_BOND_BAL').alias('vgi_bond_bal'),col('VGI_MF_MMKT_BAL').alias('vgi_mmkt_bal')\
,col('VGI_MF_STOCK_BAL').alias('rtl_bal'),'VGI_MF_TARGET_BAL','zip_cd',\
col('WEB_STMT_FL').alias('watch_list_fl'))

df_bal2 = df_bal2.withColumn('complex_bal',df_bal2['vgi_bond_bal]+df_bal2['vgi_mmkt_bal'])
df_bal2 = df_bal2.withColumn('rtl_bnd_vgi_pct',when('complex_bal'>0,df_bal2['vgi_bond_bal']/df_bal2['complex_bal']).otherwise(0))
df_bal2 = df_bal2.withColumn('rtl_mmkt_vgi_pct',when('complex_bal'>0,df_bal2['vgi_mmkt_bal']/df_bal2['complex_bal']).otherwise(0))
df_bal2 = df_bal2.withColumn('vista_pct',when('complex_bal'>0,df_bal2['vista_bal']/df_bal2['complex_bal']).otherwise(0))


df_bal2 = df_bal2.withColumn('bin_web_reg_fl_now',when('df_bal2['web_reg_fl_now']=='Y',1).otherwise(0))
df_bal2 = df_bal2.withColumn('bin_watch_list_fl',when('df_bal2['watch_list_fl']=='Y',1).otherwise(0))

df_bal3 = df_bal2.join(mrtl, on=['po_id'], how='left')
df_bal3 = df_bal3.withcolumn('best_gndr',when(((col('gendr_cd') == 'F') | (col('gendr_cd') =='M')),col('gendr_cd'))
.otherwise(when(((col('gendr_8688') == 'F') | (col('gendr_8688') == 'M')),col('gender_8688')).otherwise('U')))


df_bal3 = df_bal3.withcolumn('best_mrtl_status',when((col('mrtl_status_cd') !='UK' and col('mrtl_status_cd')!=''),\
col('mrtl_status_cd').otherwise(when(('mrtl_status_in_hhld_8609')!='X' and  col('mrtl_status_in_hhld_8609') !=''),\
 col('mrtl_status_in_hhld_8609'))otherwise('mrtl_status_cd')))                            

 
df_bal3 = df_bal3.withColumn('best_mrtl_grp',when((df_bal3.best_mrtl_status=='A')|(df_bal3.best_mrtl_status=='M')|(df_bal3.best_mrtl_status=='MA'),
'Married').\
otherwise(when((df_bal3.best_mrtl_status=='B')|(df_bal3.best_mrtl_status=='S')|(df_bal3.best_mrtl_status=='SN'),'Married').\
otherwise(when(col('best_mrtl_status')== 'B','Inferred_Single').\
otherwise(when((col('best_mrtl_status')== 'S' or col('best_mrtl_status') == 'SN'),'Single').\
otherwise(when((df_bal3.best_mrtl_status=='SP')|(df_bal3.best_mrtl_status=='WI')|(df_bal3.best_mrtl_status=='DP')|(df_bal3.best_mrtl_status=='DV'),'Other').\
otherwise(when((df_bal3.best_mrtl_status=='UD')|(df_bal3.best_mrtl_status=='UK')|(df_bal3.best_mrtl_status=='X'),'Unknown')))))))

 
df_bal3 = df_bal3.withColumn('best_mrtl_status_tx',when(col('df_bal3.best_mrtl_status')=='A','Inferred_Married').\
otherwise(when((col('best_mrtl_status')=='M' or col('best_mrtl_status') == 'MA'),'Married').\
otherwise(when(col('best_mrtl_status')== 'B','Inferred_Single').\
otherwise(when((col('best_mrtl_status')== 'S' or col('best_mrtl_status') == 'SN'),'Single').\
otherwise(when(col('best_mrtl_status')== 'SP','Separated').\
otherwise(when(col('best_mrtl_status')== 'WI','Widowed').\
otherwise(when(col('best_mrtl_status')=='DP','Domestic_Partner').\
otherwise(when(col('best_mrtl_status')=='DV' ,'Divorced').\
otherwise(when(col('best_mrtl_status')=='UD' ,'Undisclosed').\
otherwise(when((col('best_mrtl_status')=='UK' or col('best_mrtl_status')== 'X'),'Unknown').\
otherwise('error')))))))))))	


df_bal3 = df_bal3.withColumn('best_gndr_tx',when(col('best_gndr')=='M','Male').\
otherwise(when(col('best_gndr')=='F','Female').\
otherwise(when(col('best_gndr')== 'U','Unknown').\
otherwise('error'))))

df_bal3 = df_bal3.withColumn('female_fl',when(col('best_gndr_tx')=='Female',1).otherwise(0))
df_bal3 = df_bal3.withColumn('single_fl',when(col('best_mrtl_grp')=='Single',1).otherwise(0))
df_bal3 = df_bal3.withColumn('EDUC_9514_HS_FL',lit(1))                                                          
df_bal3 = df_bal3.withColumn('EDUC_9514',lit(1))

                                                      
                                                      
df_bal3 = df_bal3.join(uniq_poid_ret_wec, 'po_id' , how='left').select(df_bal3.columns)
cols_tmp = df_bal3.columns
cols_tmp.append(['Research_pgvw360','IRAs_sess360'])                                                      
df_bal3 = df_bal3.join(uniq_poid_ret_wec, 'po_id' , how='left').select(cols_tmp)
