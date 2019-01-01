
# coding: utf-8

# In[ ]:

# Sri Gurubhyonamaha


# In[1]:

import findspark


# In[2]:

findspark.init()


# In[3]:

import pandas 
import numpy as np
import copy
from pyspark import *
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import when,col
from pyspark.sql.functions import*
from pyspark import SparkContext 
from pyspark.sql import SQLContext 
from pyspark.sql import SparkSession
import datetime as DT
import dateutil.relativedelta

spark = SparkSession.builder.appName("EA_ATT_SEG").getOrCreate()


# In[4]:

acxiom_cols = ['PO_ID',
               'AGE',
               'PDP_TOT_AMT',
               'PART_BAL',
               'GENDR_CD',
               'WEB_STMT_FL',
               'PART_FND_CNT',
               'VMAP_PART_BGN_DT']


# In[4]:
#colmns=[]

df_pcar = spark.read.option("header","true").option("sep","\t").csv('C:\\Users\\KOGENTIX\\Desktop\\AC\\data_hari\\100_PAR_20180801.csv').select(acxiom_cols)



# In[6]:

att_seg =spark.read.option("header","true").option("sep","\t").csv('C:\\Users\\KOGENTIX\\Desktop\\AC\\data_hari\\100_att_seg_201807.csv')#.select("PO_ID").collect() 



# In[7]:

att_seg.head()
att_seg.createOrReplaceTempView('att_seg')
att_seg = spark.sql("""SELECT PO_ID FROM att_seg""")
att_seg.head()


# In[8]:


df_dc_only = df_pcar.where(~(df_pcar['PO_ID']).isin(att_seg.columns))
df_dc_only.head()


# In[9]:


#ToDo
now = pandas.Timestamp(DT.datetime.now())


# In[8]:

df_dc_only = df_dc_only.withColumn('ADVICE_TYPE', lit('C99_NOT_ADVICE'))
df_dc_only = df_dc_only.withColumn('CLNT_SGMNT_CD',lit('CO'))
df_dc_only = df_dc_only.withColumn('ROTH_ACCT_CNT',lit('1'))
df_dc_only = df_dc_only.withColumn('VBA_VGI_ETF_BAL',lit('0'))
df_dc_only = df_dc_only.withColumn('VBA_VGI_MF_BAL',lit('1'))
df_dc_only = df_dc_only.withColumn('LMF_BAL',lit('0'))
df_dc_only = df_dc_only.withColumn('POSN_CNT',lit('0'))
df_dc_only = df_dc_only.withColumn('ROLLOVER_ACCT_CNT',lit('0'))
df_dc_only = df_dc_only.withColumn('WC_TOT_ASSET',lit('0'))
df_dc_only = df_dc_only.withColumn('BEST_AGE',lit('PART_BAL'))
df_dc_only = df_dc_only.withColumn('BRKG_E_DELVRY_FLAG',lit('AGE'))
df_dc_only = df_dc_only.withColumn('ABUY_SELL_TOT_CNT',lit('N'))
df_dc_only = df_dc_only.withColumn('TRADITIONAL_ACCT_CNT',lit('0'))
df_dc_only = df_dc_only.withColumn('TAXABLE_ACCT_CNT',lit('0'))
df_dc_only = df_dc_only.withColumn('ADVICE_TYPE',lit('0'))
df_dc_only=df_dc_only.withColumn('WEB_STMT_PREFNC_FL',lit('WEB_STMT_FL'))
df_dc_only = df_dc_only.withColumn('WATCH_LIST_FL',when(col('WEB_STMT_FL') == 'Y', 'Y').otherwise('N'))


# In[9]:

df_dc_only.columns


# In[10]:

from pyspark.sql.types import *
schema = StructType([]) #BUY_SELL_TOT_CNT,CLNT_TENURE_YEAR,IIG_LOGON_CNT,VGI_BUY_CNT
wec_pe =spark.createDataFrame(spark.sparkContext.emptyRDD(), schema) #spark.read.option("header","true").csv('C:\\Users\\ubwv\\Desktop\\EA\\100_uniq_poid_pe_wec_red.csv')
wec_pe = wec_pe.withColumn('BUY_SELL_TOT_CNT', lit(None).cast(StringType()))
wec_pe = wec_pe.withColumn('CLNT_TENURE_YEAR', lit(None).cast(StringType()))
wec_pe = wec_pe.withColumn('IIG_LOGON_CNT', lit(None).cast(StringType()))
wec_pe = wec_pe.withColumn('VGI_BUY_CNT', lit(None).cast(StringType()))
wec_pe = wec_pe.withColumn('po_id', lit(None).cast(StringType()))
wec_pe = wec_pe.withColumn('total_pgvw360', lit(None).cast(StringType()))


# In[11]:

df_dc_only = df_dc_only.withColumnRenamed("po_id","PO_ID")


# In[14]:

#df_dc_only = df_dc_only.withColumnRenamed("IIG_LOGON_CNT","IIG_LOGON_CNT_r")
df_dc_only = df_dc_only.join(wec_pe, ['po_id'], how = 'left')


# In[15]:


print(df_dc_only.columns)


# In[16]:




# In[12]:


df_dc_only = df_dc_only.withColumn("IIG_LOGON_CNT",col("total_pgvw360"))
df_dc_only.head()


# In[17]:


df_dc_only = df_dc_only.select(['PO_ID','ADVICE_TYPE','BEST_AGE','BRKG_E_DELVRY_FLAG','BUY_SELL_TOT_CNT','CLNT_SGMNT_CD','CLNT_TENURE_YEAR','GENDR_CD','LMF_BAL','POSN_CNT','IIG_LOGON_CNT','ROLLOVER_ACCT_CNT','ROTH_ACCT_CNT','TAXABLE_ACCT_CNT','TRADITIONAL_ACCT_CNT','VBA_VGI_ETF_BAL','VBA_VGI_MF_BAL','VGI_BUY_CNT','WATCH_LIST_FL','WC_TOT_ASSET','WEB_STMT_PREFNC_FL'])




# In[13]:

# In[ ]:


#ADVICE_TYPE

df_dc_only=df_dc_only.withColumn("ADVICE_TYPE_1",when(df_dc_only["ADVICE_TYPE"]=="C01_PAS_CURRENT",1).otherwise(0))  #== check whether it is true or false and *1 changes the True/False to 1/0
df_dc_only=df_dc_only.withColumn("ADVICE_TYPE_2",when(df_dc_only["ADVICE_TYPE"]=="C02_PLAN_GEN",1).otherwise(0)   )
df_dc_only=df_dc_only.withColumn("ADVICE_TYPE_3",when(df_dc_only["ADVICE_TYPE"]=="C03_ADVICE_REC",1).otherwise(0) )  
df_dc_only=df_dc_only.withColumn("ADVICE_TYPE_4",when(df_dc_only["ADVICE_TYPE"]=="C04_PROFL_GEN",1).otherwise(0) ) 
df_dc_only=df_dc_only.withColumn("ADVICE_TYPE_99",when(df_dc_only["ADVICE_TYPE"]=="C99_NOT_ADVICE",1).otherwise(0) )


# In[14]:

#BEST_AGE
df_dc_only=df_dc_only.withColumn("BEST_AGE_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("BEST_AGE_buckets",when(df_dc_only["BEST_AGE"]<=34,1).otherwise(when((df_dc_only["BEST_AGE"]>=35) & (df_dc_only["BEST_AGE"]<=49),2).otherwise(when((df_dc_only["BEST_AGE"]>=50) & (df_dc_only["BEST_AGE"]<=64),3).otherwise(when(df_dc_only["BEST_AGE"]>=65,4).otherwise(df_dc_only["BEST_AGE_buckets"])))))


# In[ ]:


# need to check 
#BRKG_E_DELVRY_FLAG    
df_dc_only=df_dc_only.withColumn("BRKG_E_DELVRY_FLAG_1",when(df_dc_only["BRKG_E_DELVRY_FLAG"]=="Y",1).otherwise(0))


# In[15]:

#BUY_SELL_TOT_COUNT

df_dc_only=df_dc_only.withColumn("BUY_SELL_TOT_COUNT_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("BUY_SELL_TOT_COUNT_buckets",when((df_dc_only["BUY_SELL_TOT_CNT"]>=1) & (df_dc_only["BUY_SELL_TOT_CNT"]<=4),1).otherwise(when((df_dc_only["BUY_SELL_TOT_CNT"]>=5) & (df_dc_only["BUY_SELL_TOT_CNT"]<=15),2).otherwise(when((df_dc_only["BUY_SELL_TOT_CNT"]>=16) & (df_dc_only["BUY_SELL_TOT_CNT"]<=30),3).otherwise(when(df_dc_only["BUY_SELL_TOT_CNT"]>=31,4).otherwise(df_dc_only["BEST_AGE_buckets"])))))


# In[ ]:


##CLNT_SGMNT_CD
df_dc_only=df_dc_only.withColumn("CLNT_SGMNT_CD_CO",when(df_dc_only["CLNT_SGMNT_CD"]=="CO",(df_dc_only["CLNT_SGMNT_CD"])*1).otherwise(0))
df_dc_only=df_dc_only.withColumn("CLNT_SGMNT_CD_FL",when(df_dc_only["CLNT_SGMNT_CD"]=="FL",(df_dc_only["CLNT_SGMNT_CD"])*1).otherwise(0))
df_dc_only=df_dc_only.withColumn("CLNT_SGMNT_CD_VS",when(df_dc_only["CLNT_SGMNT_CD"]=="VS",(df_dc_only["CLNT_SGMNT_CD"])*1).otherwise(0))
df_dc_only=df_dc_only.withColumn("CLNT_SGMNT_CD_VY",when(df_dc_only["CLNT_SGMNT_CD"]=="VY",(df_dc_only["CLNT_SGMNT_CD"])*1).otherwise(0))


# In[ ]:


#CLNT_TENURE_YEAR

df_dc_only=df_dc_only.withColumn("CLNT_TENURE_YEAR_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("BUY_SELL_TOT_COUNT_buckets",when((df_dc_only["CLNT_TENURE_YEAR"]>=0) & (df_dc_only["CLNT_TENURE_YEAR"]<=4),1).otherwise(when((df_dc_only["CLNT_TENURE_YEAR"]>=5) & (df_dc_only["CLNT_TENURE_YEAR"]<=10),2).otherwise(when((df_dc_only["CLNT_TENURE_YEAR"]>=11) & (df_dc_only["CLNT_TENURE_YEAR"]<=15),3).otherwise(when((df_dc_only["CLNT_TENURE_YEAR"]>=16) & (df_dc_only["CLNT_TENURE_YEAR"]<=19),4).otherwise(when(df_dc_only["CLNT_TENURE_YEAR"]>=20,5).otherwise(df_dc_only["BEST_AGE_buckets"]))))))


# In[ ]:
df_dc_only["LMF_BAL"]>0

#GENDER_CD
df_dc_only=df_dc_only.withColumn("GENDER_CD_1",when(df_dc_only["GENDR_CD"]=="M",1).otherwise(0))



# In[ ]:


#LMF_BAL
df_dc_only=df_dc_only.withColumn("LMF_BAL_1",when((df_dc_only["LMF_BAL"]>0),1).otherwise(0))


# In[ ]:


#POSN_CNT
df_dc_only=df_dc_only.withColumn("POSN_CNT_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("POSN_CNT_buckets",when((df_dc_only["POSN_CNT"]>=0) & (df_dc_only["POSN_CNT"]<=1),1).otherwise(when((df_dc_only["POSN_CNT"]==2),2).otherwise(when((df_dc_only["POSN_CNT"]==3),3).otherwise(when((df_dc_only["POSN_CNT"]==4),4).otherwise(when((df_dc_only["POSN_CNT"]==5),5).otherwise(when((df_dc_only["POSN_CNT"]==6),6).otherwise(when((df_dc_only["POSN_CNT"]>=7) & (df_dc_only["POSN_CNT"]<=10),7).otherwise(when((df_dc_only["POSN_CNT"]>=11) & (df_dc_only["POSN_CNT"]<=15),8).otherwise(when(df_dc_only["POSN_CNT"]>=16,9).otherwise(df_dc_only["POSN_CNT_buckets"]))))))))))


# In[17]:

# In[ ]:


#IIG_LOGON_CNT
#IIG_LOGON_CNT
df_dc_only=df_dc_only.withColumn("IIG_LOGON_CNT_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("IIG_LOGON_CNT_buckets",when((df_dc_only["IIG_LOGON_CNT"]>=0) & (df_dc_only["IIG_LOGON_CNT"]<=2),1).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=3) & (df_dc_only["IIG_LOGON_CNT"]<=5),2).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=6) & (df_dc_only["IIG_LOGON_CNT"]<=9),3).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=10) & (df_dc_only["IIG_LOGON_CNT"]<=15),4).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=16) & (df_dc_only["IIG_LOGON_CNT"]<=20),5).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=21) & (df_dc_only["IIG_LOGON_CNT"]<=30),6).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=31) & (df_dc_only["IIG_LOGON_CNT"]<=40),7).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=41) & (df_dc_only["IIG_LOGON_CNT"]<=50),8).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=51) & (df_dc_only["IIG_LOGON_CNT"]<=75),9).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=76) & (df_dc_only["IIG_LOGON_CNT"]<=100),10).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=101) & (df_dc_only["IIG_LOGON_CNT"]<=200),11).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=201) & (df_dc_only["IIG_LOGON_CNT"]<=300),12).otherwise(when((df_dc_only["IIG_LOGON_CNT"]>=301) & (df_dc_only["IIG_LOGON_CNT"]<=400),13).otherwise(when(df_dc_only["IIG_LOGON_CNT"]>=401,401).otherwise(df_dc_only["IIG_LOGON_CNT_buckets"])))))))))))))))


# In[ ]:



#ROLLOVER_ACCT_CNT 

df_dc_only=df_dc_only.withColumn("ROLLOVER_ACCT_CNT_1",lit(0))
df_dc_only=df_dc_only.withColumn("ROLLOVER_ACCT_CNT_1",when((df_dc_only["ROLLOVER_ACCT_CNT"]>0),1).otherwise(df_dc_only["ROLLOVER_ACCT_CNT"]))


# In[ ]:


#ROTH_ACCT_CNT 

df_dc_only=df_dc_only.withColumn("ROTH_ACCT_CNT_1",lit(0))
df_dc_only=df_dc_only.withColumn("ROTH_ACCT_CNT_1",when((df_dc_only["ROTH_ACCT_CNT"]>0),1).otherwise(df_dc_only["ROTH_ACCT_CNT"]))


# In[ ]:


#TAXABLE_ACCT_CNT  

df_dc_only=df_dc_only.withColumn("TAXABLE_ACCT_CNT_1",lit(0))
df_dc_only=df_dc_only.withColumn("TAXABLE_ACCT_CNT_1",when((df_dc_only["TAXABLE_ACCT_CNT"]>0),1).otherwise(df_dc_only["TAXABLE_ACCT_CNT"]))


# In[18]:

#TRADITIONAL_ACCT_CNT   

df_dc_only=df_dc_only.withColumn("TRADITIONAL_ACCT_CNT_1",lit(0))
df_dc_only=df_dc_only.withColumn("TRADITIONAL_ACCT_CNT_1",when((df_dc_only["TAXABLE_ACCT_CNT"]>0),1).otherwise(df_dc_only["TAXABLE_ACCT_CNT"]))


# In[ ]:


#VBA_VGI_ETF_BAL

df_dc_only=df_dc_only.withColumn("VBA_VGI_ETF_BAL_1",lit(0))
df_dc_only=df_dc_only.withColumn("VBA_VGI_ETF_BAL_1",when((df_dc_only["VBA_VGI_ETF_BAL"]>0),1).otherwise(df_dc_only["VBA_VGI_ETF_BAL"]))


# In[ ]:


#VBA_VGI_MF_BAL  

df_dc_only=df_dc_only.withColumn("VBA_VGI_MF_BAL_1",lit(0))
df_dc_only=df_dc_only.withColumn("VBA_VGI_MF_BAL_1",when((df_dc_only["VBA_VGI_MF_BAL"]>0),1).otherwise(df_dc_only["VBA_VGI_MF_BAL"]))


# In[ ]:


#VGI_BUY_CNT   


df_dc_only=df_dc_only.withColumn("VGI_BUY_CNT_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("VGI_BUY_CNT_buckets",when((df_dc_only["VGI_BUY_CNT"]>=1) & (df_dc_only["VGI_BUY_CNT"]<=2),1).otherwise(when((df_dc_only["VGI_BUY_CNT"]>=3),2).otherwise(df_dc_only["VGI_BUY_CNT"])))


# In[19]:

#WATCH_LIST_FL

df_dc_only=df_dc_only.withColumn("WATCH_LIST_FL_1",lit(0))
df_dc_only=df_dc_only.withColumn("WATCH_LIST_FL_1",when((df_dc_only["WATCH_LIST_FL"]=="Y"),1).otherwise(0))


# In[ ]:


# In[20]:



#WC_TOT_ASSET
df_dc_only=df_dc_only.withColumn("WC_TOT_ASSET_buckets",lit(0))
df_dc_only=df_dc_only.withColumn("WC_TOT_ASSET_buckets",when(df_dc_only["WC_TOT_ASSET"]<=26000,1).otherwise(when((df_dc_only["WC_TOT_ASSET"]>=26001) & (df_dc_only["WC_TOT_ASSET"]<=190000),2).otherwise(when((df_dc_only["WC_TOT_ASSET"]>=190001) & (df_dc_only["WC_TOT_ASSET"]<=320000),3).otherwise(when((df_dc_only["WC_TOT_ASSET"]>=320001) & (df_dc_only["WC_TOT_ASSET"]<=485000),4).otherwise(when((df_dc_only["WC_TOT_ASSET"]>=485001) & (df_dc_only["WC_TOT_ASSET"]<=1090000),5).otherwise(when((df_dc_only["WC_TOT_ASSET"]>=1090001) & (df_dc_only["WC_TOT_ASSET"]<=1710000),6).otherwise(when((df_dc_only["WC_TOT_ASSET"]>=1710001),7).otherwise(0))))))))



# In[21]:

#WEB_STMT_PREFNC_FL
df_dc_only=df_dc_only.withColumn("WEB_STMT_PREFNC_FL_1",lit(0))
df_dc_only=df_dc_only.withColumn("WEB_STMT_PREFNC_FL_2",lit(0))
df_dc_only=df_dc_only.withColumn("WEB_STMT_PREFNC_FL_1",when((df_dc_only["WEB_STMT_PREFNC_FL"]=="N"),1).otherwise(0))
df_dc_only=df_dc_only.withColumn("WEB_STMT_PREFNC_FL_2",when((df_dc_only["WEB_STMT_PREFNC_FL"]=="Y"),1).otherwise(0))


# In[38]:

# In[ ]:


c1 = df_dc_only.select('PO_ID','ADVICE_TYPE_1',
'ADVICE_TYPE_2',
'ADVICE_TYPE_3',
'ADVICE_TYPE_4',
'ADVICE_TYPE_99',
'BEST_AGE_buckets',
'BRKG_E_DELVRY_FLAG_1',
'BUY_SELL_TOT_COUNT_buckets',
'CLNT_SGMNT_CD_CO',
'CLNT_SGMNT_CD_FL',
'CLNT_SGMNT_CD_VS',
'CLNT_SGMNT_CD_VY',
'CLNT_TENURE_YEAR_buckets',
'GENDER_CD_1',
'LMF_BAL_1',
'POSN_CNT_buckets',
'IIG_LOGON_CNT_buckets',
'ROLLOVER_ACCT_CNT_1',
'ROTH_ACCT_CNT_1',
'TAXABLE_ACCT_CNT_1',
'TRADITIONAL_ACCT_CNT_1',
'VBA_VGI_ETF_BAL_1',
'VBA_VGI_MF_BAL_1',
'VGI_BUY_CNT_buckets',
'WATCH_LIST_FL_1',
'WC_TOT_ASSET_buckets',
'WEB_STMT_PREFNC_FL_1',
'WEB_STMT_PREFNC_FL_2'                       
)


# In[7]:



# In[39]:


#acxiom_file = r'C:\\Users\\ubwv\\Desktop\\EA\\Axciom_20181031_10.csv'
acxiom_cols = ['po_id',
               'ap001374_advicepricefullsvcbrkg',
               'ap001380_buysellstockfullsvcbrkg',
               'ap001398_othfullsvcaffin',
               'ap001402_metfinplanner',
               'ap001403_usepersnlmonmgr',
               'ap002719_heavyfbuser',
               'ap002727_businessfan']

 #ax = spark.read.option("header","true").csv('C:\\Users\\ubwv\\Desktop\\EA\\100_Axciom_20181031.csv').select(acxiom_cols)


# In[40]:

ax = spark.read.option("header","true").option("sep","\t").csv('C:\\Users\\KOGENTIX\\Desktop\\AC\\data_hari\\Axciom_20181031_10.csv').select(acxiom_cols)


# In[ ]:


ax=ax.fillna(np.nan)
#ax[acxiom_cols[1:]] = ax[acxiom_cols[1:]].astype(float)
#ax['po_id'] = ax['po_id'].astype(int)


# In[ ]:


#ap001374_advicepricefullsvcbrkg

ax=ax.withColumn("ap001374_advicepricefullsvcbrkg_1",when((ax["ap001374_advicepricefullsvcbrkg"]==1),(ax["ap001374_advicepricefullsvcbrkg"])*1).otherwise(0))
ax=ax.withColumn("ap001374_advicepricefullsvcbrkg_2",when((ax["ap001374_advicepricefullsvcbrkg"]==2),(ax["ap001374_advicepricefullsvcbrkg"])*1).otherwise(0))
ax=ax.withColumn("ap001374_advicepricefullsvcbrkg_3",when((ax["ap001374_advicepricefullsvcbrkg"]==3),(ax["ap001374_advicepricefullsvcbrkg"])*1).otherwise(0))
ax=ax.withColumn("ap001374_advicepricefullsvcbrkg_4to5",when((ax["ap001374_advicepricefullsvcbrkg"]>=4) & (ax["ap001374_advicepricefullsvcbrkg"]<=5),1).otherwise(0))
ax=ax.withColumn("ap001374_advicepricefullsvcbrkg_6to10",when((ax["ap001374_advicepricefullsvcbrkg"]>=6) & (ax["ap001374_advicepricefullsvcbrkg"]<=10),1).otherwise(0))


# In[41]:




# In[ ]:


#ap001380_buysellstockfullsvcbrkg_1to3
ax=ax.withColumn("ap001380_buysellstockfullsvcbrkg_1to3",when((ax["ap001380_buysellstockfullsvcbrkg"]>=1) & (ax["ap001380_buysellstockfullsvcbrkg"]<=3),1).otherwise(0))


# In[42]:

#ap001398_othfullsvcaffin

#ap001398_othfullsvcaffin

ax=ax.withColumn("ap001398_othfullsvcaffin_1",when((ax["ap001398_othfullsvcaffin"]==1),(ax["ap001398_othfullsvcaffin"])*1).otherwise(0))

ax=ax.withColumn("ap001398_othfullsvcaffin_2",when((ax["ap001398_othfullsvcaffin"]==2),(ax["ap001398_othfullsvcaffin"])*1).otherwise(0))

ax=ax.withColumn("ap001398_othfullsvcaffin_3",when((ax["ap001398_othfullsvcaffin"]==3),(ax["ap001398_othfullsvcaffin"])*1).otherwise(0))

ax=ax.withColumn("ap001398_othfullsvcaffin_4to5",when((ax["ap001398_othfullsvcaffin"]>=4) & (ax["ap001398_othfullsvcaffin"]<=5),1).otherwise(0))
ax=ax.withColumn("ap001398_othfullsvcaffin_6to10",when((ax["ap001398_othfullsvcaffin"]>=6) & (ax["ap001398_othfullsvcaffin"]<=10),1).otherwise(0))


# In[ ]:





# In[43]:

#ap001402_metfinplanner

ax=ax.withColumn("ap001402_metfinplanner_1to5",when((ax["ap001402_metfinplanner"]>=1) & (ax["ap001402_metfinplanner"]<=5),1).otherwise(0))

ax=ax.withColumn("ap001402_metfinplanner_6to9",when((ax["ap001402_metfinplanner"]>=6) & (ax["ap001402_metfinplanner"]<=9),1).otherwise(0))

ax=ax.withColumn("ap001402_metfinplanner_10plus",when((ax["ap001402_metfinplanner"]>=10),1).otherwise(0))


# In[ ]:


#ap001403_usepersnlmonmgr

ax=ax.withColumn("ap001403_usepersnlmonmgr_1",when((ax["ap001403_usepersnlmonmgr"]==1),(ax["ap001403_usepersnlmonmgr"])*1).otherwise(0))
ax=ax.withColumn("ap001403_usepersnlmonmgr_2",when((ax["ap001403_usepersnlmonmgr"]==2),(ax["ap001403_usepersnlmonmgr"])*1).otherwise(0))
ax=ax.withColumn("ap001403_usepersnlmonmgr_3",when((ax["ap001403_usepersnlmonmgr"]==3),(ax["ap001403_usepersnlmonmgr"])*1).otherwise(0))
ax=ax.withColumn("ap001403_usepersnlmonmgr_4to5",when((ax["ap001403_usepersnlmonmgr"]>=4) & (ax["ap001403_usepersnlmonmgr"]<=5),1).otherwise(0))
ax=ax.withColumn("ap001403_usepersnlmonmgr_6to10",when((ax["ap001403_usepersnlmonmgr"]>=6) & (ax["ap001403_usepersnlmonmgr"]<=10),1).otherwise(0))


# In[ ]:


#AP002719_HEAVYFBUSER

ax=ax.withColumn("ap002719_heavyfbuser_1",when((ax["ap002719_heavyfbuser"]>=1) & (ax["ap002719_heavyfbuser"]<=10),1).otherwise(0))
ax=ax.withColumn("ap002719_heavyfbuser_2",when((ax["ap002719_heavyfbuser"]>=11) & (ax["ap002719_heavyfbuser"]<=20),1).otherwise(0))

ax=ax.withColumn("ap002719_heavyfbuser_5to12",when((ax["ap002719_heavyfbuser"]>=5) & (ax["ap002719_heavyfbuser"]<=12),1).otherwise(0))




# In[44]:

# In[ ]:


#AP002727_BUSINESSFAN

ax=ax.withColumn("ap002727_businessfan_8to10",when((ax["ap002727_businessfan"]>=8) & (ax["ap002727_businessfan"]<=10),1).otherwise(0))
ax=ax.withColumn("ap002727_businessfan_11to12",when((ax["ap002727_businessfan"]>=11) & (ax["ap002727_businessfan"]<=12),1).otherwise(0))

ax=ax.withColumn("ap002727_businessfan_13to14",when((ax["ap002727_businessfan"]>=13) & (ax["ap002727_businessfan"]<=14),1).otherwise(0))

ax=ax.withColumn("ap002727_businessfan_15to16",when((ax["ap002727_businessfan"]>=15) & (ax["ap002727_businessfan"]<=16),1).otherwise(0))

ax=ax.withColumn("ap002727_businessfan_17andup",when((ax["ap002727_businessfan"]>=17),1).otherwise(0))


# In[ ]:


ax = ax.withColumnRenamed("PO_ID",'po_id')
ax.head()


# In[45]:

ax.columns


# In[46]:

ax.registerTempTable('ax')
a1 = spark.sql(""" SELECT PO_ID,
ap001374_advicepricefullsvcbrkg_1,
ap001374_advicepricefullsvcbrkg_2,
ap001374_advicepricefullsvcbrkg_3,
ap001374_advicepricefullsvcbrkg_4to5,
ap001374_advicepricefullsvcbrkg_6to10,
ap001380_buysellstockfullsvcbrkg_1to3,
ap001398_othfullsvcaffin_1,
ap001398_othfullsvcaffin_2,
ap001398_othfullsvcaffin_3,
ap001398_othfullsvcaffin_4to5,
ap001398_othfullsvcaffin_6to10,
ap001402_metfinplanner_1to5,
ap001402_metfinplanner_6to9,
ap001402_metfinplanner_10plus,
ap001403_usepersnlmonmgr_1,
ap001403_usepersnlmonmgr_2,
ap001403_usepersnlmonmgr_3,
ap001403_usepersnlmonmgr_4to5,
ap001403_usepersnlmonmgr_6to10,
ap002719_heavyfbuser_1,
ap002719_heavyfbuser_2,
ap002719_heavyfbuser_5to12,
ap002727_businessfan_8to10,
ap002727_businessfan_11to12,
ap002727_businessfan_13to14,
ap002727_businessfan_15to16,
ap002727_businessfan_17andup FROM ax""" )
a1short=a1.head(5)
a1.head()

model =  c1.join(a1, 'PO_ID', 'inner').orderBy('PO_ID')


# In[47]:

########################### MODEL 5 (which has 6 varibales)
mult5=spark.read.option("header","True").csv('C:\\Users\\KOGENTIX\\Desktop\\AC\\data_hari\\Att Seg Model 5.csv') #read_csv is a funcction in pandas
#mult5names=pd.read_csv(r'\\vanguard\entdfs\prdentas01cia11\MZippilli\2018\Attitudnal Segmentation\AttSegModel 5Names.csv') #read_csv is a funcction in pandas
con5=spark.read.option("header","True").csv('C:\\Users\\KOGENTIX\\Desktop\\AC\\data_hari\\con5.csv') #read_csv is a funcction in pandas


# In[ ]:


mod5=['ADVICE_TYPE_1',
'ADVICE_TYPE_2',
'ADVICE_TYPE_3',
'ADVICE_TYPE_4',
'ADVICE_TYPE_99',
'ap001380_buysellstockfullsvcbrkg_1to3',
'ap001402_metfinplanner_10plus',
'ap001402_metfinplanner_1to5',
'ap001402_metfinplanner_6to9',
'ap002719_heavyfbuser_1',
'ap002719_heavyfbuser_2',
'ap002727_businessfan_11to12',
'ap002727_businessfan_13to14',
'ap002727_businessfan_15to16',
'ap002727_businessfan_17andup',
'ap002727_businessfan_8to10',
'BEST_AGE_buckets',
'BRKG_E_DELVRY_FLAG_1',
'BUY_SELL_TOT_COUNT_buckets',
'CLNT_SGMNT_CD_CO',
'CLNT_SGMNT_CD_FL',
'CLNT_SGMNT_CD_VS',
'CLNT_SGMNT_CD_VY',
'CLNT_TENURE_YEAR_buckets',
'GENDER_CD_1',
'LMF_BAL_1',
'POSN_CNT_buckets',
'IIG_LOGON_CNT_buckets',
'ROLLOVER_ACCT_CNT_1',
'ROTH_ACCT_CNT_1',
'TAXABLE_ACCT_CNT_1',
'VBA_VGI_ETF_BAL_1',
'VBA_VGI_MF_BAL_1',
'VGI_BUY_CNT_buckets',
'WATCH_LIST_FL_1',
'WC_TOT_ASSET_buckets',
'WEB_STMT_PREFNC_FL_1',
'WEB_STMT_PREFNC_FL_2']



# In[50]:

model5copy.columns


# In[52]:

model5copy=model.toPandas()
mult5=mult5.toPandas()
#

# In[ ]:


for n in range(6):
    sum_name="SEG_"+str(n+1)+"_SUM"
    model5copy[sum_name]=model5copy[mod5].dot(mult5.iloc[:,n].values) #the : refers to all the rows


# In[ ]:

# In[ ]:


m5 = model5copy[['PO_ID','t1','t2','t3','t4','t5','t6']]
m5short=m5.head(50)


# In[ ]:


m5["pred"]=m5[['t1','t2','t3','t4','t5','t6']].max(axis=1)
m5pred=m5.head(50)


# In[ ]:


m5["SEG_NUMBER"]=0
m5.loc[m5["pred"]==m5["t1"],["SEG_NUMBER"]]=1
m5.loc[m5["pred"]==m5["t2"],["SEG_NUMBER"]]=2
m5.loc[m5["pred"]==m5["t3"],["SEG_NUMBER"]]=3
m5.loc[m5["pred"]==m5["t4"],["SEG_NUMBER"]]=4
m5.loc[m5["pred"]==m5["t5"],["SEG_NUMBER"]]=5
m5.loc[m5["pred"]==m5["t6"],["SEG_NUMBER"]]=6

m5fs=m5.head(50)


# In[ ]:


m5["SEG_DESCRIPTION"]=0
m5.loc[m5["SEG_NUMBER"]==[1],["SEG_DESCRIPTION"]]="Confident Wealth-Builders"
m5.loc[m5["SEG_NUMBER"]==[2],["SEG_DESCRIPTION"]]="Unsure Information-Seekers"
m5.loc[m5["SEG_NUMBER"]==[3],["SEG_DESCRIPTION"]]="Professional Advice Delegators"
m5.loc[m5["SEG_NUMBER"]==[4],["SEG_DESCRIPTION"]]="Ambitious Wealth-Builders"
m5.loc[m5["SEG_NUMBER"]==[5],["SEG_DESCRIPTION"]]="Discerning DIYers"
m5.loc[m5["SEG_NUMBER"]==[6],["SEG_DESCRIPTION"]]="Apprehensive Advice-Seekers"


# In[ ]:


m5["PERSONA"]=0
m5.loc[m5["SEG_NUMBER"]==[1],["PERSONA"]]="Red"
m5.loc[m5["SEG_NUMBER"]==[2],["PERSONA"]]="Lavender"
m5.loc[m5["SEG_NUMBER"]==[3],["PERSONA"]]="Peach"
m5.loc[m5["SEG_NUMBER"]==[4],["PERSONA"]]="Violet"
m5.loc[m5["SEG_NUMBER"]==[5],["PERSONA"]]="Orange"
m5.loc[m5["SEG_NUMBER"]==[6],["PERSONA"]]="Sky"


# In[ ]:


ATT_SEG=m5[['PO_ID','SEG_NUMBER','SEG_DESCRIPTION','PERSONA']]

ATT_PERSONA=m5[['PO_ID','PERSONA']]


# In[ ]:


m5short=m5.head(20)

m5['SEG_NUMBER'].value_counts(sort=False)


# In[ ]:


ATT_SEGshort=ATT_SEG.head(20)


# In[ ]:


ATT_SEG['SEG_NUMBER'].unique()


# In[ ]:


ATT_SEG.to_csv(path_or_buf=r''C:\\Users\\ubwv\\Desktop\\EA\\ATT_SEG_201807.csv', sep=',',na_rep='',index=False)


# In[ ]:


rtl_att_seg=spark.read.option("header","True").csv(r'C:\\Users\\ubwv\\Desktop\\EA\\ATT_SEG201809.csv') #read_csv is a funcction in pandas


# In[ ]:


ATT_SEG.columns


# In[ ]:


rtl_att_seg.columns


# In[ ]:
dc

frames = [ATT_SEG, rtl_att_seg]
rtl_iig_att_seg = pandas.concat(frames)


# In[ ]:


rtl_iig_att_seg.write.option("header","True").csv(r'C:\\Users\\ubwv\\Desktop\\EA\\RTL_IIG_ATT_SEG_201809.csv')


