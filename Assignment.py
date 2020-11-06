#!/usr/bin/env python
# coding: utf-8

# # Assignment Solution

# ### Importing packages and files

# In[588]:


import pandas as pd


# In[589]:


order_list=pd.read_csv('C:/Users/lmeer/Documents/Personal/Assignments/Robotics/assignment_1_order_list (3).csv', index_col=0)


# In[590]:


status_list=pd.read_csv('C:/Users/lmeer/Documents/Personal/Assignments/Robotics/assignment_1_status_list (1) (3).csv', index_col=0)


# #### Converting the datasets into DataFrames 

# In[591]:


orders=pd.DataFrame(order_list)


# In[592]:


statuses=pd.DataFrame(status_list)


# #### Exploring the two datasets

# In[593]:


orders.head()


# In[594]:


statuses.head()


# <font color=purple>__Since both of the datasets have ORDER_ID as an index and it doesn't contribute any ease of access to information, I will reset it and convert it to a column in the dataframe__</font>

# In[595]:


orders.reset_index(inplace=True)


# In[596]:


statuses.reset_index(inplace=True)


# In[597]:


orders.head()


# In[598]:


statuses.head()


# <font color=purple>__Changing the upper case to lower for the column names so it would be easier to work with down the road__</font>

# In[599]:


orders.columns=orders.columns.str.lower()


# In[600]:


statuses.columns=statuses.columns.str.lower()


# In[601]:


orders.head()


# In[602]:


statuses.head()


# <font color=purple>__Exploring the data for missing values__</font>

# In[603]:


orders.isna().any()


# In[604]:


statuses.isna().any()


# <font color=purple>__Exploring the data types of each dataframe__</font>

# In[605]:


orders.dtypes


# In[606]:


statuses.dtypes


# <font color=purple>__Exploring data size__</font>

# In[607]:


orders.shape


# In[608]:


statuses.shape


# <font color=purple>__In order to understand the data a little bit better, I want to see how many unique orders there are in each table__</font>

# In[609]:


orders['order_id'].nunique()


# In[610]:


statuses['order_id'].nunique()


# ### Question 1

# ### Since the status_time is a string we convert it into an object before proceeding

# In[611]:


statuses['status_time'] = pd.to_datetime(statuses['status_time'])


# In[612]:


statuses.info()


# ### checking for a random order the different status times, we can see that 'in_progress' appears twice, we only want the last updated time

# In[613]:


statuses[statuses["order_id"].isin(["2237317"])]


# #### For each order and status, find the last time the status was updated

# In[614]:


lat_sts_time=pd.DataFrame(statuses.groupby(["order_id","order_status"])["status_time"].max())


# #### Making sure that after the update there is only one status time for each order and order status

# In[615]:


g = lat_sts_time.groupby(['order_status','order_id'])


# In[616]:


g.filter(lambda x: len(x) > 1)


# #### Converting the indexes of dataframe lat_sts_time to columns

# In[617]:


lat_sts_time.reset_index(inplace=True)


# #### also, we can see that for the previous checked order_id there is only one status time, the latest

# In[618]:


lat_sts_time[lat_sts_time["order_id"].isin(["2237317"])]


# #### Since there are multiple rows for an order_id and in the final result we only want to see one record per order_id, I've pivoted the dataframe

# In[619]:


status_p = lat_sts_time.pivot(index='order_id', columns='order_status', values='status_time')


# In[620]:


status_p.head()


# #### Changing the order of hte order_status columns to fit the process

# In[621]:


status_p=status_upd[['in_progress','picked','started_dispatch','in_dispatch_buffer','packed','dispatched','order_aborted']]


# In[622]:


status_p.head()


# #### Convert again the order_id from index to a clomun

# In[623]:


status_p.reset_index(inplace=True)


# In[624]:


status_p.info()


# In[625]:


status_p.shape


# In[626]:


status_p.head()


# In[627]:


status_p.columns.tolist()


# In[628]:


status_p.info()


# #### Converting all status times to datetime objects

# In[629]:


cols_7_extract = status_p.columns[1:8]


# In[630]:


status_p[cols_7_extract] = status_p[cols_7_extract].applymap(lambda x : pd.to_datetime(x))


# #### Now all the time values are inthe correct format

# In[631]:


status_p.info()


# #### Performing a full outer join to the orders dataframe
# #### the reason for the outer join is because some orders may exist in one table but not in the other
# #### For example, there could be orders creates but haven't started the status process

# In[632]:


merged_df = pd.merge(status_p,orders,how='outer',on=['order_id'])


# In[633]:


merged_df.head()


# In[634]:


merged_df.shape


# In[635]:


merged_df.info()


# #### DataFrame to csv

# In[636]:


merged_df.to_csv("Answer_1.csv")


# ## Question 2

# ### The steps for this answer is:
# 1. Convert order_created_at column to a datetime (it is a string now)
# 2. Substract the order_created_at from the dispatched column to get the process time for each order
# 3. Calculate the mean of these processes times

# #### <font color=red>step 1: Convert order_created_at column to a datetime</font>

# #### After exploring the merged dataframe, we see that there are 25 records where there is no value for the creation of the order, we will remove these records before proceeding

# In[637]:


merged_df['order_created_at'].isna().sum()


# In[638]:


merged_df.dropna(subset=['order_created_at'],how='any',inplace=True)


# In[639]:


merged_df['order_created_at'].isna().sum()


# In[640]:


merged_df.info()


# In[641]:


merged_df['order_created_at'] = pd.to_datetime(merged_df['order_created_at'])


# #### Verifying that the change took place

# In[642]:


assert merged_df['order_created_at'].dtype=='datetime64[ns]'


# #### Since we need the process time, we need to take only the records that were dispatched, we will hence filter out all the records that haven't got a value in the 'dispatched' column, we can see below that there are 54 records that need to be filtered out

# In[643]:


merged_df['dispatched'].isna().sum()


# In[644]:


merged_df.dropna(subset=['dispatched'],how='any',inplace=True)


# In[645]:


merged_df['dispatched'].isna().sum()


# #### Since there are orders that were aborted, we need to remove them from the result as well

# In[646]:


merged_df=merged_df[merged_df['order_aborted'].isnull()]


# #### We are remained with order_aborted column with only null values, meaning, none of the orders was aborted

# In[647]:


merged_df['order_aborted'].notnull().count()


# #### <font color=red>Step 2: Substract the order_created_at from the dispatched column to get the process time for each order</font>

# In[648]:


merged_df['pr_time']=merged_df['dispatched']-merged_df['order_created_at']


# In[649]:


merged_df


# #### <font color=red>step 3: Calculate the mean of these processes times</font>

# In[650]:


print(merged_df['pr_time'].mean())


# ## Question 3

# ### Steps for solution:
# 1. Convert delivery_schedule column to datetime
# 1. Filter only the orders that were packed from the previous df_merged dataframe
# 2. Filter only the records that their packing time is before their delivery schedule time

# #### <font color=magenta>step 1: Convert delivery_schedule column to datetime</font>

# In[651]:


merged_df['delivery_schedule'] = pd.to_datetime(merged_df['delivery_schedule'])


# In[652]:


assert merged_df['delivery_schedule'].dtype=='datetime64[ns]'


# #### <font color=magenta>step 2: Filter only the packed orders, as we can see all orders were packed</font>

# In[653]:


merged_df['packed'].isna().sum()


# #### Filtering the orders that were pcked before their delivery schedule

# In[654]:


early_pack=merged_df[merged_df['packed']<merged_df['delivery_schedule']]


# In[655]:


early_pack.head()


# In[656]:


early_pack.shape


# In[658]:


early_pack.to_csv("Answer_3_early_pack.csv")


# ## Question 3a 
# ### KPI - Average,  Interquartile range and median of delivery time
# #### I will consider the trip_id column as a time the order was delivered to its destination
# #### I will  calculate then the delivery time since the mooment the order was created in the system
# #### <font color=red>_Median is much more robust for skewed dataset_</font>
# #### **steps**
# 1. Cleaning the trip_id column
# 2. Creating a column named delivered_on
# 3. Convering new column to datetime
# 4. Creating a column containig the difference between the delivered_on time and the order_created_at time

# In[659]:


merged_df.head()


# #### <font color=magenta>step 1+2: Cleaning trip_id column and Creating a column named delivered_on</font>

# In[667]:


merged_df['delivered_on']=merged_df['trip_id'].str.replace('_', ' ')


# In[668]:


merged_df['delivered_on']=merged_df['delivered_on'].str.replace('[a-z]', '')


# In[670]:


merged_df.head()


# In[671]:


merged_df.info()


# #### <font color=magenta>step 3: Converting the new column to datime</font>

# In[672]:


merged_df['delivered_on'] = pd.to_datetime(merged_df['delivered_on'])


# In[674]:


assert merged_df['delivered_on'].dtype=='datetime64[ns]'


# In[676]:


merged_df['delivered_time']=merged_df['delivered_on']-merged_df['order_created_at']


# In[720]:


merged_df.head()


# #### <font color=magenta>step 4: Calculating the KPI's</font>

# In[680]:


import numpy as np


# In[681]:


def iqr(column):
    return column.quantile(0.75)-column.quantile(0.25)


# In[683]:


print(merged_df['delivered_time'].agg([iqr,np.median,np.mean]))


# #### interquartile range or the measure of spread os close to the mean and the median
# #### _The spread of delivery time is not large, that means that the different carriers distributing the packages are completing the distribution more or less in the same time interval_
# #### <font color=orange>__There is no advantage or diadvantage of on carries over the other__</font>

# In[725]:


import numpy as np
import time
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns


# In[722]:


merged_df['delivered_time_hours'] = merged_df['delivered_time'] / np.timedelta64(1, 'h')


# In[723]:


merged_df.head()


# In[724]:


merged_df.boxplot(column=['delivered_time_hours'], 
                       grid=False)


# In[733]:


bplot = sns.boxplot(y='delivered_time_hours', 
                 data=merged_df, 
                 width=0.5,
                 palette="colorblind")


# In[734]:


plot_file_name="boxplot_and_swarmplot_with_seaborn.jpg"
 
# save as jpeg
bplot.figure.savefig(plot_file_name,
                    format='jpeg',
                    dpi=100)


# ## Question 3b 
# ### KPI - Maximum, minimum and average number of unique items in one order

# In[739]:


merged_df['num_of_lines']=merged_df['num_of_lines'].astype(int)


# In[740]:


merged_df.info()


# In[749]:


print(merged_df['num_of_lines'].agg([min,max,np.mean]))


# In[758]:


plt.figure(figsize=[10,8])
n, bins, patches = plt.hist(x=merged_df['num_of_lines'], bins=8, color='#0504aa',alpha=0.7, rwidth=0.85)
plt.grid(axis='y', alpha=0.75)
plt.xlabel('unique items',fontsize=15)
plt.ylabel('orders',fontsize=15)
plt.xticks(fontsize=15)
plt.xticks(np.arange(0, 31, 2))
plt.yticks(fontsize=15)
plt.title('Distribution of number of items ordered',fontsize=15)
plt.show()

