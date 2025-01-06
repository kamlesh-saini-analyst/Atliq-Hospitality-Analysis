from itertools import groupby
from unittest.mock import inplace

import pandas as pd
from numpy.ma.extras import unique
from matplotlib import pyplot as plt
import numpy as np
from pandas import read_csv
from pandas.core.interchange.from_dataframe import primitive_column_to_ndarray

df_bookings=pd.read_csv("fact_bookings.csv")
print(df_bookings)
shape=df_bookings.shape
print(shape)
room_bookings=df_bookings.room_category.unique()
print(room_bookings)
booking_platform=df_bookings.booking_platform.unique()
print(booking_platform)
# to count the booking per platform
no_booking_platform=df_bookings.booking_platform.value_counts()
print(no_booking_platform)
no_booking_platform=df_bookings.booking_platform.value_counts().plot(kind='bar')
plt.show()
print(pd.set_option('display.max_columns', None))
print(df_bookings.describe())
print(df_bookings.revenue_generated.min(), df_bookings.revenue_generated.max())
df_date=pd.read_csv("dim_date.csv")
print(df_date)
df_hotels=pd.read_csv("dim_hotels.csv")


print(df_hotels.category.value_counts())
print(df_hotels.head(4))
df_rooms=pd.read_csv("dim_rooms.csv")
print(df_rooms)
print(df_hotels.city.value_counts())
df_aggregate_booking=pd.read_csv("fact_aggregated_bookings.csv")
print(df_aggregate_booking)
print(df_aggregate_booking.property_id.unique())
print(df_aggregate_booking.groupby("property_id")['successful_bookings'].sum())
print(df_aggregate_booking.property_id.value_counts()/df_aggregate_booking.successful_bookings.value_counts())
print(df_aggregate_booking.successful_bookings>df_aggregate_booking.capacity)
print(df_aggregate_booking.capacity.max())
print(df_aggregate_booking[df_aggregate_booking.capacity==df_aggregate_booking.capacity.max()])

# DATA CLEANING
df_bookings=pd.read_csv("fact_bookings.csv")
print(df_bookings)
print(df_bookings.describe())
print(pd.set_option('display.max_columns', None))
print(df_bookings[df_bookings.no_guests<=0])
print(pd.set_option('display.max_columns', None))
df_bookings=df_bookings[df_bookings.no_guests>=0]
print(df_bookings)
print(df_bookings.revenue_generated.min(), df_bookings.revenue_generated.max())
avg=df_bookings.revenue_generated.mean()
std=df_bookings.revenue_generated.std()
print(avg,std)
higher_limit= avg + 3 * std
print(higher_limit)
lower_limit= avg - 3 * std
print(lower_limit)
print(pd.set_option('display.max_columns', None))
print(df_bookings[df_bookings.revenue_generated>higher_limit])
print(pd.set_option('display.max_columns', None))
df_bookings=df_bookings[df_bookings.revenue_generated<higher_limit]
print(df_bookings)
print(df_bookings.shape)
#DATA CLEANING ON REVENUE_REALIZED
print(df_bookings.revenue_realized.describe())
avg=df_bookings.revenue_realized.mean()
std=df_bookings.revenue_realized.std()
print(avg,std)
print(df_bookings[(df_bookings.room_category=='RT4')].revenue_realized.describe())
print(23439+3*9048)
print(df_bookings.isnull().sum())

df_aggregate_bookings=pd.read_csv("fact_aggregated_bookings.csv")
print(pd.set_option('display.max_columns', None))
print(df_aggregate_bookings)
print(df_aggregate_booking[df_aggregate_booking.successful_bookings<=0])
print(df_aggregate_booking[df_aggregate_booking.capacity<=0])
print(df_aggregate_bookings.describe())
av=df_aggregate_booking[df_aggregate_booking.successful_bookings>df_aggregate_bookings.capacity]
print(av)
print(av.shape)
print(df_aggregate_bookings.isnull().sum())
print(df_aggregate_bookings.capacity.isna())
print(df_aggregate_bookings.capacity.median())
print(df_aggregate_bookings.capacity.fillna(df_aggregate_booking.capacity.median(),inplace=True))
#adding columns into a data set
df_aggregate_bookings['occ_pct']=df_aggregate_bookings['successful_bookings']/df_aggregate_bookings['capacity']*100

#df_aggregate_bookings['occ_pct']=df_aggregate_bookings['occ_pct'].apply(lambda x:round(x*100,2))
print(df_aggregate_bookings.head(4))

#INSIGHT GENERATION
#1. WHAT IS THE AVERAGE OCCUPANCY RATE IN EACH OF ROOM CATEGORY
print(df_aggregate_bookings.groupby('room_category')['occ_pct'].mean().round(2))

print(df_rooms)
df=pd.merge(df_aggregate_bookings,df_rooms,left_on="room_category",right_on="room_id")
print(df.head(4))
print(df.groupby('room_class')['occ_pct'].mean().round(2))
print(df.drop('room_id',axis=1,inplace=True))
print(df_hotels)
#2. average occupancy rate per city
df2=pd.merge(df,df_hotels,on='property_id')
print(df2)
print(df2.columns)
gp=df2.groupby('city')['occ_pct'].mean()
print(gp)
#3. when was the occupancy better? weekday or weekend?
print(df_date)
df2=pd.merge(df2, df_date, left_on="check_in_date", right_on="date")
print(df2)
print(df2.groupby('day_type')['occ_pct'].mean().round(2))
# IN MONTH OF JUNE , WHAT IS THE OCCUPANCY FOR DIFFERENT CITIES
print(df2['mmm yy'].unique())
df_june22=df2[df2['mmm yy']=='Jun 22']
print(df_june22.head(3))
print(df_june22.groupby('city')['occ_pct'].mean().round(2).sort_values(ascending=False))
df_aug=read_csv('new_data_august.csv')
latest_df=pd.concat([df2,df_aug],ignore_index=True,axis=0)
print(latest_df.tail(10))
print(latest_df.shape)
# print revenue realized per city
df_bookings_all=pd.merge(df_bookings, df_hotels,on='property_id')
print(df_bookings_all.groupby('city')['revenue_generated'].sum().round(2))
print(df_bookings_all)
# print month by revenue
df_bookings['check_in_date']=df_bookings['check_in_date'].astype('datetime64[ns]')
df_date=pd.to_datetime(df_date['date'], format='mixed')

print(df_date['date'].dtype)
# Convert the column to datetime64[ns]
df_bookings['check_in_date'] = df_bookings['check_in_date'].astype('datetime64[ns]')
#print(df_date.head(3))
df_bookings=pd.to_datetime(df_bookings['check_in_date'],format='%m/%d/%Y')
print(df_bookings['check_in_date'].dtype)
print(df_date.head(4))
# df_bookings_all=pd.merge(df_bookings_all, df_date, left_on="check_in_date", right_on="date")
# print(df_bookings_all.columns)

df_date=pd.read_csv("dim_date.csv")
print(df_date)
