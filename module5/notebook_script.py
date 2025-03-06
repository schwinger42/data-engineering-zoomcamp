#!/usr/bin/env python
# -*- coding: utf-8 -*-

# # 電影串流平台資料探索與分析
#
# 這個筆記本演示如何使用我們的 ETL 流程處理後的數據進行探索性數據分析 (EDA)

# ---------------------------
# Cell 1: 導入所需的庫
# ---------------------------
import os
import sys

# 將專案根目錄添加到 Python 路徑
sys.path.append(os.path.abspath('..'))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb

from src.connectors import create_spark_session

# 設定顯示選項
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

# 設定繪圖風格
plt.style.use('fivethirtyeight')
sns.set(rc={'figure.figsize': (12, 8)})

# ---------------------------
# Cell 2: 連接 DuckDB 並加載數據
# ---------------------------
from src.config import DUCKDB_PATH

conn = duckdb.connect(DUCKDB_PATH)

# 列出所有表
print("可用的表:")
tables = conn.execute("SHOW TABLES").fetchdf()
display(tables)

# 加載 Gold 層數據
user_viewing_df = conn.execute("SELECT * FROM gold_user_viewing_summary").fetchdf()
movie_popularity_df = conn.execute("SELECT * FROM gold_movie_popularity").fetchdf()
subscription_df = conn.execute("SELECT * FROM gold_subscription_analysis").fetchdf()
genre_preferences_df = conn.execute("SELECT * FROM gold_genre_preferences").fetchdf()

# ---------------------------
# Cell 3: 用戶觀看行為分析
# ---------------------------
# 每日觀看統計
daily_views = user_viewing_df.groupby('interaction_date')['movies_watched'].sum().reset_index()
daily_minutes = user_viewing_df.groupby('interaction_date')['total_watch_minutes'].sum().reset_index()

fig, axs = plt.subplots(2, 1, figsize=(14, 10))

axs[0].plot(daily_views['interaction_date'], daily_views['movies_watched'], marker='o')
axs[0].set_title('每日觀看電影數量')
axs[0].set_ylabel('觀看次數')
axs[0].grid(True)

axs[1].plot(daily_minutes['interaction_date'], daily_minutes['total_watch_minutes'], marker='o', color='orange')
axs[1].set_title('每日總觀看時間（分鐘）')
axs[1].set_ylabel('總分鐘數')
axs[1].set_xlabel('日期')
axs[1].grid(True)

plt.tight_layout()
plt.show()

# ---------------------------
# Cell 4: 電影人氣分析
# ---------------------------
# 最受歡迎的前 10 部電影
top_movies = movie_popularity_df.sort_values('total_views', ascending=False).head(10)

plt.figure(figsize=(12, 8))
sns.barplot(x='total_views', y='title', data=top_movies)
plt.title('觀看次數最多的前 10 部電影')
plt.xlabel('觀看次數')
plt.ylabel('電影標題')
plt.tight_layout()
plt.show()

# 按完成率排序的前 10 部電影
top_completion = movie_popularity_df.sort_values('completion_rate', ascending=False).head(10)

plt.figure(figsize=(12, 8))
sns.barplot(x='completion_rate', y='title', data=top_completion)
plt.title('完成率最高的前 10 部電影')
plt.xlabel('完成率')
plt.ylabel('電影標題')
plt.tight_layout()
plt.show()

# ---------------------------
# Cell 5: 訂閱分析
# ---------------------------
# 按訂閱狀態分組的用戶觀看行為
subscription_viewing = subscription_df.groupby('subscription_or_not')['total_movies_watched'].agg(['mean', 'median', 'std']).reset_index()
print(subscription_viewing)

# 訂閱用戶 vs 非訂閱用戶的觀看量分佈
plt.figure(figsize=(12, 8))
sns.boxplot(x='subscription_or_not', y='total_movies_watched', data=subscription_df)
plt.title('訂閱 vs 非訂閱用戶的觀看量分佈')
plt.xlabel('訂閱狀態')
plt.ylabel('觀看的電影總數')
plt.show()

# ---------------------------
# Cell 6: 類型偏好分析
# ---------------------------
# 最受歡迎的電影類型
genre_popularity = genre_preferences_df.groupby('genre')['movies_count'].sum().reset_index().sort_values('movies_count', ascending=False)

plt.figure(figsize=(14, 8))
sns.barplot(x='movies_count', y='genre', data=genre_popularity.head(15))
plt.title('最受歡迎的電影類型（按觀看次數）')
plt.xlabel('觀看次數')
plt.ylabel('類型')
plt.tight_layout()
plt.show()

# 最高完成率的電影類型
genre_completion = genre_preferences_df.groupby('genre').agg(
    total_movies=('movies_count', 'sum'),
    total_completions=('completions', 'sum')
).reset_index()

genre_completion['completion_rate'] = genre_completion['total_completions'] / genre_completion['total_movies']
genre_completion = genre_completion.sort_values('completion_rate', ascending=False)

plt.figure(figsize=(14, 8))
sns.barplot(x='completion_rate', y='genre', data=genre_completion.head(15))
plt.title('完成率最高的電影類型')
plt.xlabel('完成率')
plt.ylabel('類型')
plt.tight_layout()
plt.show()

# ---------------------------
# Cell 7: 使用 Spark 進行進階分析
# ---------------------------
# 創建 Spark Session
spark = create_spark_session("EDA_Analysis")

# 從 DuckDB 讀取數據為 Spark DataFrame
from src.connectors import read_from_duckdb
silver_interactions = read_from_duckdb(spark, "silver_user_interactions")
silver_users = read_from_duckdb(spark, "silver_users")
silver_movies = read_from_duckdb(spark, "silver_movies")

# 註冊臨時表
silver_interactions.createOrReplaceTempView("interactions")
silver_users.createOrReplaceTempView("users")
silver_movies.createOrReplaceTempView("movies")

# 用戶行為分析 - 找出高活躍用戶和他們的觀看模式
active_users_df = spark.sql("""
    SELECT u.user_id, 
           u.subscription_or_not,
           u.register_date,
           COUNT(DISTINCT i.movie_id) as unique_movies_watched,
           SUM(i.watch_duration_minutes) as total_minutes,
           AVG(i.watch_duration_minutes) as avg_minutes_per_movie,
           COUNT(DISTINCT i.interaction_date) as active_days
    FROM users u
    JOIN interactions i ON u.user_id = i.user_id
    GROUP BY u.user_id, u.subscription_or_not, u.register_date
    ORDER BY unique_movies_watched DESC
    LIMIT 100
""")

# 轉換為 Pandas DataFrame 以便繪圖
active_users_pd = active_users_df.toPandas()

# 分析高活躍用戶的訂閱狀態
plt.figure(figsize=(10, 6))
sns.countplot(x='subscription_or_not', data=active_users_pd)
plt.title('高活躍用戶的訂閱狀態分佈')
plt.xlabel('訂閱狀態')
plt.ylabel('用戶數量')
plt.show()

# 分析高活躍用戶的觀看時間與觀看電影數的關係
plt.figure(figsize=(12, 8))
sns.scatterplot(
    x='unique_movies_watched',
    y='total_minutes',
    hue='subscription_or_not',
    size='active_days',
    sizes=(20, 200),
    data=active_users_pd
)
plt.title('高活躍用戶的觀看模式')
plt.xlabel('觀看的獨特電影數')
plt.ylabel('總觀看時間（分鐘）')
plt.legend(title='訂閱狀態')
plt.show()

# 關閉 Spark Session
spark.stop()
