# %%
from datetime import datetime
from wmfdata.spark import get_session
spark = get_session(type='regular')

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window

# %%
df = spark.read.parquet('gender_features.parquet')
df.printSchema()
#root
# |-- page_id: long (nullable = true)
# |-- page_title: string (nullable = true)
# |-- wiki_db: string (nullable = true)
# |-- qid: string (nullable = true)
# |-- is_human: boolean (nullable = true)
# |-- gender_label: string (nullable = true)
# |-- date_created: string (nullable = true)

# %%
df.where(F.col('date_created').isNull()).count()
# 178353

# %%
def number_of_gender_article(input_df):
    """
    `input_df` should contain `date_created`, `wiki_db`, and `gender_label` columns.
    returns a dataframe with a `article_count` column 
        means the number of artcicle with `gender_label` created in the period of `year` and `month` in `wiki_db`.
    """
    df = (input_df
        .where(F.col('date_created').isNotNull())
        .withColumn('year', F.substring('date_created', 1, 4))
        .withColumn('month', F.substring('date_created', 6, 2))
        .groupby('wiki_db','year','month','gender_label').count()
        .withColumnRenamed('count', 'article_count')
        .withColumn('cumsum', (F.sum('article_count')
                                .over(Window.partitionBy('wiki_db', 'gender_label')
                                .orderBy('year', 'month')))))
    return df

# %%
metric_df = number_of_gender_article(df)
metric_df.printSchema()
#root
# |-- wiki_db: string (nullable = true)
# |-- year: string (nullable = true)
# |-- month: string (nullable = true)
# |-- gender_label: string (nullable = true)
# |-- article_count: long (nullable = false)
# |-- cumsum: long (nullable = true)


# %%
(metric_df
    .where(F.col('wiki_db') == 'enwiki')
    .where(F.col('gender_label') == '\"male\"')
    .show(10)
)
#+-------+----+-----+------------+-------------+------+
#|wiki_db|year|month|gender_label|article_count|cumsum|
#+-------+----+-----+------------+-------------+------+
#| enwiki|2001|   01|      "male"|            5|     5|
#| enwiki|2001|   02|      "male"|           12|    17|
#| enwiki|2001|   03|      "male"|           31|    48|
#| enwiki|2001|   04|      "male"|           32|    80|
#| enwiki|2001|   05|      "male"|          100|   180|
#| enwiki|2001|   06|      "male"|           89|   269|
#| enwiki|2001|   07|      "male"|          113|   382|
#| enwiki|2001|   08|      "male"|          359|   741|
#| enwiki|2001|   09|      "male"|          411|  1152|
#| enwiki|2001|   10|      "male"|          502|  1654|
#+-------+----+-----+------------+-------------+------+

# %%
(metric_df
    .where(F.col('wiki_db') == 'enwiki')
    .where(F.col('gender_label') == '\"female\"')
    .show(10)
)
#+-------+----+-----+------------+-------------+------+
#|wiki_db|year|month|gender_label|article_count|cumsum|
#+-------+----+-----+------------+-------------+------+
#| enwiki|2001|   01|    "female"|            4|     4|
#| enwiki|2001|   02|    "female"|            6|    10|
#| enwiki|2001|   03|    "female"|            7|    17|
#| enwiki|2001|   04|    "female"|            1|    18|
#| enwiki|2001|   05|    "female"|            8|    26|
#| enwiki|2001|   06|    "female"|           11|    37|
#| enwiki|2001|   07|    "female"|           14|    51|
#| enwiki|2001|   08|    "female"|           30|    81|
#| enwiki|2001|   09|    "female"|           43|   124|
#| enwiki|2001|   10|    "female"|           71|   195|
#+-------+----+-----+------------+-------------+------+

# %%
target_wikis='enwiki frwiki dewiki eswiki itwiki zhwiki'
monthly_snapshot = '2021-07'

def number_of_wikipedia_pages(target_wikis, monthly_snapshot):
    """
    returns a dataframe with a `article_count` column 
        means the number of artcicle created in the period of `year` and `month` in `wiki_db`.
    """
    query = """
        SELECT p.wiki_db, p.page_id, 
            from_unixtime(MIN(unix_timestamp(mp.page_first_edit_timestamp, 'yyyy-MM-dd HH:mm:ss'))) AS date_created 
        FROM wmf_raw.mediawiki_page p
        LEFT JOIN wmf.mediawiki_page_history mp
        ON p.page_id=mp.page_id
        WHERE p.snapshot='"""+monthly_snapshot+"""'
            AND p.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\")
            AND p.page_namespace=0
            AND p.page_is_redirect=0
            AND mp.snapshot='"""+monthly_snapshot+"""'
        GROUP BY p.wiki_db, p.page_id
        """
    df = spark.sql(query)
    df = (df
        .withColumn('year', F.substring('date_created', 1, 4))
        .withColumn('month', F.substring('date_created', 6, 2))
        .groupby('wiki_db','year','month').count()
        .withColumnRenamed('count', 'article_count')
        .withColumn('cumsum', (F.sum('article_count')
                                .over(Window.partitionBy('wiki_db')
                                .orderBy('year', 'month')))))
    return df
# %%
wp_df = number_of_wikipedia_pages(target_wikis, monthly_snapshot)
wp_df.printSchema()
#root
# |-- wiki_db: string (nullable = true)
# |-- year: string (nullable = true)
# |-- month: string (nullable = true)
# |-- article_count: long (nullable = false)
# |-- cumsum: long (nullable = true)

# %%
wp_df.where(F.col('wiki_db') == 'enwiki').show(10)
#+-------+----+-----+-------------+------+
#|wiki_db|year|month|article_count|cumsum|
#+-------+----+-----+-------------+------+
#| enwiki|1999|   12|            2|     2|
#| enwiki|2001|   01|         6010|  6012|
#| enwiki|2001|   02|          427|  6439|
#| enwiki|2001|   03|         1001|  7440|
#| enwiki|2001|   04|         1011|  8451|
#| enwiki|2001|   05|         2410| 10861|
#| enwiki|2001|   06|          792| 11653|
#| enwiki|2001|   07|         1472| 13125|
#| enwiki|2001|   08|         2151| 15276|
#| enwiki|2001|   09|         3454| 18730|
#+-------+----+-----+-------------+------+
# %%
wp_df.where(F.col('wiki_db') == 'dewiki').show(10)
#+-------+----+-----+-------------+------+
#|wiki_db|year|month|article_count|cumsum|
#+-------+----+-----+-------------+------+
#| dewiki|1999|   11|            1|     1|
#| dewiki|2001|   01|         4845|  4846|
#| dewiki|2001|   02|          671|  5517|
#| dewiki|2001|   03|         1054|  6571|
#| dewiki|2001|   04|          920|  7491|
#| dewiki|2001|   05|         1802|  9293|
#| dewiki|2001|   06|          799| 10092|
#| dewiki|2001|   07|         1292| 11384|
#| dewiki|2001|   08|         1826| 13210|
#| dewiki|2001|   09|         3019| 16229|
#+-------+----+-----+-------------+------+

# %%
(metric_df
    .join((wp_df.withColumnRenamed('article_count', 'wp_count')
                .withColumnRenamed('cumsum', 'wp_cumsum'))
            , ['wiki_db', 'year', 'month'])
    .where(F.col('wiki_db') == 'enwiki')
    .where(F.col('gender_label') == '\"male\"')
    .withColumn('monthly_pct', F.round(F.col('article_count')/F.col('wp_count')*100, 3))
    .withColumn('accumulated_pct', F.round(F.col('cumsum')/F.col('wp_cumsum')*100, 3))
    .orderBy('year', 'month')
    .show(10))

#+-------+----+-----+------------+-------------+------+--------+---------+-----------+---------------+
#|wiki_db|year|month|gender_label|article_count|cumsum|wp_count|wp_cumsum|monthly_pct|accumulated_pct|
#+-------+----+-----+------------+-------------+------+--------+---------+-----------+---------------+
#| enwiki|2001|   01|      "male"|            5|     5|    6010|     6012|      0.083|          0.083|
#| enwiki|2001|   02|      "male"|           12|    17|     427|     6439|       2.81|          0.264|
#| enwiki|2001|   03|      "male"|           31|    48|    1001|     7440|      3.097|          0.645|
#| enwiki|2001|   04|      "male"|           32|    80|    1011|     8451|      3.165|          0.947|
#| enwiki|2001|   05|      "male"|          100|   180|    2410|    10861|      4.149|          1.657|
#| enwiki|2001|   06|      "male"|           89|   269|     792|    11653|     11.237|          2.308|
#| enwiki|2001|   07|      "male"|          113|   382|    1472|    13125|      7.677|           2.91|
#| enwiki|2001|   08|      "male"|          359|   741|    2151|    15276|      16.69|          4.851|
#| enwiki|2001|   09|      "male"|          411|  1152|    3454|    18730|     11.899|          6.151|
#| enwiki|2001|   10|      "male"|          502|  1654|    4715|    23445|     10.647|          7.055|
#+-------+----+-----+------------+-------------+------+--------+---------+-----------+---------------+