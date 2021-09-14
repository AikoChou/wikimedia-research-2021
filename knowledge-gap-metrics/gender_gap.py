# %%
from wmfdata.spark import get_session
spark = get_session(type='regular')

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
# %%
from common_functions import *

# %%
snapshot = '2021-08-02'
monthly_snapshot = '2021-07'
target_wikis='enwiki frwiki dewiki zhwiki'

# %%
### Methods for extracting gender features ###
def get_wikidata_for_gender_gap_metrics(snapshot):
    """
    query `wmf.wikidata_entity` for wikidata items with property of:
    * P31 (instance of): Q5 (human)
    * P21 (sex or gender)

    returns a dataframe with schema of
        |-- qid: string (nullable = true) - QID for the wikidata item
        |-- is_human: boolean (nullable = true)
        |-- gender: string (nullable = true) - QID for the gender entity e.g. male (Q6581097), female (Q6581072)
    """
    query = """
        SELECT id AS qid,
            MAX(CASE WHEN claim.mainSnak.property = 'P31' THEN True ELSE False END) AS is_human,
            MAX(CASE WHEN claim.mainSnak.property = 'P21' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS gender
        FROM wmf.wikidata_entity
        LATERAL VIEW explode(claims) AS claim
        WHERE typ='item'
            AND snapshot='"""+snapshot+"""'
            AND ((claim.mainSnak.property='P31' AND claim.mainSnak.datavalue.value like '%\"id\":\"Q5\"%')
                OR claim.mainSnak.property='P21')
        GROUP BY qid
        """
    df = spark.sql(query)
    df = df.select('qid', 'is_human', F.json_tuple('gender', 'id').alias('gender'))
    return df

def append_gender_label(input_df, snapshot):
    """
    `df` should contain `gender` column (QIDs for the gender entities)

    returns a dataframe with a `gender_label` containing the english labels for the gender entities.
        e.g. male (Q6581097), female (Q6581072), transgender female (Q1052281)
    """
    input_df.createOrReplaceTempView('temp_df')
    query = """
        SELECT gender, label_val AS gender_label
        FROM temp_df
        LEFT JOIN wmf.wikidata_entity we
        ON gender=we.id
        LATERAL VIEW explode(labels) t AS label_lang, label_val
        WHERE t.label_lang='en'
        AND typ='item'
        AND we.snapshot='"""+snapshot+"""'
    """
    df = spark.sql(query)
    return df

### Methods for selection metrics ###
def number_of_gender_article(input_df):
    """
    `input_df` should contain `wiki_db`, `gender` and `page_first_edit_timestamp` columns.
    
    returns a dataframe with schema of
        |-- wiki_db: string (nullable = true)
        |-- year: string (nullable = true)
        |-- month: string (nullable = true)
        |-- gender: string (nullable = true)
        |-- article_count: long (nullable = false)
        |-- cumsum: long (nullable = true)
    
    * `article_count`: number of artcicle with `gender` created in the period of `year` and `month` in `wiki_db`.
    * `cumsum`: cumulative sum of `article_count`
    """
    df = (input_df
        .where(F.col('page_first_edit_timestamp').isNotNull())
        .withColumn('year', F.substring('page_first_edit_timestamp', 1, 4))
        .withColumn('month', F.substring('page_first_edit_timestamp', 6, 2))
        .groupby('wiki_db','year','month','gender').count()
        .withColumnRenamed('count', 'article_count')
        .withColumn('cumsum', (F.sum('article_count')
                                .over(Window.partitionBy('wiki_db', 'gender')
                                .orderBy('year', 'month')))))
    return df
# %%
df = get_wikidata_for_gender_gap_metrics(snapshot)
# %%
df.show(10)
#+----------+--------+--------+
#|       qid|is_human|  gender|
#+----------+--------+--------+
#|Q100000831|    true|Q6581097|
#|Q100057301|    true|Q6581097|
#|  Q1000800|    true|Q6581097|
#|Q100139984|    true|Q6581072|
#|Q100145109|    true|Q6581097|
#|Q100145789|    true|Q6581097|
#|Q100145932|    true|Q6581097|
#|Q100146384|    true|Q6581097|
#|Q100148950|    true|Q6581097|
#|Q100149111|    true|Q6581097|
#+----------+--------+--------+
# %%
df.count()
# 9312450
# %%
# not human but has gender property -> fictional human, anime character, etc.
df.where(F.col('is_human').isNull()).count()
# 82518
# %%
df.where(F.col('gender').isNull()).count()
# 1949973
# %%
df.select('gender').distinct().count() 
# 41

###########################################################
# %%
gender_items = (df
    .select(F.col('gender'))
    .dropDuplicates()
)
gender_labels = append_gender_label(gender_items, snapshot)
# %%
gender_labels.show()
#+---------+-----------------+
#|   gender|     gender_label|
#+---------+-----------------+
#|Q15145778| "cisgender male"|
#| Q3277905|           "māhū"|
#|  Q179294|         "eunuch"|
#|  Q859614|       "bigender"|
#|Q52261234|    "neutral sex"|
#|   Q48279|   "third gender"|
#| Q3177577|           "muxe"|
#|Q27679684|  "transfeminine"|
#|   Q51415|          "queer"|
#|Q18116794|    "genderfluid"|
#|   Q44148|  "male organism"|
#|Q27679766| "transmasculine"|
#|  Q505371|        "agender"|
#|  Q207959|      "androgyny"|
#| Q4700377|      "akava'ine"|
#|Q17148251|       "travesti"|
#| Q7130936|      "pangender"|
#|  Q303479|  "hermaphrodite"|
#|Q16674976|"hermaphroditism"|
#|Q64017034|      "cogenitor"|
#+---------+-----------------+
# %%
(df
    .join(gender_labels, 'gender', 'left')
    .show(10)
)
#+---------+---------+--------+----------------+
#|   gender|      qid|is_human|    gender_label|
#+---------+---------+--------+----------------+
#|Q15145778|Q89290871|    true|"cisgender male"|
#|Q15145778|Q16221844|    true|"cisgender male"|
#|Q15145778| Q6144533|    true|"cisgender male"|
#|Q15145778| Q5927684|    true|"cisgender male"|
#|Q15145778|Q15111952|    true|"cisgender male"|
#|Q15145778|Q47532275|    true|"cisgender male"|
#|Q15145778|   Q14045|    true|"cisgender male"|
#|Q15145778|Q53866350|    true|"cisgender male"|
#|Q15145778| Q6186917|    true|"cisgender male"|
#| Q3277905| Q3048308|    true|          "māhū"|
#+---------+---------+--------+----------------+
#only showing top 10 rows

###########################################################
# %%
wikidata_items = (df
    .select(F.col('qid'))
)
pages = get_wikipedia_page_from_wikidata(wikidata_items, snapshot, target_wikis)
pages = df.join(pages, 'qid', 'inner')
pages.printSchema()
#root
# |-- qid: string (nullable = true)
# |-- is_human: boolean (nullable = true)
# |-- gender: string (nullable = true)
# |-- wiki_db: string (nullable = true)
# |-- page_id: long (nullable = true)
# |-- page_title: string (nullable = true)
# %%
pages.show(10)
#+----------+--------+--------+-------+--------+--------------------+
#|       qid|is_human|  gender|wiki_db| page_id|          page_title|
#+----------+--------+--------+-------+--------+--------------------+
#|   Q100066|    true|Q6581072| enwiki|25969063|        Holly_Brooks|
#|   Q100066|    true|Q6581072| dewiki| 8253345|        Holly_Brooks|
#|   Q100066|    true|Q6581072| frwiki| 7822835|        Holly_Brooks|
#|Q100137735|    true|Q6581097| enwiki|65511505|       De'Jon_Harris|
#|Q100146397|    true|Q6581097| enwiki|65395540|         Sisir_Ghosh|
#|Q100152085|    true|Q6581097| dewiki|11485675|       Felix_Lampert|
#|Q100166821|    true|Q6581097| enwiki|65522146|      Paulin_Lanteri|
#|   Q100209|    true|Q6581097| enwiki| 1280729|     August_Schrader|
#|   Q100209|    true|Q6581097| enwiki|53809746|     August_Schrader|
#|   Q100209|    true|Q6581097| dewiki| 3160099|August_Schrader_(...|
#+----------+--------+--------+-------+--------+--------------------+
#only showing top 10 rows

###########################################################

page_with_timestamps = append_page_first_edit_timestamp(pages, monthly_snapshot)
page_with_timestamps = pages.join(page_with_timestamps, ['page_id', 'wiki_db'], 'inner')
page_with_timestamps.printSchema()
#root
# |-- page_id: long (nullable = true)
# |-- wiki_db: string (nullable = true)
# |-- qid: string (nullable = true)
# |-- is_human: boolean (nullable = true)
# |-- gender: string (nullable = true)
# |-- page_title: string (nullable = true)
# |-- page_first_edit_timestamp: string (nullable = true)
# %%
page_with_timestamps.show(10)
#+-------+-------+--------+--------+--------+--------------------+-------------------------+
#|page_id|wiki_db|     qid|is_human|  gender|          page_title|page_first_edit_timestamp|
#+-------+-------+--------+--------+--------+--------------------+-------------------------+
#|    527| frwiki|    Q410|    true|Q6581097|          Carl_Sagan|     2002-01-10 09:47:...|
#|    756| frwiki|  Q81716|    true|Q6581097|Charles-Augustin_...|     2002-08-24 10:42:...|
#|   1208| enwiki|   Q7251|    true|Q6581097|         Alan_Turing|     2001-11-12 19:39:...|
#|   1239| enwiki|   Q8589|    true|Q6581097|              Ashoka|     2001-07-30 06:23:...|
#|   1662| frwiki| Q506449|    true|Q6581097|       Jacques_Tardi|     2002-10-20 01:09:...|
#|   1702| enwiki|Q4723684|    true|Q6581097|  Alfred_of_Beverley|     2002-02-25 15:51:...|
#|   2524| frwiki| Q441161|    true|Q6581097|       Prince_Buster|     2002-10-16 14:53:...|
#|   2794| frwiki| Q316013|    true|Q6581097|        Satoru_Iwata|     2002-06-27 09:23:...|
#|   3147| frwiki| Q190572|    true|Q6581097|         Vicente_Fox|     2002-07-08 17:56:...|
#|   3621| dewiki| Q219377|    true|Q6581097|     Nicholas_Sparks|     2002-07-12 09:30:...|
#+-------+-------+--------+--------+--------+--------------------+-------------------------+
#only showing top 10 rows

###########################################################
# %%
metric_df = number_of_gender_article(page_with_timestamps)
metric_df.printSchema()
#root
# |-- wiki_db: string (nullable = true)
# |-- year: string (nullable = true)
# |-- month: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- article_count: long (nullable = false)
# |-- cumsum: long (nullable = true)
# %%
wp_df = number_of_wikipedia_pages(monthly_snapshot, target_wikis)
wp_df.printSchema()
#root
# |-- wiki_db: string (nullable = true)
# |-- year: string (nullable = true)
# |-- month: string (nullable = true)
# |-- article_count: long (nullable = false)
# |-- cumsum: long (nullable = true)
# %%
### compute selection metric for gender gaps (absolute value and relative value)
en_df = (metric_df
    .join((wp_df.withColumnRenamed('article_count', 'wp_count')
                .withColumnRenamed('cumsum', 'wp_cumsum'))
            , ['wiki_db', 'year', 'month'], 'outer')
    .where(F.col('wiki_db') == 'enwiki')
    .withColumn('monthly_pct', F.round(F.col('article_count')/F.col('wp_count')*100, 3))
    .withColumn('accumulated_pct', F.round(F.col('cumsum')/F.col('wp_cumsum')*100, 3))
    .orderBy('year', 'month'))
# %%
gender_items = (en_df
    .select(F.col('gender'))
    .dropDuplicates()
)
gender_labels = append_gender_label(gender_items, snapshot)
en_df = en_df.join(gender_labels, 'gender', 'left')
# %%
### print stats of male article in 2021
(en_df
    .where(F.col('gender_label') == '\"male\"')
    .where(F.col('year') == '2021')
    .orderBy('month')
    .show()
)
#+--------+-------+----+-----+-------------+-------+--------+---------+-----------+---------------+------------+
#|  gender|wiki_db|year|month|article_count| cumsum|wp_count|wp_cumsum|monthly_pct|accumulated_pct|gender_label|
#+--------+-------+----+-----+-------------+-------+--------+---------+-----------+---------------+------------+
#|Q6581097| enwiki|2021|   01|         5219|1459957|   22856|  6217138|     22.834|         23.483|      "male"|
#|Q6581097| enwiki|2021|   02|         5212|1465169|   22881|  6240019|     22.779|          23.48|      "male"|
#|Q6581097| enwiki|2021|   03|         6144|1471313|   24547|  6264566|      25.03|         23.486|      "male"|
#|Q6581097| enwiki|2021|   04|         5110|1476423|   19103|  6283669|      26.75|         23.496|      "male"|
#|Q6581097| enwiki|2021|   05|         5992|1482415|   22995|  6306664|     26.058|         23.506|      "male"|
#|Q6581097| enwiki|2021|   06|         5474|1487889|   20399|  6327063|     26.835|         23.516|      "male"|
#|Q6581097| enwiki|2021|   07|         5999|1493888|   20981|  6348044|     28.593|         23.533|      "male"|
#|Q6581097| enwiki|2021|   08|            8|1493896|      27|  6348071|      29.63|         23.533|      "male"|
#+--------+-------+----+-----+-------------+-------+--------+---------+-----------+---------------+------------+
# %%
### print stats of female article in 2021
(en_df
    .where(F.col('gender_label') == '\"female\"')
    .where(F.col('year') == '2021')
    .orderBy('month')
    .show()
)
#+--------+-------+----+-----+-------------+------+--------+---------+-----------+---------------+------------+
#|  gender|wiki_db|year|month|article_count|cumsum|wp_count|wp_cumsum|monthly_pct|accumulated_pct|gender_label|
#+--------+-------+----+-----+-------------+------+--------+---------+-----------+---------------+------------+
#|Q6581072| enwiki|2021|   01|         2067|339916|   22856|  6217138|      9.044|          5.467|    "female"|
#|Q6581072| enwiki|2021|   02|         2112|342028|   22881|  6240019|       9.23|          5.481|    "female"|
#|Q6581072| enwiki|2021|   03|         2720|344748|   24547|  6264566|     11.081|          5.503|    "female"|
#|Q6581072| enwiki|2021|   04|         2503|347251|   19103|  6283669|     13.103|          5.526|    "female"|
#|Q6581072| enwiki|2021|   05|         2468|349719|   22995|  6306664|     10.733|          5.545|    "female"|
#|Q6581072| enwiki|2021|   06|         2088|351807|   20399|  6327063|     10.236|           5.56|    "female"|
#|Q6581072| enwiki|2021|   07|         2936|354743|   20981|  6348044|     13.994|          5.588|    "female"|
#|Q6581072| enwiki|2021|   08|            8|354751|      27|  6348071|      29.63|          5.588|    "female"|
#+--------+-------+----+-----+-------------+------+--------+---------+-----------+---------------+------------+