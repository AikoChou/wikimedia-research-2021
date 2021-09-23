# %%
from wmfdata.spark import get_session
spark = get_session(type='regular')

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window

snapshot = '2021-08-02'
monthly_snapshot = '2021-07'

# %%
### Methods for wikidata dataframe ###
def create_wikidata_dataframe_for_gender_gap(snapshot):
    """
    1. query `wmf.wikidata_entity` for wikidata items with property of:
        * P31 (instance of): Q5 (human)
        * P21 (sex or gender)
    2. add gender_label based on the gender QID
    3. add zero_ill to indicate if the item has links to Wikipedia

    returns a dataframe with schema of
        |-- qid: string (nullable = true) - QID for the wikidata item
        |-- is_human: boolean (nullable = true)
        |-- gender: string (nullable = true) - QID for the gender entity e.g. male (Q6581097), female (Q6581072)
        |-- gender_label: string (nullable = true) 
        |-- zero_ill: boolean (nullable = true)
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
    df = add_gender_label(df, snapshot)
    df = add_zero_ill(df)
    return df

def add_gender_label(input_df, snapshot):
    """
    `df` should contain `gender` column (QIDs for the gender entities)

    returns a dataframe appended a `gender_label` 
        containing the english labels for the gender entities.
        e.g. male (Q6581097), female (Q6581072), transgender female (Q1052281)
    """
    if 'gender' not in input_df.columns: print('input_df should contain a gender column!'); return
    input_df.createOrReplaceTempView('input_df')
    query = """
        SELECT input_df.*, label_val AS gender_label
        FROM input_df
        LEFT JOIN wmf.wikidata_entity we
        ON input_df.gender=we.id
        LATERAL VIEW explode(labels) t AS label_lang, label_val
        WHERE t.label_lang='en'
        AND typ='item'
        AND we.snapshot='"""+snapshot+"""'
    """
    df = spark.sql(query)
    return df

def add_zero_ill(input_df):
    """
    `input_df` should contain `qid` column (QIDs for wikidata items)

    returns a dataframe appended a `zero_ill` columns,
        if true, the item does not have any links to Wikipedia,
        if false, the item has links to Wikipedia.
    """
    if 'qid' not in input_df.columns: print('input_df should contain a qid column!'); return
    input_df.createOrReplaceTempView('input_df')
    query = """
        SELECT qid
        FROM input_df
        LEFT ANTI JOIN wmf.wikidata_item_page_link wipl
        ON input_df.qid=wipl.item_id
    """
    zero_ill = spark.sql(query).alias('zero_ill')
    wikidata = input_df.alias('wikidata')
    df = (wikidata
        .join(zero_ill, F.col('wikidata.qid') == F.col('zero_ill.qid'), 'left')
        .withColumn('zero_ill', F.when(F.col('zero_ill.qid').isNull(), False).otherwise(True))
        .select('wikidata.*', 'zero_ill'))
    return df

# %%
#wikidata_df = spark.read.parquet('gender_wikidata_df.parquet')
# %%
wikidata_df = create_wikidata_dataframe_for_gender_gap(snapshot)
wikidata_df.printSchema()
#root
# |-- qid: string (nullable = true)
# |-- is_human: boolean (nullable = true)
# |-- gender: string (nullable = true)
# |-- gender_label: string (nullable = true)
# |-- zero_ill: boolean (nullable = false)

# %%
wikidata_df.show(10)
#+----------+--------+--------+------------+--------+
#|       qid|is_human|  gender|gender_label|zero_ill|
#+----------+--------+--------+------------+--------+
#|   Q100066|    true|Q6581072|    "female"|   false|
#|Q100067390|    true|Q6581072|    "female"|    true|
#|Q100088925|    true|Q6581097|      "male"|    true|
#|Q100137735|    true|Q6581097|      "male"|   false|
#|Q100139645|    true|Q6581072|    "female"|    true|
#|Q100145504|    true|Q6581072|    "female"|    true|
#|Q100146255|    true|Q6581097|      "male"|    true|
#|Q100146397|    true|Q6581097|      "male"|   false|
#|Q100152085|    true|Q6581097|      "male"|   false|
#|Q100153066|    true|Q6581072|    "female"|    true|
#+----------+--------+--------+------------+--------+
#only showing top 10 rows

wikidata_df.count()
# 7362476

wikidata_df.where(F.col('is_human') == False).count()
# 82322

wikidata_df.where(F.col('gender').isNull()).count()
# 0

wikidata_df.where(F.col('gender_label').isNull()).count()
# 0

wikidata_df.select('gender').distinct().count() 
# 39

wikidata_df.where(F.col('zero_ill') == True).count()
# 3344332

#wikidata_df.write.parquet('gender_wikidata_df.parquet')
###########################################################
# %%
### Methods for page dataframe ###
def create_page_dataframe_from_wikidata(input_df, snapshot, monthly_snapshot, target_wikis=None):
    if 'qid' not in input_df.columns: print('input_df should contain a qid column!'); return
    input_df.createOrReplaceTempView('input_df')
    query = """
        SELECT input_df.*, wipl.wiki_db, wipl.page_id, wipl.page_title
        FROM input_df
        LEFT JOIN wmf.wikidata_item_page_link wipl
        ON input_df.qid=wipl.item_id
        INNER JOIN wmf_raw.mediawiki_page mp
        ON wipl.page_id=mp.page_id AND wipl.wiki_db=mp.wiki_db
        WHERE wipl.snapshot='"""+snapshot+"""'
            AND mp.snapshot='"""+monthly_snapshot+"""'
            AND mp.page_namespace=0
            AND mp.page_is_redirect=0
    """
    if target_wikis:
        query += """AND wipl.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\") """
        query += """AND mp.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\") """
    df = spark.sql(query)
    # remove duplicate pages
    df1 = df.select('wiki_db', 'page_id', 'page_title').groupby('wiki_db', 'page_title').count().where(F.col('count') > 1)
    df2 = df.select('wiki_db', 'page_id', 'page_title').groupby('wiki_db', 'page_id').count().where(F.col('count') > 1)
    df = df.join(df1, [df.wiki_db == df1.wiki_db, df.page_title == df1.page_title], 'left_anti')
    df = df.join(df2, [df.wiki_db == df2.wiki_db, df.page_id == df2.page_id], 'left_anti')
    # add datetime of page creation
    df = add_page_first_edit_timestamp(df, monthly_snapshot)
    return df

def add_page_first_edit_timestamp(input_df, monthly_snapshot):
    """
    `df` should contain `page_id`, `wiki_db` columns

    returns a dataframe with `page_first_edit_timestamp` containing the datetime of page creation
        in 'yyyy-MM-dd HH:mm:ss' string format.
    """
    if 'page_id' not in input_df.columns: print('page_id column is missing!'); return
    if 'wiki_db' not in input_df.columns: print('wiki_db column is missing!'); return
    input_df.createOrReplaceTempView('input_df')
    # use MAX aggregation because of the case of
    # |20156072| enwiki|                     null|
    # |20156072| enwiki|     2008-11-10 22:23:...|
    columns = input_df.columns
    columns.remove('page_id')
    columns.remove('wiki_db')
    query = """    
        SELECT input_df.page_id, input_df.wiki_db,
            """+' '.join(['FIRST(input_df.{}) AS {},'.format(c, c) for c in  columns])+"""
            MAX(mp.page_first_edit_timestamp) AS page_first_edit_timestamp
        FROM input_df
        LEFT JOIN wmf.mediawiki_page_history mp
        ON input_df.page_id=mp.page_id 
            AND input_df.wiki_db=mp.wiki_db
        WHERE mp.snapshot='"""+monthly_snapshot+"""'
        GROUP BY input_df.page_id, input_df.wiki_db
        """
    # there may have duplicate timestamps due to rows with different action 
    # (move, create, rename, etc) in the page history
    df = spark.sql(query).dropDuplicates()
    return df
# %%
target_wikis='enwiki frwiki dewiki zhwiki'
page_df = create_page_dataframe_from_wikidata(wikidata_df, snapshot, monthly_snapshot, target_wikis)
page_df.count()
# 3520071

# %%
# all languages and projects
page_df = create_page_dataframe_from_wikidata(wikidata_df, snapshot, monthly_snapshot)
page_df.printSchema()
#root
# |-- page_id: long (nullable = true)
# |-- wiki_db: string (nullable = true)
# |-- qid: string (nullable = true)
# |-- is_human: boolean (nullable = true)
# |-- gender: string (nullable = true)
# |-- gender_label: string (nullable = true)
# |-- zero_ill: boolean (nullable = true)
# |-- page_title: string (nullable = true)
# |-- page_first_edit_timestamp: string (nullable = true)

page_df.count()
# 11466677

page_df.select('wiki_db').distinct().count() 
# 503

page_df.where(F.col('page_first_edit_timestamp').isNull()).count()
# 132

page_df.where(F.col('is_human') == True).groupby('wiki_db').count().orderBy(F.col('count').desc()).show(10)
#+-------+-------+
#|wiki_db|  count|
#+-------+-------+
#| enwiki|1816561|
#| dewiki| 824897|
#|arzwiki| 727334|
#| frwiki| 634726|
#| arwiki| 492852|
#| ruwiki| 479249|
#| eswiki| 430274|
#| itwiki| 428979|
#| plwiki| 380960|
#| jawiki| 361412|
#+-------+-------+
#only showing top 10 rows

###########################################################

##### Selection metric for gender gaps #####
# 1. gender items / people items -> `number_of_gender_item`
# 2. gender articles / gender items -> `number_of_gender_article_overall`
# 3. gender articles / total number of article -> `number_of_gender_article_by_month`
# 4. gender items have zero_ill / gender items -> `number_of_gender_zero_ill`

###########################################################
# %%
def number_of_gender_item(wikidata_df):
    # gender items / people items (people = instance of human)
    people_count = wikidata_df.where(F.col('is_human') == True).count()
    df = (wikidata_df
        .where(F.col('is_human') == True)
        .groupby('gender', 'gender_label').count()
        .withColumn('people_count', F.lit(people_count))
        .withColumn('gender_ratio', F.col('count')/F.col('people_count')*100)
        .orderBy(F.col('count').desc())
        .withColumnRenamed('count', 'gender_count')
        .select('gender', 'gender_label', 'gender_count', 'people_count', 'gender_ratio')
    )
    return df
# %%
#gender_metric_1 = spark.read.parquet('gender_metric_1.parquet')
# %%
gender_metric_1 = number_of_gender_item(wikidata_df)
#gender_metric_1.write.parquet('gender_metric_1.parquet')
# %%
gender_metric_1.orderBy(F.col('gender_count').desc()).show(10)
#+---------+--------------------+------------+------------+--------------------+
#|   gender|        gender_label|gender_count|people_count|        gender_ratio|
#+---------+--------------------+------------+------------+--------------------+
#| Q6581097|              "male"|     5550576|     7280154|   76.24256300072774|
#| Q6581072|            "female"|     1727639|     7280154|   23.73080294730029|
#| Q1052281|"transgender female"|         980|     7280154|0.013461253704248563|
#|   Q48270|        "non-binary"|         422|     7280154|0.005796580676727443|
#| Q2449503|  "transgender male"|         251|     7280154|0.003447729265067...|
#|   Q44148|     "male organism"|          86|     7280154|0.001181293692413...|
#| Q1097630|          "intersex"|          39|     7280154|5.357029535364224E-4|
#|Q18116794|       "genderfluid"|          26|     7280154|3.571353023576149E-4|
#|Q12964198|       "genderqueer"|          22|     7280154|3.021914096872126...|
#|  Q505371|           "agender"|          13|     7280154|1.785676511788074...|
#+---------+--------------------+------------+------------+--------------------+
#only showing top 10 rows
###########################################################
# %%
def number_of_gender_article_overall(page_df, wikidata_df): 
    # gender articles / gender items   
    df1 = (page_df
        .where(F.col('is_human') == True)
        .groupby('wiki_db', 'gender', 'gender_label').count()
        .withColumnRenamed('count', 'article_count')
    )
    df2 = (wikidata_df
        .where(F.col('is_human') == True)
        .groupby('gender', 'gender_label').count()
        .withColumnRenamed('count', 'gender_count')
    )
    df1 = df1.alias('df1')
    df = (df1
        .join(df2, 'gender')
        .withColumn('article_ratio', F.col('article_count')/F.col('gender_count')*100)
        .orderBy(F.col('article_count').desc())
        .select('wiki_db', 'df1.gender', 'df1.gender_label', 'article_count', 'gender_count', 'article_ratio')
    )
    return df
# %%
#gender_metric_2 = spark.read.parquet('gender_metric_2.parquet')
# %%
gender_metric_2 = number_of_gender_article_overall(page_df, wikidata_df)
#gender_metric_2.write.parquet('gender_metric_2.parquet')
# %%
gender_metric_2.orderBy(F.col('article_count').desc()).show(10)
#+-------+--------+------------+-------------+------------+------------------+
#|wiki_db|  gender|gender_label|article_count|gender_count|        article(%)|
#+-------+--------+------------+-------------+------------+------------------+
#| enwiki|Q6581097|      "male"|      1470365|     5550576|26.490313798063482|
#| dewiki|Q6581097|      "male"|       687859|     5550576|12.392569708080748|
#|arzwiki|Q6581097|      "male"|       614994|     5550576|11.079823067011423|
#| frwiki|Q6581097|      "male"|       514409|     5550576| 9.267668796896034|
#| arwiki|Q6581097|      "male"|       412657|     5550576| 7.434489681791584|
#| ruwiki|Q6581097|      "male"|       404582|     5550576| 7.289009284802154|
#| itwiki|Q6581097|      "male"|       360164|     5550576|6.4887680125450045|
#| enwiki|Q6581072|    "female"|       345055|     1727639|19.972633171629024|
#| eswiki|Q6581097|      "male"|       334758|     5550576|  6.03104975051238|
#| plwiki|Q6581097|      "male"|       317423|     5550576| 5.718739820876248|
#+-------+--------+------------+-------------+------------+------------------+
#only showing top 10 rows
###########################################################
# %%
def number_of_gender_article_by_month(page_df, monthly_snapshot):
    # gender articles / total number of article 
    wp_df = number_of_wikipedia_pages(monthly_snapshot)
    df = (page_df
        .where(F.col('is_human') == True)
        .where(F.col('page_first_edit_timestamp').isNotNull())
        .withColumn('year', F.substring('page_first_edit_timestamp', 1, 4))
        .withColumn('month', F.substring('page_first_edit_timestamp', 6, 2))
        .groupby('wiki_db','year','month','gender','gender_label').count()
        .withColumnRenamed('count', 'article_count')
        .withColumn('cumsum', (F.sum('article_count')
                                .over(Window.partitionBy('wiki_db', 'gender', 'gender_label')
                                .orderBy('year', 'month'))))
        .join(wp_df, ['wiki_db', 'year', 'month'], 'outer')
        .withColumn('monthly_article_ratio', F.col('article_count')/F.col('wp_count')*100)
        .withColumn('article_ratio', F.col('cumsum')/F.col('wp_cumsum')*100)
        .select('wiki_db','year','month','gender_label','article_count','monthly_article_ratio','cumsum','article_ratio')
    )
    return df

def number_of_wikipedia_pages(monthly_snapshot):
    query = """
        SELECT p.wiki_db, p.page_id, mp.page_first_edit_timestamp 
        FROM wmf_raw.mediawiki_page p
        LEFT JOIN wmf.mediawiki_page_history mp
        ON p.page_id=mp.page_id
            AND P.wiki_db=mp.wiki_db
        WHERE p.snapshot='"""+monthly_snapshot+"""'
            AND p.page_namespace=0
            AND p.page_is_redirect=0
            AND mp.snapshot='"""+monthly_snapshot+"""'
        """
    df = spark.sql(query).dropDuplicates()
    df = (df
        .where(F.col('page_first_edit_timestamp').isNotNull())
        .withColumn('year', F.substring('page_first_edit_timestamp', 1, 4))
        .withColumn('month', F.substring('page_first_edit_timestamp', 6, 2))
        .groupby('wiki_db','year','month').count()
        .withColumnRenamed('count', 'wp_count')
        .withColumn('wp_cumsum', (F.sum('wp_count')
                                .over(Window.partitionBy('wiki_db')
                                .orderBy('year', 'month')))))
    return df
# %%
#gender_metric_3 = spark.read.parquet('gender_metric_3.parquet')
# %%
gender_metric_3 = number_of_gender_article_by_month(page_df, monthly_snapshot)
#gender_metric_3.write.parquet('gender_metric_3.parquet')
gender_metric_3.printSchema()
#root
# |-- wiki_db: string (nullable = true)
# |-- year: string (nullable = true)
# |-- month: string (nullable = true)
# |-- gender_label: string (nullable = true)
# |-- article_count: long (nullable = true)
# |-- monthly_article_ratio: double (nullable = true)
# |-- cumsum: long (nullable = true)
# |-- article_ratio: double (nullable = true)
# %%
gender_metric_3.where(F.col('wiki_db') == 'enwiki').where(F.col('year') == '2014').where(F.col('gender_label') == '\"female\"').orderBy('month').show(10)
#+-------+----+-----+------------+-------------+---------------------+------+------------------+
#|wiki_db|year|month|gender_label|article_count|monthly_article_ratio|cumsum|     article_ratio|
#+-------+----+-----+------------+-------------+---------------------+------+------------------+
#| enwiki|2014|   01|    "female"|         1379|    5.053873781426373|170703| 3.926280152991131|
#| enwiki|2014|   02|    "female"|         1759|   6.7763309962246705|172462|3.9431954145508765|
#| enwiki|2014|   03|    "female"|         1503|    6.093160903231038|173965|3.9552529961385328|
#| enwiki|2014|   04|    "female"|         1399|    6.009966491966664|175364|3.9660702468740996|
#| enwiki|2014|   05|    "female"|         1298|    5.418266822507931|176662|3.9738957639101646|
#| enwiki|2014|   06|    "female"|         1033|    4.439001332130119|177695| 3.976317754545762|
#| enwiki|2014|   07|    "female"|         1325|    4.760536054324003|179020| 3.981171828194489|
#| enwiki|2014|   08|    "female"|         1372|    5.476390053087455|180392| 3.989456222290682|
#| enwiki|2014|   09|    "female"|         1260|     5.46566607382987|181652|  3.99694418113824|
#| enwiki|2014|   10|    "female"|         1690|    6.402485225034097|183342| 4.010834867587453|
#+-------+----+-----+------------+-------------+---------------------+------+------------------+
#only showing top 10 rows

# %%
gender_metric_3.where(F.col('wiki_db') == 'enwiki').where(F.col('year') == '2021').where(F.col('gender_label') == '\"female\"').orderBy('month').show(10)
#+-------+----+-----+------------+-------------+------------------+------+------------------+
#|wiki_db|year|month|gender_label|article_count|monthly article(%)|cumsum|        article(%)|
#+-------+----+-----+------------+-------------+------------------+------+------------------+
#| enwiki|2021|   01|    "female"|         2017|  8.82481624081204|330528| 5.316401212261977|
#| enwiki|2021|   02|    "female"|         2067| 9.033696079716796|332595| 5.330031847659438|
#| enwiki|2021|   03|    "female"|         2675|10.897462011651118|335270| 5.351847198991917|
#| enwiki|2021|   04|    "female"|         2410|12.615819504789824|337680| 5.373930421860222|
#| enwiki|2021|   05|    "female"|         2410|10.480539247662534|340090|5.3925498488582875|
#| enwiki|2021|   06|    "female"|         2060|10.098534241874601|342150| 5.407722350796886|
#| enwiki|2021|   07|    "female"|         2880|13.726705114150898|345030| 5.435217525272352|
#| enwiki|2021|   08|    "female"|            8|29.629629629629626|345038| 5.435320430411065|
#+-------+----+-----+------------+-------------+------------------+------+------------------+
###########################################################
# %%
def number_of_gender_zero_ill(wikidata_df): 
    # gender items without interlanguage link / gender items
    df1 = (wikidata_df
        .where(F.col('is_human') == True)
        .where(F.col('zero_ill') == True)
        .groupby('gender', 'gender_label').count()   
        .withColumnRenamed('count', 'zero_ill_count') 
    )
    df2 = (wikidata_df
        .where(F.col('is_human') == True)
        .groupby('gender', 'gender_label').count()
        .withColumnRenamed('count', 'gender_count')
    )
    df1 = df1.alias('df1')
    df = (df1
        .join(df2, 'gender')
        .withColumn('zero_ill_ratio', F.col('zero_ill_count')/F.col('gender_count')*100)
        .orderBy(F.col('zero_ill_count').desc())
        .select('df1.gender', 'df1.gender_label', 'zero_ill_count', 'gender_count', 'zero_ill_ratio')
    )
    return df
# %%
#gender_metric_4 = spark.read.parquet('gender_metric_4.parquet')
# %%
gender_metric_4 = number_of_gender_zero_ill(wikidata_df)
#gender_metric_4.write.parquet('gender_metric_4.parquet')
# %%
gender_metric_4.orderBy(F.col('zero_ill_count').desc()).show(10)
#+---------+--------------------+--------------+------------+------------------+
#|   gender|        gender_label|zero_ill_count|gender_count|    zero_ill_ratio|
#+---------+--------------------+--------------+------------+------------------+
#| Q6581097|              "male"|       2298460|     5550576|41.409396069885354|
#| Q6581072|            "female"|       1007250|     1727639| 58.30211056823792|
#| Q1052281|"transgender female"|           106|         980|10.816326530612246|
#|   Q48270|        "non-binary"|            94|         422|22.274881516587676|
#| Q2449503|  "transgender male"|            36|         251|14.342629482071715|
#| Q1097630|          "intersex"|             9|          39|23.076923076923077|
#|   Q44148|     "male organism"|             6|          86| 6.976744186046512|
#|  Q189125|"transgender person"|             5|          13| 38.46153846153847|
#|Q18116794|       "genderfluid"|             4|          26|15.384615384615385|
#|  Q505371|           "agender"|             4|          13| 30.76923076923077|
#+---------+--------------------+--------------+------------+------------------+
#only showing top 10 rows 
