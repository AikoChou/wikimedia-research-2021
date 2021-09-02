# %%
from wmfdata.spark import get_session
spark = get_session(type='regular')

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T

# %%
snapshot = '2021-08-02'
monthly_snapshot = '2021-07'
target_wikis='enwiki arwiki kowiki cswiki viwiki frwiki fawiki ptwiki ruwiki trwiki plwiki hewiki svwiki ukwiki huwiki hywiki srwiki euwiki arzwiki cebwiki dewiki bnwiki eswiki itwiki'

# %%
def get_wikidata_for_gender(snapshot):
    """
    query `wmf.wikidata_entity` for entities with property of:
    * P31 (instance of): Q5 (human)
    * P21 (sex or gender)
    """
    query = """
        SELECT id AS qid,
            MAX(CASE WHEN claim.mainSnak.property = 'P31' THEN True ELSE NULL END) AS is_human,
            MAX(CASE WHEN claim.mainSnak.property = 'P21' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS gender
        FROM wmf.wikidata_entity
        LATERAL VIEW explode(claims) AS claim
        WHERE typ='item'
            AND snapshot='"""+snapshot+"""'
            AND ((claim.mainSnak.property='P31' AND claim.mainSnak.datavalue.value like '%\"id\":\"Q5\"%')
                OR claim.mainSnak.property='P21')
        GROUP BY qid
        """
    output_df = spark.sql(query)
    output_df = output_df.select('qid', 'is_human', F.json_tuple('gender', 'id').alias('gender'))
    return output_df

# %%
df = get_wikidata_for_gender(snapshot)

df.printSchema()
# root
#  |-- qid: string (nullable = true)
#  |-- is_human: boolean (nullable = true)
#  |-- gender: string (nullable = true)

# %%
df.count()
# 9312450

# %%
df.where(F.col('is_human').isNull()).count() # not human but has gender property -> fictional human, anime character...
# 82518

# %%
df.select('gender').distinct().count() 
# 41

# %%
def add_gender_label(input_df, snapshot):
    """
    query `wmf.wikidata_entity` to get gender labels for gender entities in `input_df`
    """
    input_df.createOrReplaceTempView('wikidata')
    query = """
        SELECT qid, is_human, gender, label_val AS gender_label
        FROM wikidata
        LEFT JOIN wmf.wikidata_entity we
        ON gender=we.id
        LATERAL VIEW explode(labels) t AS label_lang, label_val
        WHERE t.label_lang='en'
        AND typ='item'
        AND we.snapshot='"""+snapshot+"""'
    """
    output_df = spark.sql(query)
    return output_df

# %%
df = add_gender_label(df, snapshot)

df.printSchema()
# root
#  |-- qid: string (nullable = true)
#  |-- is_human: boolean (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- gender_label: string (nullable = true)

# %%
def add_related_wikipedia_pages(input_df, snapshot, target_wikis):
    """
    query `wmf.wikidata_item_page_link` to get page_id, page_title the wikidata item links to 
    for each of wikis in `target_wikis`
    """
    input_df.createOrReplaceTempView('wikidata')
    query = """
        SELECT q.qid, q.is_human, q.gender, q.gender_label,
            wipl.page_id, wipl.page_title, wipl.wiki_db
        FROM wikidata q
        LEFT JOIN wmf.wikidata_item_page_link wipl
        ON q.qid=wipl.item_id
        WHERE wipl.snapshot='"""+snapshot+"""'
            AND wipl.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\")
        ORDER BY q.qid
    """
    output_df = spark.sql(query)
    return output_df

# %%
df = add_related_wikipedia_pages(df, snapshot, target_wikis)

df.printSchema()
# root
#  |-- qid: string (nullable = true)
#  |-- is_human: boolean (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- gender_label: string (nullable = true)
#  |-- page_id: long (nullable = true)
#  |-- page_title: string (nullable = true)
#  |-- wiki_db: string (nullable = true)

# %%
df.count()
# 8220632

# %%
df.select(F.countDistinct('qid')).show()
# +-------------------+
# |count(DISTINCT qid)|
# +-------------------+
# |            3301150|
# +-------------------+

# %%
def add_page_creation_time(input_df, monthly_snapshot):
    """
    query `wmf.mediawiki_page_history` to get page creation time for each of pages in `input_df`
    """
    input_df.createOrReplaceTempView('wikidata_pages')
    query = """    
        SELECT p.page_id, p.page_title, p.wiki_db, 
            p.qid, p.is_human, p.gender_label,
            from_unixtime(MIN(unix_timestamp(mp.page_first_edit_timestamp, 'yyyy-MM-dd HH:mm:ss'))) AS date_created
        FROM wikidata_pages p
        LEFT JOIN wmf.mediawiki_page_history mp
        ON p.page_id=mp.page_id
        AND p.page_title=mp.page_title
        WHERE mp.snapshot='"""+monthly_snapshot+"""'
        GROUP BY p.page_id, p.page_title, p.wiki_db, p.qid, p.is_human, p.gender_label
        """
    output_df = spark.sql(query)
    return output_df

# %%
df = add_page_creation_time(df, monthly_snapshot)

df.printSchema()
# root
#  |-- page_id: long (nullable = true)
#  |-- page_title: string (nullable = true)
#  |-- wiki_db: string (nullable = true)
#  |-- qid: string (nullable = true)
#  |-- is_human: boolean (nullable = true)
#  |-- gender_label: string (nullable = true)
#  |-- date_created: string (nullable = true)

# %%
df.count()
# 8219153

# %%
df.where(F.col('date_created').isNull()).count()
# 178353

# %%
df.show(10)
# +-------+--------------------+-------+--------+--------+------------+-------------------+
# |page_id|          page_title|wiki_db|     qid|is_human|gender_label|       date_created|
# +-------+--------------------+-------+--------+--------+------------+-------------------+
# |    116|         Quang_Trung| viwiki| Q379775|    true|      "male"|2003-12-04 22:32:52|
# |    154|    Vasilij_Erošenko| cswiki|  Q20887|    true|      "male"|2003-03-16 12:23:29|
# |    332|                Aedh| plwiki| Q305341|    true|      "male"|2002-09-13 07:57:20|
# |    405|   Abraham_Fornander| plwiki|Q4668863|    true|      "male"|2002-10-11 08:09:35|
# |    790|        Bill_Clinton| trwiki|   Q1124|    true|      "male"|2004-01-20 04:21:54|
# |    825|           קלוד_מונה| hewiki|    Q296|    true|      "male"|2003-08-17 09:15:45|
# |   1198|        אנטון_ברוקנר| hewiki|  Q81752|    true|      "male"|2003-10-03 21:35:52|
# |   1408|Dmitrij_Ivanovič_...| cswiki|   Q9106|    true|      "male"|2003-11-19 00:17:19|
# |   1449|            Alan_Kay| enwiki|  Q92742|    true|      "male"|2001-08-16 04:49:41|
# |   1512|Pedro_Álvares_Cabral| ptwiki| Q174432|    true|      "male"|2003-01-07 03:36:17|
# +-------+--------------------+-------+--------+--------+------------+-------------------+
