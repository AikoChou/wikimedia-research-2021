from wmfdata.spark import get_session
spark = get_session(type='regular')

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window


def get_page_dataframe(monthly_snapshot, target_wikis):
    """
    returns a dataframe with `wiki_db`, `page_id` and `page_title` of the wikipedia articles in `target_wikis`.
    """
    query = """
        SELECT wiki_db, page_id, page_title
        FROM wmf_raw.mediawiki_page
        WHERE snapshot='"""+monthly_snapshot+"""'
            AND wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\")
            AND page_namespace=0
            AND page_is_redirect=0
        """
    df = spark.sql(query)
    return df


def get_wikipedia_page_from_wikidata(df, snapshot, target_wikis):
    """
    `df` should contain `qid` column (QIDs for wikidata items)

    returns a dataframe with `wiki_db`, `page_id` and `page_title` of the wikipedia articles the wikidata items link to.
    """
    df.createOrReplaceTempView('temp_df')
    query = """
        SELECT qid, wipl.wiki_db, wipl.page_id, wipl.page_title
        FROM temp_df
        LEFT JOIN wmf.wikidata_item_page_link wipl
        ON qid=wipl.item_id
        WHERE wipl.snapshot='"""+snapshot+"""'
            AND wipl.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\")
    """
    output_df = spark.sql(query)
    return output_df


def number_of_wikipedia_pages(monthly_snapshot, target_wikis):
    """
    returns a dataframe with schema of
        |-- wiki_db: string (nullable = true)
        |-- year: string (nullable = true)
        |-- month: string (nullable = true)
        |-- article_count: long (nullable = false)
        |-- cumsum: long (nullable = true)
   
    * `article_count`: number of artcicle created in the period of `year` and `month` in `wiki_db`
    * `cumsum`: cumulative sum of `article_count`
    """
    query = """
        SELECT p.wiki_db, p.page_id, mp.page_first_edit_timestamp 
        FROM wmf_raw.mediawiki_page p
        LEFT JOIN wmf.mediawiki_page_history mp
        ON p.page_id=mp.page_id
            AND P.wiki_db=mp.wiki_db
        WHERE p.snapshot='"""+monthly_snapshot+"""'
            AND p.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\")
            AND p.page_namespace=0
            AND p.page_is_redirect=0
            AND mp.snapshot='"""+monthly_snapshot+"""'
            AND mp.wiki_db in (\""""+target_wikis.replace(' ', '\",\"')+"""\")
        """
    df = spark.sql(query).dropDuplicates()
    df = (df
        .where(F.col('page_first_edit_timestamp').isNotNull())
        .withColumn('year', F.substring('page_first_edit_timestamp', 1, 4))
        .withColumn('month', F.substring('page_first_edit_timestamp', 6, 2))
        .groupby('wiki_db','year','month').count()
        .withColumnRenamed('count', 'article_count')
        .withColumn('cumsum', (F.sum('article_count')
                                .over(Window.partitionBy('wiki_db')
                                .orderBy('year', 'month')))))
    return df


### Methods for extracting page properties ###
def append_page_first_edit_timestamp(df, monthly_snapshot):
    """
    `df` should contain `pageid`, `wiki_db` columns

    returns a dataframe with `page_first_edit_timestamp` containing the creatation timestamps of the articles
        in 'yyyy-MM-dd HH:mm:ss' string format.
    """
    df.createOrReplaceTempView('temp_df')
    # use MAX aggregation because of the case of
    # |20156072| enwiki|                     null|
    # |20156072| enwiki|     2008-11-10 22:23:...|
    query = """    
        SELECT t.wiki_db, t.page_id,  
            MAX(mp.page_first_edit_timestamp) AS page_first_edit_timestamp
        FROM temp_df t
        LEFT JOIN wmf.mediawiki_page_history mp
        ON t.page_id=mp.page_id 
            AND t.wiki_db=mp.wiki_db
        WHERE mp.snapshot='"""+monthly_snapshot+"""'
        GROUP BY t.page_id, t.wiki_db
        """
    # there may have duplicate timestamps due to rows with different action 
    # (move, create, rename, etc) in the page history
    output_df = spark.sql(query).dropDuplicates()
    return output_df

