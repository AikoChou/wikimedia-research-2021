# %%
from datetime import datetime
from wmfdata.spark import get_session
spark = get_session(type='regular')

import pyspark
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T

# %%
df = spark.read.parquet('gender_features.parquet')
df.printSchema()
# %%
monthly_snapshot = '2021-07'
# %%
def get_unix_timestamps(yyyy_mm):
    return str(datetime.fromisoformat('{}-01 00:00:00'.format(yyyy_mm)).timestamp())

print(get_unix_timestamps('2001-01'))
print(get_unix_timestamps('2021-07'))

# %%
def number_of_gender_article(input_df, wiki, start_t, end_t, gender_label=None):
    input_df.createOrReplaceTempView('wikidata_pages')
    if not gender_label: # count all biographical (is_human=True) articles
        query = """    
            SELECT COUNT(*) AS count
            FROM wikidata_pages
            WHERE wiki_db='"""+wiki+"""'
                AND is_human=True
                AND unix_timestamp(date_created, 'yyyy-MM-dd HH:mm:ss') >= """+start_t+"""
                AND unix_timestamp(date_created, 'yyyy-MM-dd HH:mm:ss') < """+end_t+"""
            """
    else:
        query = """    
            SELECT COUNT(*) AS count
            FROM wikidata_pages
            WHERE wiki_db='"""+wiki+"""'
                AND is_human=True
                AND unix_timestamp(date_created, 'yyyy-MM-dd HH:mm:ss') >= """+start_t+"""
                AND unix_timestamp(date_created, 'yyyy-MM-dd HH:mm:ss') < """+end_t+"""
                AND gender_label='\""""+gender_label+"""\"'
            """
    output_df = spark.sql(query)
    return output_df.collect()[0]['count']

def number_of_wikipedia_pages(wiki, start_t, end_t):
    query = """
        SELECT COUNT(*) AS count
        FROM wmf_raw.mediawiki_page p
        LEFT JOIN wmf.mediawiki_page_history mp
        ON p.page_id=mp.page_id
        WHERE p.snapshot='"""+monthly_snapshot+"""'
            AND p.wiki_db='"""+wiki+"""'
            AND p.page_namespace=0
            AND p.page_is_redirect=0
            AND mp.snapshot='"""+monthly_snapshot+"""'
            AND mp.wiki_db='"""+wiki+"""'
            AND unix_timestamp(mp.page_first_edit_timestamp, 'yyyy-MM-dd HH:mm:ss') >= """+start_t+"""
            AND unix_timestamp(mp.page_first_edit_timestamp, 'yyyy-MM-dd HH:mm:ss') < """+end_t+"""
        """
    output_df = spark.sql(query)
    return output_df.collect()[0]['count']

# %%
def generate_gender_metric(wiki, month_list, gender_list):
    result = {}
    for i, t in enumerate(month_list):
        if i == len(month_list)-1:
            continue
        result[t] = {}
        result[t]['total'] = number_of_wikipedia_pages(wiki=wiki,
                                    start_t=get_unix_timestamps(month_list[i]), 
                                    end_t=get_unix_timestamps(month_list[i+1]))
        for gender in gender_list:
            r = number_of_gender_article(input_df=df, wiki=wiki, 
                                        start_t=get_unix_timestamps(month_list[i]), 
                                        end_t=get_unix_timestamps(month_list[i+1]), 
                                        gender_label=gender)
            if not gender:
                result[t]['people'] = r
            else:
                result[t][gender] = r
    return result

# %%
wiki = 'enwiki'
month_list = ['2021-01', '2021-02', '2021-03']
gender_list = [None, 'male', 'female']

r = generate_gender_metric('enwiki', month_list, gender_list)
r
# {'2021-01': {'total': 50033, 'people': 7252, 'male': 5199, 'female': 2050},
# '2021-02': {'total': 50180, 'people': 7290, 'male': 5189, 'female': 2089}}