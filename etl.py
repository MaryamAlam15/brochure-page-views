from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import udf
from pyspark.sql.window import Window


def create_spark_session():
    """
    this function creates Spark session.
    """
    spark = SparkSession.builder.getOrCreate()
    return spark


def extract_data(spark, input_data_path):
    """
    extracts data rom json file, create respective data-frames and views.
    :param spark: the spark session object.
    :param input_data_path: input data dir.
    :param output_data: output data dir.
    """
    # read input files.
    spark.read.json(f'{input_data_path}/brochure_clicks.json').createOrReplaceTempView('brochure_clicks')
    enters_events_df = spark.read.json(f'{input_data_path}/enters.json')
    page_turns_events_df = spark.read.json(f'{input_data_path}/page_turns.json')

    # create DFs and select required cols.
    enters_events_df = enters_events_df \
        .withColumn('enters_count', get_page_view_count('page_view_mode')) \
        .select('brochure_click_uuid', 'page', 'page_view_mode', 'date_time', 'enters_count')

    exits_events_df = spark.read.json(f'{input_data_path}/exits.json') \
        .withColumn('exits_count', get_page_view_count('page_view_mode'))

    page_turns_events_df = page_turns_events_df \
        .withColumn('turns_count', get_page_view_count('page_view_mode')) \
        .select('brochure_click_uuid', 'page', 'page_view_mode', 'date_time', 'turns_count')

    return enters_events_df, exits_events_df, page_turns_events_df


@udf
def get_page_view_count(view_mode):
    """
    returns the page view count wrt page view mode.
    :param view_mode: page view mode
    :return:
    """
    if view_mode == 'DOUBLE_PAGE_MODE':
        return 2
    elif view_mode == 'SINGLE_PAGE_MODE':
        return 1
    else:
        return 0


def calculate_user_clicks_events(spark, enters_events_df, exits_events_df, page_turns_events_df):
    """
    union all data-frames and partition by `brochure_click_uuid`
    sum-up page count in each partition.
    further sum-up viewed-count from all partitions against a user to get total_viewed.
    :param spark: spark session object.
    :param enters_events_df: user's enters_events dataframe.
    :param exits_events_df: user's exits_events dataframe.
    :param page_turns_events_df: user's page_turns_events dataframe.
    :return:
    """
    # partition turns_events_df by `brochure_click_uuid` to sum-up page_count in a single partition.
    window_spec = Window.partitionBy(page_turns_events_df.brochure_click_uuid) \
        .orderBy(page_turns_events_df.brochure_click_uuid)

    # the page user entered-on at first should also be counted as "viewed".
    page_viewed_events_df = page_turns_events_df.union(enters_events_df)

    # sum-up `turns_count` in each partition (i.e wtr brochure_click_uuid)
    viewed_count = functions.sum(page_viewed_events_df.turns_count).over(window_spec)
    page_viewed_events_df \
        .select('brochure_click_uuid', viewed_count.alias('viewed_count')) \
        .drop_duplicates() \
        .createOrReplaceTempView('page_viewed_events')

    # create views to query.
    enters_events_df.createOrReplaceTempView('enters_events')
    exits_events_df.createOrReplaceTempView('exits_events')

    # final query to collect required cols from source tables and sum-up view counts
    return spark.sql("""
            select
                bc.user_ident
                ,cast(sum(coalesce(ee1.enters_count, 0)) as int) as total_enters
                ,cast(sum(coalesce(ee.exits_count, 0)) as int) as total_exits
                ,cast(sum(pve.viewed_count) as int) as total_viewed
            from brochure_clicks bc
            inner join page_viewed_events pve
            on bc.brochure_click_uuid=pve.brochure_click_uuid
            inner join enters_events ee1
            on bc.brochure_click_uuid=ee1.brochure_click_uuid
            -- this is possible that user enter/viewed the brochure page but never exited.
            left join exits_events ee
            on bc.brochure_click_uuid=ee.brochure_click_uuid
            group by bc.user_ident, ee1.enters_count, ee.exits_count
            order by bc.user_ident
        """)


def load_data(df, output_data_path):
    """
    loads final data into json file int output dir.
    :param df: DF containing user clicks events
    :param output_data_path: path to output dir.
    :return:
    """
    df.show(truncate=False)
    df.coalesce(1).write.mode('overwrite').format('json').save(output_data_path)


def main():
    input_data_path = "data/"
    output_data_path = "output/"

    spark = create_spark_session()

    enter_events_df, exits_events_df, pg_turns_events_df = extract_data(spark, input_data_path)
    user_clicks_events = calculate_user_clicks_events(spark, enter_events_df, exits_events_df, pg_turns_events_df)
    load_data(user_clicks_events, output_data_path)


if __name__ == "__main__":
    main()
