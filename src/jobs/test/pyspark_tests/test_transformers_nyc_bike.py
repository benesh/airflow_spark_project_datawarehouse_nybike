from chispa.dataframe_comparer import *
import pytest
from pyspark.testing.utils import assertDataFrameEqual

from ...pyspark.interfaces import DataTransformer
from ...pyspark.transformers import RenameColumn,AddColumnDiffTime
from collections import namedtuple




OldDataNybikeBronze = namedtuple("tripduration",
                                    "starttime",
                                    "stoptime",
                                    "start station id",
                                    "start station name",
                                    "start station latitude",
                                    "start station longitude",
                                    "end station id",
                                    "end station name",
                                    "end station latitude",
                                    "end station longitude",
                                    "bikeid",
                                    "usertype",
                                    "birth year",
                                    "gender")

NewDataNybikeBronze= namedtuple("ride_id",
                                    "rideable_type",
                                    "started_at",
                                    "ended_at",
                                    "start_station_name",
                                    "start_station_id",
                                    "end_station_name",
                                    "end_station_id",
                                    "start_lat",
                                    "start_lng",
                                    "end_lat",
                                    "end_lng",
                                    "member_casual")

StandardDataNybikeBronze=namedtuple("start_station_id",   
                                        "start_station_name",
                                        "start_station_latitude",
                                        "start_station_longitude",
                                        "end_station_id",
                                        "end_station_name",
                                        "end_station_latitude",
                                        "end_station_longitude",
                                        "bike_id",
                                        "user_type",
                                        "gender",
                                        "customer_year_birth",
                                        "rideable_type",
                                        "start_at",
                                        "stop_at",
                                        "trip_duration")

TimesFormat=namedtuple("start_at","stop_at")
TimesFormat2=namedtuple("start_at","stop_at","trip_duration")

# def test_RenameColumn(spark):
#     input_data=[
#         StandardDataNybikeBronze("239","Willoughby St & Fleet St",	40.69196566 , -73.9813018, "366","Clinton Ave & Myrtle Ave",40.693261,-73.968896,16052,"Subscriber","1","1982","2013-10-01 00:01:08","2013-10-01 00:06:34",326),
#         StandardDataNybikeBronze("322","Clinton St & Tillary St",40.696192,-73.991218,"398","Atlantic Ave & Furman St",40.69165183,-73.9999786,19412,"Customer","0","","2013-10-01 00:01:21","2013-10-01 00:13:30",729)
#     ]
#     OldDataNybikeBronze=()
#     NewDataNybikeBronze=()


def test_AddcolumnDiffTime(spark):
    input_data = [
        TimesFormat("2013-10-01 00:01:08","2013-10-01 00:06:34"),
        TimesFormat("2013-10-01 00:01:21","2013-10-01 00:13:30")
    ]
    config={
        'column_result':'trip_duration',
        'column_greather':'stop_at',
        'colmun_lesser':'start_at'
    }
    source_df=spark.createDataFrame(input_data)
    result_df = AddColumnDiffTime.run(source_df,config)
    
    expected_data = [
        TimesFormat("2013-10-01 00:01:08","2013-10-01 00:06:34",326),
        TimesFormat("2013-10-01 00:01:21","2013-10-01 00:13:30",729)
    ]
    expected_df = spark.createDataframe(expected_data)

    assert_df_equality(expected_df,result_df)