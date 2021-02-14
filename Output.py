from Constants import *
from paths import *
from pyspark.sql import DataFrame


class output:
    '''
    This class contains all function necessary to write the files in ORC format for each case
    '''
    def __init__(self, spark):
        self.spark = spark

    def writeByNationality(self, data: DataFrame):
        '''
        This function write the data partition by nationality
        :param data: A data frame that contain a column named Nationality
        :return: A file in  ORC format on your local file system partitioned by nationality
        '''
        try:
            data.coalesce(2).write.partitionBy(Nationality).mode('overwrite').orc(output_path_nationality)
        except Exception as ex:
            print(ex)

    def writePlyByClubPos(self, data: DataFrame):
        '''
        This function write the data corresponding to how many players for each position have each Club
        :param data: A dataframe
        :return: A file in  ORC format on your local file system
        '''
        try:
            data.coalesce(2).write.mode('overwrite').orc(output_path_ply_by_club_pos)
        except Exception as ex:
            print(ex)

    def writeTopSprSpdAvg(self, data: DataFrame):
        '''
        This function write the data corresponding to the top 10 clubs for sprint speed average
        :param data: A dataframe
        :return: A file in  ORC format on your local file system
        '''
        try:
            data.coalesce(2).write.mode('overwrite').orc(output_path_top_spr_spd_avg)
        except Exception as ex:
            print(ex)

    def writePlyOverweight(self, data: DataFrame):
        '''
        This function write the data corresponding to the all players with overweight (IMC>25)
        :param data: A dataframe
        :return: A file in  ORC format on your local file system
        '''
        try:
            data.coalesce(2).write.mode('overwrite').orc(output_path_ply_overweight)
        except Exception as ex:
            print(ex)

