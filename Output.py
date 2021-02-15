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
            data.coalesce(2).write.partitionBy(Nationality).mode(overwrite).orc(output_path_nationality)
        except Exception as ex:
            print(ex)

    def writeTopOVA(self, data: DataFrame):
        '''
        This function writes the dataframe partitioning by position
        :param data: A dataframe
        :return:
        '''
        try:
            data.coalesce(2).write.partitionBy(Position).mode(overwrite).orc(output_path_top_player_each_pos)
        except Exception as ex:
            print(ex)

    def writeDF(self, data: DataFrame, path: str):
        '''
        This function writes the data to the provided path.
        :param data: The data to write.
        :param path: The path where to write the data.
        :return:
        '''
        try:
            data.coalesce(2).write.mode(overwrite).orc(path)
        except Exception as ex:
            print(ex)
