from Constants import *
from pyspark.sql.functions import split, explode
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class process:
    """
    This class contain all function to process the data
    """
    def __init__(self, spark):
        self.spark = spark

    def renameColumns(self, data: DataFrame):
        """
        This function rename all column with incorrect format
        :param data: A dataframe
        :return: The same dataframe but with all names of column with correct format
        """
        try:
            old_names = data.schema.names
            new_names = [x.replace(' ', '_') for x in old_names]
            new_names = [x.replace('&', 'and') for x in new_names]
            new_names = [x.replace('/', '_') for x in new_names]
            for i in zip(old_names, new_names):
                data = data.withColumnRenamed(i[0], i[1])
            return data
        except Exception as ex:
            print(ex)

    def splitDataFrame(self, data: DataFrame):
        """
        This function separates the positions of the players leaving them in separate rows
        :param data: A dataframe
        :return: A dataframe with one player and only one position per row
        """
        try:
            return data.withColumn(Single_position, explode(split(Position, ' '))).drop(Position)
        except Exception as ex:
            print(ex)

    def countPlayerPosClub(self, data: DataFrame):
        """
        This function counts how many players per position each club has
        :param data: A dataframe with the players
        :return: A dataframe with the player counts of each club by position
        """
        try:
            return data.groupBy(Club, Single_position).count()\
                .withColumnRenamed(count, numbers_of_players_by_club_and_position)
        except Exception as ex:
            print(ex)

    def topSprintSpeedAverage(self, data: DataFrame):
        """
        This function finds the top 10 clubs according to the average sprint speed
        :param data: A dataframe
        :return: A dataframe with only the top 10 clubs average sprint speed
        """
        try:
            return data.groupBy(Club).agg(F.avg(F.col(Sprint_Speed)))\
                .orderBy(F.desc('avg(Sprint_Speed)')).withColumnRenamed('avg(Sprint_Speed)', top_sprint_speed_average)\
                .limit(10)
        except Exception as ex:
            print(ex)

    def calculateIMC(self, data: DataFrame):
        """
        The IMC was developed by the mathematician Lambert Adolphe Quetelet in the 19th century, based on the weight
        and height of each subject.
        Currently it is used in the assessment of patients to determine the ideal weight, since it is obtained only by
        weighing and measuring their height, and applying the following formula:
        IMC = weight / (height^2)   ((Kg/m^2))
        In this case we use:
            1 feet = 0.3048 m (by Google)
            1 lbs = 0.453592 Kg  (by Google)
            overweight (IMC>25)
        :param data: A dataframe
        :return: A data frame with the name, weight in kilograms, height in meters and the BMI of each overweight
        player (IMC>25)
        """
        try:
            return data.withColumn(height_temp, F.regexp_replace(Height, '"', ''))\
                .withColumn(height_temp1, F.regexp_replace(height_temp, '\'', '.'))\
                .withColumn(height_mtr, F.col(height_temp1)*0.3048)\
                .withColumn(weight_temp, F.regexp_replace(Weight, 'lbs', ''))\
                .withColumn(weight_kg, F.col(weight_temp)*0.453592)\
                .withColumn(IMC, F.col(weight_kg)/(pow(F.col(height_mtr), 2)))\
                .select(F.col(Name), F.col(weight_kg), F.col(height_mtr), F.col(IMC))\
                .filter(F.col(IMC) > 25).distinct().orderBy(F.desc(IMC))
        except Exception as ex:
            print(ex)

    def topPlayersOVA(self, data: DataFrame):
        """
        This feature finds the top 10 players for each position based on the OVA indicator
        :param data: A dataframe
        :return: A dataframe with the top 10 players for each position
        """
        try:
            df = data.select(F.col(Name), F.col(OVA), F.col(Single_position).alias(Position),
                             F.row_number().over(Window.partitionBy(Single_position)
                                                 .orderBy(F.col(OVA).desc())).alias(row_number))\
                .filter(F.col(row_number).isin(top10)).drop(row_number)
            return df
        except Exception as ex:
            print(ex)
