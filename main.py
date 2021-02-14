from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from Input import input
from Process import process
from Output import output


class main:

    def runProcess(self):
        '''
        This function runs the whole process writing the requested outputs in the project's output folder.
        :return: write 5 dataframes
        '''
        try:
            sc = SparkContext(master='local', appName='Developer_Challenge')
            spark = SQLContext(sc)

            # class declaration
            read_data = input(spark)
            process_data = process(spark)
            write = output(spark)

            # Read data
            df: DataFrame = read_data.readData()

            # Process the data
            data = process_data.splitDataFrame(df)
            data = process_data.renameColumns(data)

            data.persist()

            # Write by nationality
            write.writeByNationality(data)

            # the 10 top players for each position
            # Falta

            # How many players for each position have each Club
            ply_by_club_pos = process_data.countPlayerPosClub(data)
            write.writePlyByClubPos(ply_by_club_pos)

            # top 10 clubs for sprint speed average
            top_spr_spd_avg = process_data.topSprintSpeedAverage(data)
            write.writeTopSprSpdAvg(top_spr_spd_avg)

            # All players with overweight (IMC>25)
            ply_overweight = process_data.claculateIMC(data)
            write.writePlyOverweight(ply_overweight)

        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    run = main()
    run.runProcess()


