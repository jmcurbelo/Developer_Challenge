from paths import input_path


class input:
    """
    This class is used for read the input data
    """
    def __init__(self, spark):
        self.spark = spark

    def readData(self):
        """
        This function read the input data used in the project
        :return: A dataframe
        """
        try:
            return self.spark.read.option('header', 'true').csv(input_path)
        except Exception as ex:
            print(ex)
