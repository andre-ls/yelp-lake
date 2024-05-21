class Process:
    def __init__(self, spark, bronzeDirectory, silverDirectory):
        self.spark = spark
        self.bronzeDirectory = bronzeDirectory
        self.silverDirectory = silverDirectory

    def readFromBronze(self):
        return self.spark.read.option("inferSchema","true").parquet(self.bronzeDirectory)

    def writeToSilver(self, df):
        df.write.mode("overwrite").parquet(self.silverDirectory)
