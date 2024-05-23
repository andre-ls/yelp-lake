class View:
    def __init__(self, spark, silverDirectory, goldDirectory):
        self.spark = spark
        self.silverDirectory = silverDirectory
        self.goldDirectory = goldDirectory

    def readFromSilver(self):
        return self.spark.read.option("inferSchema","true").parquet(self.silverDirectory)
    
    def writeToGold(self, df):
        df.write.mode("overwrite").parquet(self.goldDirectory)
