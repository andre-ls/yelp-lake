from Silver.process import Process
from pyspark.sql import SparkSession, functions as f

class ProcessCheckinData(Process):

    def explodeDates(self, df):
        df = df.withColumn("date",f.split(f.col("date"),", "))
        df = df.withColumn("timestamp",f.explode("date"))
        return df

    def splitTimeStamp(self, df):
        df = df.withColumn("date",f.split(f.col("timestamp"), " ").getItem(0))
        df = df.withColumn("hour",f.split(f.col("timestamp"), " ").getItem(1))
        df = df.drop(f.col("timestamp"))
        return df

    def run(self):
        df = self.readFromBronze()
        df = self.explodeDates(df)
        df = self.splitTimeStamp(df)
        self.writeToSilver(df)
