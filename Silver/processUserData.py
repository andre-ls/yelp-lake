from Silver.process import Process
from pyspark.sql import SparkSession, functions as f

class ProcessUserData(Process):

    def splitFriends(self, df):
        df = df.withColumn("friends",f.split(f.col("friends"), ", "))
        return df

    def run(self):
        df = self.readFromBronze()
        df = self.splitFriends(df)
        self.writeToSilver(df)
