from Silver.process import Process
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

class ProcessTipData(Process):

    def splitTimeStamp(self, df):
        df = df.withColumn("timestamp",f.col("date"))
        df = df.withColumn("date",f.split(f.col("timestamp"), " ").getItem(0))
        df = df.withColumn("hour",f.split(f.col("timestamp"), " ").getItem(1))
        df = df.drop(f.col("timestamp"))
        return df

    def removeTipStopWords(self, df):
        df = self.removeTextPunctuation(df)
        df = df.withColumn("no_punc_text",f.split(f.col("no_punc_text")," "))
        remover = StopWordsRemover(inputCol="no_punc_text",outputCol="filtered_text")

        df = remover.transform(df)
        return df.drop("no_punc_text","text")

    def removeTextPunctuation(self, df):
        regex = r',|\.|&|\\|\||-|_|  '
        df = df.withColumn("no_punc_text",f.regexp_replace(f.col("text"), regex, ''))
        return df

    def run(self):
        df = self.readFromBronze()
        df = self.splitTimeStamp(df)
        df = self.removeTipStopWords(df)
        self.writeToSilver(df)
