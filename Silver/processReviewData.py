from Silver.process import Process
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import StopWordsRemover

class ProcessReviewData(Process):

    def removeReviewStopWords(self, df):
        df = self.cleanText(df)
        remover = StopWordsRemover(inputCol="clean_text",outputCol="filtered_text")
        df = remover.transform(df)
        return df.drop("clean_text","text")

    def cleanText(self, df):
        regex = r',|\.|&|-|_|\'.|\"|\?|\!'
        df = df.withColumn("clean_text",f.regexp_replace(f.col("text"), regex, ''))
        df = df.withColumn("clean_text",f.regexp_replace(f.col("clean_text"), r'[\n]', ' '))
        df = df.withColumn("clean_text",f.lower(f.col("clean_text")))
        df = df.withColumn("clean_text",f.split(f.col("clean_text")," "))
        df = df.withColumn("clean_text",f.filter(f.col("clean_text"),lambda x: x!=''))
        return df

    def countWordsMap(self, df):
        counts = df.rdd.flatMap(lambda a: [(w,1) for w in a.filtered_text]).reduceByKey(lambda a,b: a+b).collect()
        print(counts)

    def run(self):
        df = self.readFromBronze()
        df = self.removeReviewStopWords(df)
        self.writeToSilver(df)
