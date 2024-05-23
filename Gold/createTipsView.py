from Gold.view import View
from pyspark.sql import SparkSession, functions as f

class CreateTipsView(View):

    def groupData(self, df):
        df = df.groupBy("business_id")\
               .agg(f.avg("compliment_count").alias("avg_compliment_count"),\
                    f.flatten(f.collect_list("filtered_text")).alias("filtered_text")\
                )
        return df

    def run(self):
        df = self.readFromSilver()
        df = self.groupData(df)
        self.writeToGold(df)
