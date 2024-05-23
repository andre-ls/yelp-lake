from Gold.view import View
from pyspark.sql import SparkSession, functions as f

class CreateReviewsView(View):

    def groupData(self, df):
        df = df.groupBy("business_id")\
               .agg(f.avg("stars").alias("avg_stars"),\
                    f.count("*").alias("number_reviews"),\
                    f.avg("useful").alias("avg_useful"),\
                    f.avg("funny").alias("avg_funny"),\
                    f.avg("cool").alias("avg_cool"),\
                    f.flatten(f.collect_list("filtered_text")).alias("filtered_text")\
                )
        return df

    def run(self):
        df = self.readFromSilver()
        df = self.groupData(df)
        self.writeToGold(df)
