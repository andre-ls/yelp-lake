from Silver.process import Process
from pyspark.sql import SparkSession, functions as f

class ProcessBusinessData(Process):

    def convertBooleanAttributesToList(self, df):
        df_attributes = df.select("attributes.*")
        non_null_cols = [f.when(f.col("attributes." + c) == "True", f.lit(c)) for c in df_attributes.columns]
        df = df.withColumn("attributes_list", f.array(*non_null_cols))\
               .withColumn("attributes_list", f.expr("array_join(attributes_list, ',')"))

        df = df.drop(f.col("attributes"))
        return df

    def run(self):
        df = self.readFromBronze()
        df = self.convertBooleanAttributesToList(df)
        self.writeToSilver(df)
