from Gold.view import View
from pyspark.sql import SparkSession, functions as f

class CreateCheckinsView(View):

    def groupData(self, df):
        return df.groupBy(["business_id","date"]).count()

    def run(self):
        df = self.readFromSilver()
        df = self.groupData(df)
        self.writeToGold(df)
