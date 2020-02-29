from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator, \
    MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


class PostLengthExtractor(Transformer):
    """Transformer to extract post length."""

    def __init__(self, inputCol="post_text", outputCol='post_text_len'):
        """inputCol: name of input column, outputCol: name of output column."""
        super().__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol
        self.len_text_udf = udf(lambda l: len(l), IntegerType())

    def _transform(self, df):
        return df.withColumn(
            self.outputCol, self.len_text_udf(col(self.inputCol)))


class ArrayToVectorTransformer(Transformer):
    """Transformer to convert array to Vector."""

    def __init__(self, inputCol="post_text_emb", outputCol='post_text_emb'):
        """inputCol: name of input column, outputCol: name of output column."""
        super().__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol
        self.list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

    def _transform(self, df):
        return df.withColumn(
            self.outputCol, self.list_to_vector_udf(col(self.inputCol)[0]))


class RenameTransformer(Transformer):
    """Transformer to change column name."""

    def __init__(self, inputCol="post_text_len", outputCol='label'):
        """inputCol: name of input column, outputCol: name of output column."""
        super().__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        return df.withColumnRenamed(self.inputCol, self.outputCol)


class BooleanToIntTransformer(Transformer):
    """Transformer to convert boolean to int."""

    def __init__(self, inputCol="is_NSFW", outputCol='is_NSFW'):
        """inputCol: name of input column, outputCol : name of output column."""
        super().__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        return df.withColumn(self.outputCol, col(self.inputCol).cast('integer'))


class ClassTransformer(Transformer):
    """Transformer to extract classes based on subreddit_name length."""

    def __init__(self, inputCol="subreddit_name", outputCol='label'):
        """inputCol: name of input column, outputCol: name of output column."""
        super().__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol
        self.len_to_class = udf(lambda l: self.separate_class(l), IntegerType())

    def separate_class(self, subreddit_name):
        """Method to assign class based on subreddit_name."""
        subreddit_lenght = len(subreddit_name)
        if subreddit_lenght < 9:
            return 1
        elif subreddit_lenght < 18:
            return 2
        else:
            return 3

    def _transform(self, dataframe):
        return dataframe.withColumn(
            self.outputCol, self.len_to_class(col(self.inputCol)))


my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb/lsdp.reddits") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb.lsdp.reddits") \
    .config('spark.jars.packages',
            'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1') \
    .getOrCreate()

my_spark.sparkContext.setLogLevel("ERROR")
df = my_spark.read.format("mongo").load()

# Na podstawie embedingu pierwszego słowa -> dlugosc postu
# Na podstawie embedingu pierwszego słowa -> is NSFW
# Na podstawie embedingu pierwszego słowa -> dlugosc subreddit(<9, 9<18, 18<)


def split_data(data):
    """Split data to test and train."""
    train, test = data.randomSplit([0.8, 0.2], seed=123)
    return train, test


def regression(data):
    """Pipeline to linear regression."""
    yp = PostLengthExtractor()
    at = ArrayToVectorTransformer()
    rt = RenameTransformer()
    va = VectorAssembler(inputCols=['post_text_emb'], outputCol='features')
    lr = LinearRegression(featuresCol='features', labelCol='label')

    train, test = split_data(data)
    pipeline = Pipeline(stages=[yp, at, rt, va, lr])
    model = pipeline.fit(train)

    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="label", predictionCol="prediction")
    predictions = model.transform(train)
    rmse_train = evaluator.evaluate(predictions)
    print("Root-mean-square error na zbiorze treningowym= " + str(rmse_train))

    predictions = model.transform(test)
    rmse_test = evaluator.evaluate(predictions)
    print("Root-mean-square error na zbiorze tesotwym = " + str(rmse_test))


def binary_classification(data):
    """Pipeline to binary classification."""
    at = ArrayToVectorTransformer()
    bi = BooleanToIntTransformer()
    rt = RenameTransformer('is_NSFW')
    va = VectorAssembler(inputCols=['post_text_emb'], outputCol='features')
    lr = LogisticRegression()

    train, test = split_data(data)
    pipeline = Pipeline(stages=[bi, at, rt, va, lr])
    model = pipeline.fit(train)

    predictions = model.transform(test)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1")
    f1 = evaluator.evaluate(predictions)
    print("F1 score dla binary classification= %g " % f1)


def multiclass_classification(data):
    """Pipeline to multiclass classification."""
    at = ArrayToVectorTransformer()
    yp = PostLengthExtractor()
    ct = ClassTransformer()
    va = VectorAssembler(inputCols=['post_text_emb'], outputCol='features')
    lr = RandomForestClassifier(labelCol="label", featuresCol="features")

    train, test = split_data(data)
    pipeline = Pipeline(stages=[yp, at, ct, va, lr])
    model = pipeline.fit(train)

    predictions = model.transform(test)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1")
    f1 = evaluator.evaluate(predictions)
    print("F1 score dla multiclass classification = %g " % f1)


regression(df)
binary_classification(df)
multiclass_classification(df)
