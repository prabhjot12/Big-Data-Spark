import sys
from pyspark.ml.linalg import Vectors
from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.sql import SparkSession, functions, types,Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer,VectorAssembler,SQLTransformer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier,MultilayerPerceptronClassifier

spark = SparkSession.builder.appName('colour predicter').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+


def main(inputs):
    data = spark.read.csv(inputs, header=True, schema=colour_schema)
    lab_query = rgb2lab_query(passthrough_columns=['labelword'])
    sqlTrans = SQLTransformer(statement=lab_query)
    #data=sqlTrans.transform(data)
    #data.show()
    # TODO: actually build the components for the pipelines, and the pipelines.
    indexer = StringIndexer(inputCol="labelword", outputCol="indexed", handleInvalid='error')
    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    lab_assembler= VectorAssembler(inputCols=["lL","lA","lB"],outputCol="features")
    # TODO: need an evaluator
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="indexed")
    # TODO: split data into training and testing
    train, test = data.randomSplit([0.8,0.2],seed=1234)
    train = train.cache()
    test = test.cache()
    rf = RandomForestClassifier(featuresCol="features",numTrees=30, labelCol="indexed", seed=42)
    mlp = MultilayerPerceptronClassifier(featuresCol="features", labelCol="indexed", layers=[3, 90,90, 11])
    models = [
        ('RGB-forest', Pipeline(stages=[indexer,rgb_assembler,rf])),
        ('RGB-MLP', Pipeline(stages=[indexer,rgb_assembler,mlp])),
        ('LAB-forest', Pipeline(stages=[sqlTrans,indexer,lab_assembler,rf])),
        ('LAB-MLP', Pipeline(stages=[sqlTrans,indexer,lab_assembler,mlp])),
    ]
    for label, pipeline in models:
        # TODO: fit the pipeline to create a model
        model=pipeline.fit(train)
        prediction=model.transform(test)
        # Output a visual representation of the predictions we're
        # making: uncomment when you have a model working
        plot_predictions(model, label)

        # TODO: predict on the test data
        #predictions =

        # calculate a score
        score = evaluator.evaluate(prediction)
        print(label, score)


if __name__ == "__main__":
    inputs = sys.argv[1]
    #output = sys.argv[2]
    main(inputs)
