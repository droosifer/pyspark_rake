# run as is after cloning

import pyspark.sql
from pyspark_rake.rake import generate_rake
import pyspark.ml
import pyspark.sql.functions as F
import sparknlp

spark = (
    pyspark
    .sql
    .SparkSession
    .builder
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.0.0")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

pos_movie_reviews = (
    spark
    .read
    .format("text")
    .schema("text STRING")
    .load("/home/drew/Documents/Projects/spark_nlp_rake/src/data/pos/")
    .limit(2)
    )

documentAssembler = sparknlp.DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

sentenceDetector = sparknlp.annotators.SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

token = sparknlp.annotators.Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token") \
    .setContextChars(["(", "]", "?", "!", ".", ","])

lemmatizer = sparknlp.annotators.LemmatizerModel.pretrained() \
    .setInputCols(["token"]) \
    .setOutputCol("lemma_token")

pos_tagger = sparknlp.annotators.PerceptronModel.pretrained("pos_ud_ewt", "en") \
    .setInputCols(["document", "lemma_token"]) \
    .setOutputCol("pos")

chunker = sparknlp.annotators.Chunker() \
    .setInputCols("sentence", "pos") \
    .setOutputCol("chunk") \
    .setRegexParsers(["<NOUN>+", "<ADJ>+", "<NOUN>+<ADJ>+","<ADJ>+<NOUN>+","<ADJ>+<NOUN>+<ADJ>+", "<NOUN>+<ADJ>+<NOUN>+", "<NOUN>+<ADJ>+<NOUN>+<ADJ>+", "<ADJ>+<NOUN>+<ADJ>+<NOUN>+"])

pipeline = pyspark.ml.Pipeline().setStages([
    documentAssembler,
    sentenceDetector,
    token,
    lemmatizer,
    pos_tagger,
    chunker
])

return_df = pipeline.fit(pos_movie_reviews).transform(pos_movie_reviews)

generate_rake(input_df=return_df, chunkCol='chunk')