from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions

def _keyword_degree(input_df : DataFrame) -> DataFrame: 
    """Create the keyword degree
    
    Take the chunk annotation from spark nlp and give each keyword an
    
    id, array of words, and a degree

    Args:
        input_df (DataFrame): A spark dataframe
    """

    return_df = (
        input_df
        .withColumn(
            "keyword_data",
            pyspark.sql.functions.expr(
                """
                TRANSFORM(
                    chunk, 
                    x -> struct(
                        x.result as keyword, 
                        CONCAT(x.metadata.sentence, x.metadata.chunk) as keyword_id, 
                        split(x.result, ' ', 0) as words, 
                        size(split(x.result, ' ', 0))-1 as keyword_degree
                    )
                )"""
            )
        )
    )
    
    return return_df
    
def _word_rake(input_df : DataFrame) -> DataFrame:
    """Generate frequency, degree, and rake for each word

    Args:
        input_df (DataFrame): a spark dataframe 

    Returns:
        DataFrame: a spark dataframe
    """
    
    return_df = (
        input_df
        .withColumn(
            "word_rake",
            pyspark.sql.functions.expr(
                """
                TRANSFORM(
                    array_distinct(flatten(keyword_data.words)),  
                    x-> struct(
                            x as word, 
                            aggregate(
                                keyword_data, 
                                0,
                                (acc,t) -> acc + IF(array_contains(t.words, x), t.keyword_degree, 0)) / aggregate(flatten(keyword_data.words), (0 as n, 0 as tot_n), (acc,t) -> (acc.n+IF(t=x,1,0), acc.tot_n+1), acc -> 100 * acc.n/acc.tot_n
                                ) as word_rake
                        )
                )"""
            )
        )
    )
    
    return return_df

def _keyword_rake(input_df : DataFrame) -> DataFrame:
    """Calculate final rake score of keyword

    Args:
        input_df (DataFrame): a spark dataframe

    Returns:
        DataFrame: a spark dataframe
    """


    return_df = (
        input_df
        .withColumn(
            "keyword_rake",
            pyspark.sql.functions.expr(
                """
                TRANSFORM(                                              
                    array_distinct(arrays_zip(keyword_data.keyword, keyword_data.words, keyword_data.keyword_degree)),  
                    x-> struct(
                        x['0'] as keyword,
                        aggregate(keyword_data.keyword, (0 as n, 0 as tot_n), (acc,t) -> (acc.n+IF(t=x['0'],1,0), acc.tot_n+1), acc -> acc.n) as freq,
                        x['2'] + 1 as ngram,
                        aggregate(word_rake, cast(0 as double), (acc,t) -> acc + IF(array_contains(x['1'], t.word), t.word_rake, cast(0 as double)), acc -> round(acc, 4)) as rake

                        )
                )"""
            )
        )
    )
    
    return return_df 


def generate_rake(input_df: DataFrame, chunkCol: str) -> DataFrame:
    """Calculate rake and make output nice

    Args:
        input_df (DataFrame): a spark dataframe that contains a 'annotation chunk' column from Spark NLP to generate RAKE off of
        chunkCol (str): the name of the column that contains the annotation information

    Returns:
        DataFrame: a spark dataframe of keywords, ngram, frequency, and rake score
    """
    
    assert isinstance(input_df, DataFrame), 'input_df should be a spark dataframe'
    
    assert isinstance(chunkCol, str), 'chunkCol should be a string'
    
    assert chunkCol in input_df.columns, f'{chunkCol} is not a column in the dataframe supplied' 
    
    key_df = _keyword_degree(input_df = input_df.withColumnRenamed(chunkCol, 'chunk')).cache()
      
    word_df = _word_rake(key_df).cache()

    return_df = _keyword_rake(word_df).cache()

    return return_df
    
    
def explode_rake(input_df: DataFrame) -> DataFrame:
    """Transform RAKE column in input_df to a dataframe of words and rake scores

    Args:
        input_df (DataFrame): a spark dataframe containing a rake column 

    Returns:
        DataFrame: a spark dataframe of words and rake scores
    """

    return (
        input_df
        .select(
            pyspark.sql.functions.explode('keyword_rake').alias('rake')
        )
        .select(
            "rake.keyword",
            "rake.freq",
            "rake.ngram",
            "rake.rake"
        )
    )