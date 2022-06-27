#!/usr/bin/env python

"""Tests for `pyspark_rake` package."""

import pytest
from pyspark_rake.rake import _keyword_degree, _word_rake, _keyword_rake
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


@pytest.mark.usefixtures("spark_session")
def test_keyword(spark_session):
    test_df = spark_session.read.parquet('./tests/data')
    assert _keyword_degree(test_df.select('chunk')).select("keyword_data").sort("keyword_data").collect() ==  test_df.select("keyword_data").sort("keyword_data").collect(), "keywords not generated correctly"
    
    
@pytest.mark.usefixtures("spark_session")
def test_word_rake(spark_session):
    test_df = spark_session.read.parquet('./tests/data')
    assert _word_rake(test_df.select('keyword_data')).select("word_rake").sort("word_rake").collect() ==  test_df.select("word_rake").sort("word_rake").collect(), "word rake not generated correctly"
    
    
@pytest.mark.usefixtures("spark_session")
def test_rake(spark_session):
    test_df = spark_session.read.parquet('./tests/data')
    assert _keyword_rake(test_df.drop('chunk', 'keyword_rake')).select("keyword_rake").sort("keyword_rake").collect() ==  test_df.select("keyword_rake").sort("keyword_rake").collect(), "rake not generated correctly"
