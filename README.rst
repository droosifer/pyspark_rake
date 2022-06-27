============
pyspark_rake
============


RAKE implementation using spark sql and pyspark.

`RAKE <https://www.analyticsvidhya.com/blog/2021/10/rapid-keyword-extraction-rake-algorithm-in-natural-language-processing/>`_ (Rapid Keyword Extraction) 
is a way to rank and extract keywords based on word frequency and co-currences. 

The motivitation for creating this is to translate the RAKE algorithim 
that was available in the R `udpipe <https://github.com/bnosac/udpipe>`_ package.

R does not contain the same Spark streaming support as Python and in order to scale RAKE
there was a need to translate it to Spark SQL.

`YAKE <https://nlp.johnsnowlabs.com/api/python/reference/autosummary/sparknlp/annotator/keyword_extraction/yake_keyword_extraction/index.html?highlight=yake#module-sparknlp.annotator.keyword_extraction.yake_keyword_extraction>`_ 
is another keyword extraction algorithim implemented in scala which is available
via the sparknlp Python API.

My hope for the  future would be to have RAKE and other keyword extraction techniques
available within the sparknlp ecosystem due to its reliability and ease of use.

For now this is my hacked together implementation.

This package assumes that you are...

* developing your nlp solution within the `sparknlp <https://nlp.johnsnowlabs.com/api/python/>`_ ecosystem
* have a "chunk" type annotation column that contains your keywords in a spark dataframe.
* developing solely in Spark/PySpark

Features
--------

* Generate a new column in a Spark dataframe of keywords and their associated RAKE scores
* Explode RAKE column into more user friendly format
* Pure Spark SQL implementation (no serialization to python)
* No shuffling of the data (high order functions to transform keyword arrays in place)

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

Sample movie data used to test functions and demonstrate examples can be found here 

License & Doc
-------------

* Free software: MIT license
* Documentation: https://pyspark-rake.readthedocs.io.

