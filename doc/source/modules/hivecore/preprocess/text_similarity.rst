Text Similarity
===============

.. autofunction:: hivecore.preprocess.text_similarity
   :noindex:

Parameters
^^^^^^^^^^
* method: Defined only for the vectorization type when using cosine similarity as the engine.
    * "tfid"
    * "count"

* engine: The type of similarity to apply. The following are good options:
    * cosine: Uses cosine similarity. Recommended for long texts.
    * jaro: Uses Jaro similarity. Recommended for short texts or words.
    * jaro_winkler: Uses Jaro-winkler similarity.
    * levenshtein
    * damerau_levenshtein
    * hamming

Basic Example
^^^^^^^^^^^^^
In this case, the text_similarity will apply cosine similarity using tfid vectorization and creates a new column with the most similar lable.

..  code-block:: python
    
    text_similarity(df, "username")

Engine Example
^^^^^^^^^^^^^^
In this case, the text_similarity will apply jaro similarity and overwrite the original column with the most similar lable with a threshold of 75%.

..  code-block:: python
    
    text_similarity(df, ["firstname", "lastname"], engine="jaro", overwrite=True, threshold=0.75)