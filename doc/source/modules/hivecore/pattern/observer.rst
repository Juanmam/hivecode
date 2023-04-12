Observer
========

.. autoclass:: hivecore.pattern.Observer
    :members: set, get, delete

Example
^^^^^^^
In this example we will check out how to access values defined in one scope from another.
In this case, we are accessing values inside a lambda and setting values inside a lambda.

..  code-block:: python
    
    from nltk.sentiment import SentimentIntensityAnalyzer

    from hivecore.pattern import Observer

    Observer().set("sentiment_analizer", SentimentIntensityAnalyzer())
    Observer().set("indicator", 0)

    phrases = ["test one", "happy test two", "sad test three"]

    def process_phrase(phrase):
        score = Observer.get("sentiment_analizer").polarity_scores(phrase)
        Observer().set("indicator", Observer().get("indicator") + score)
        return score

    scores = list(map(lambda phrase: , phrases))

    Observer().delete("sentiment_analizer")