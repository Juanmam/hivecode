Observer
========

.. role:: method

hivecore.pattern. :method:`Observer()`

    An Observer pattern implementation. It includes a
    setter, a getter and a delete method to manipulate
    data inside the Observer.

Methods
^^^^^^^
=================================================  ================================================
:doc:`set(key, val) <methods/observer_set>`        Stores a value in the Observer with a key.      
:doc:`get(key) <methods/observer_get>`             Returns the value from the Observer given a key.
:doc:`delete(key) <methods/observer_delete>`       Deletes the (key,value) from the observer.      
=================================================  ================================================

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