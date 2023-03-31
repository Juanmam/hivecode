from hivecore.decorator import singleton
from typing import Any

@singleton
class Observer:
    """
    An observer class. This type of class let's you to add attributes, fetch their values and remove them dynamiclly. Quite useful to access data inside a defined scope as it behaves as a Singleton.
    """

    def set(self, key: str, val: Any) -> None:
        """
        Let's you add and overwrite attributes given the name and value. You could also overwrite it by just accessing the value, but that may not be recommended as later versions may change attributes to be encapsulated.

        :param key: Name of the attribute to create/modify.
        :type key: str
        :param val: Value of the attribute to create/modify.
        :type val: Any
        :return: None
        :rtype: None
        """
        setattr(self, key, val)

    def get(self, key: str) -> Any:
        """
        Let's you fetch data from the Observer. Current version lets you access data directly, but later versions may not support this, so the use of this method is highly recommended.

        :param key: Name of the attribute you want to fetch.
        :type key: str
        :return: The value stored in the attribute.
        :rtype: Any
        """
        return getattr(self, key)

    def delete(self, key: str) -> None:
        """
        Let's you remove data from the Observer.

        :param key: Name of the attribute to remove.
        :type key: str
        :return: None
        :rtype: None
        """
        delattr(self, key)
