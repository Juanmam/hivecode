import tiktoken


def token_estimator(string: str, encoding_name: str) -> int:
    """
        Estimate the number of tokens for a given string using a specified encoding.

        This function uses the tiktoken library to encode the given string and then 
        returns the number of tokens in the encoded string.

        :param string: The string for which the token count needs to be estimated.
        :type string: str

        :param encoding_name: The name of the encoding to be used for token estimation.
        :type encoding_name: str

        :return: The number of tokens in the encoded string.
        :rtype: int
    """
    encoding = tiktoken.encoding_for_model(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens
