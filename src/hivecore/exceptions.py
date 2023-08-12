"""
A set of well defined exceptions to describe errors during the code.
"""

class StrategyNotDefinedError(Exception):
    """Exception raised for undefined strategy scenarios."""
    
    def _init_(self, strategy_name: str):
        """
        Initialize the exception with the name of the undefined strategy.
        
        :param strategy_name: The name of the undefined strategy.
        :type strategy_name: str
        """
        self.strategy_name = strategy_name
        super()._init_(f"Strategy '{strategy_name}' is not defined.")