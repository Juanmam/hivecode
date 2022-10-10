from native.decorators import Singleton
import sys, os

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

@Singleton
class MyTest:
    def __init__(self):
        pass

def main():
    assert MyTest() is MyTest() # Check if both instances are the same

if __name__ == '__main__':
    main()