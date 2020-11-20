import sys
from pickle import dump

if __name__ == '__main__':
    from .bitcoin import Bitcoin
    b = Bitcoin()
    b.build(sys.argv[1])
    with open('blocks/metadata.pickle', 'wb') as f:
        dump(b, f)

        
