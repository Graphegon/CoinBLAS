breakpoint()

import sys

from .bitcoin import *
b = Bitcoin(State.load_all(path=sys.argv[1]))
