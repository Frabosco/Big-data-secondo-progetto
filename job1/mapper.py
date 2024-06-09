#!/usr/bin/env python3
"""mapper.py"""

import sys



for line in sys.stdin:
    line = line.strip()

    cells=line.split(",")

    if cells[0]!='':
        print(f"{cells[1:3]}\t{cells[5:]}")
