#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    line = line.strip()

    cells=line.split(",")
    cells.pop(6)
    cells.pop(6)

    if cells[0]!='':
        print(f"{cells[1:5]}\t{cells[5:]}")
