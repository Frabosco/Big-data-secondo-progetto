#!/usr/bin/env python3
"""reducer.py"""

import sys
word_2_sum = {}

for line in sys.stdin:

    line = line.strip()
    current_word, current_count = line.split("\t")

    try:
        current_count = int(current_count)
    except ValueError:
        continue

    if current_word not in word_2_sum:
        word_2_sum[current_word] = 0
    word_2_sum[current_word]+= current_count

for word in word_2_sum:
    print("%s\t%i" % (word, word_2_sum[word]))