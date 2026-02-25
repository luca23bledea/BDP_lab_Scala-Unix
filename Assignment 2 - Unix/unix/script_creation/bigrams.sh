#!/usr/bin/env bash
# This program should take an fileIn as first parameter
# It takes the input text file and outputs the 5 most common word bigrams (https://en.wikipedia.org/wiki/Bigram) in the file.
# Your solution should be case insensitive.
#
# example: when `./bigrams.sh ../data/myBook/01-chapter1.txt' is ran the output should look like this:
#   3 the     little
#   3 little  blind
#   3 blind   text
#   2 the     word
#   2 the     copy

fileIn="$1"

tr '[:upper:]' '[:lower:]' < "$fileIn" |          
tr -c '[:alpha:]' ' ' |                          
tr -s ' ' '\n' |                                  # put one word per line
awk '{
    if (prev != "") {
        print prev, $1      # print bigram: previous word + current word
    }
    prev = $1               # remember current word as previous
}' |
sort |
uniq -c |
sort -rn |
head -n 5
