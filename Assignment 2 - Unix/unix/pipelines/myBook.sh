#!/usr/bin/env bash
# ---- MY AMAZING BOOK ----
cd ./../data/myBook/ || exit

# -- Q1 --
echo "-- Q1 --"
# Write a pipeline that for the current directory prints the 10 most common letters in all text files.
# Your pipeline should be case insensitive and ignore punctuation and space characters.
# Example output:
# 309 e
# 229 t
mostCommonLetters=$(cat 00-intro.txt 01-chapter1.txt 02-chapter2.txt |
    tr "[:upper:]" "[:lower:]" |
    grep -o "[a-z]" |     # each char in its own line (could also use fold -w1)
    sort |
    uniq -c | 
    sort -rn |
    head -n 10)
# Prints the mostCommonLetters
echo "Most common letters in my book:"
echo "$mostCommonLetters"

# -- Q2 --
# Write a pipeline that finds the number of words in the book that have been used only once.
# Your pipeline should be case insensitive
echo "-- Q2 --"
onlyOnce=$(cat 00-intro.txt 01-chapter1.txt 02-chapter2.txt |
    tr -c "[:alpha:]" " " |
    tr "[:upper:]" "[:lower:]" |
    grep -o "[a-z]\+" |
    sort |
    uniq -c |
    awk '$1 == 1' |     # column 1 = 1
    wc -l)
echo "Words used only once:"
echo "$onlyOnce"

cd ../../pipelines/ || exit
