json-to-sql
===========

A little script that parses a JSON file and pushes it into a sql server

Use like:

    python j2s.py -i file.json -o 'postgres://didi@localhost:5432/test' -t test
