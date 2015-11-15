#!/usr/bin/env python

import logging
import argparse
import sqlline

parser = argparse.ArgumentParser()
parser.add_argument('--debug', '-d', action='store_true')
parser.add_argument('url')
args = parser.parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)

with sqlline.SqlLine() as sqlline:
    sqlline.connect('phoenixdb', args.url)
    sqlline.connection.autocommit = True
    sqlline.run()
