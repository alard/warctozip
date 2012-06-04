#!/usr/bin/env python
"""warcextract - dump warc record context to standard out"""

import os
import sys

import sys
import os.path

from optparse import OptionParser
from contextlib import closing

from zipfile import ZipFile
import re

from hanzo.warctools import ArchiveRecord, WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

parser = OptionParser(usage="%prog [options] warc zip")

#parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
    (options, args) = parser.parse_args(args=argv[1:])

    out = sys.stdout
    if len(args) < 1:
        # dump the first record on stdin
        with closing(WarcRecord.open_archive(file_handle=sys.stdin, gzip=None)) as fh:
            dump_record(fh)
        
    else:
        filename = args[0]
        zipfilename = args[1]

        with ZipFile(zipfilename, "w") as outzip:
            with closing(ArchiveRecord.open_archive(filename=filename, gzip="auto")) as fh:
                dump_record(fh, outzip)


    return 0

def dump_record(fh, outzip):
    for (offset, record, errors) in fh.read_records(limit=None):
        if record and record.type == WarcRecord.RESPONSE and record.content[0] == ResponseMessage.CONTENT_TYPE:
            message = ResponseMessage(RequestMessage())
            leftover = message.feed(record.content[1])
            message.close()

            outzip.writestr(re.sub(r'^https?://', '', record.url), message.get_body())
            print(record.url)
        elif errors:
            print >> sys.stderr, "warc errors at %s:%d"%(name, offset if offset else 0)
            for e in errors:
                print '\t', e


if __name__ == '__main__':
    sys.exit(main(sys.argv))



