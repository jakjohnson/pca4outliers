#!/usr/bin/env python

import sys
import os
import math

sys.path.append(
    os.path.join(os.environ['SPLUNK_HOME'], 'etc', 'apps',
                 'searchcommands_app', 'lib'))
from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, validators
from collections import defaultdict
from __future__ import absolute_import, division, print_function, \
    unicode_literals

@Configuration(requires_preop=True)
class EntropyCommand(ReportingCommand):
    @Configuration()
    def map(self, records):
        # TODO configure
        key = "Bin"
        fieldnames = self.fieldnames
        frequencies = {}
        for record in records:
            for field in fieldnames:
                if record[key] not in frequencies:
                    frequencies[record[key]] = {}
                if field not in frequencies[record[key]]:
                    frequencies[record[key]][field] = defaultdict(int)

                frequencies[record[key]][field][record[field]] += 1
                frequencies[record[key]][field]['Sum'] += 1

        yield frequencies

    def reduce(self, records):
        total = {}
        for record in records:
            for group in record.keys():
                if group not in total:
                    total[group] = {}

                for field in record[group].keys():
                    if field not in total[group]:
                        total[group][field] = {}

                    for value in record[group][field].keys():
                        if value not in total[group][field]:
                            total[group][field][value] = defaultdict(int)
                        else:
                            total[group][field][value] += total[group][field][
                                value]

        for group in total.keys():
            for field in total[group].keys():
                total = 0
                for value in total[group][field].keys():
                    if value == 'Sum':
                        continue
                    frequency = total[group][field][value] / total[group][
                        field]["Sum"]
                    total += frequency * math.log(frequency, 2)
                total[group][field] = -total

        yield total


dispatch(EntropyCommand, sys.argv, sys.stdin, sys.stdout, __name__)
