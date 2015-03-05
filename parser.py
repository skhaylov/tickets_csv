# coding=utf-8;


__author__ = 'skhaylov'

import sys
import csv


class CommonParser(object):
    unique_key = None

    def process(self, row):
        raise NotImplementedError()

    def to_csv_row(self):
        raise NotImplementedError()

    @classmethod
    def write_to_csv(cls, filename, instances):
        if not isinstance(instances, (list, tuple, set)):
            instances = list(instances)

        with open(filename, 'wb+') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for instance in instances:
                writer.writerow(instance.to_csv_row())


class MasterParser(CommonParser):
    column_c = None
    column_d = None
    column_e = None
    column_f = None
    column_g = None
    column_h = None
    column_i = None
    column_j = None

    def process(self, row):
        self.unique_key = row[55:76]
        self.column_c = row[17:27]
        self.column_d = row[27:40]
        self.column_e = row[40]
        self.column_f = row[41:49]
        self.column_g = row[49:59]
        self.column_h = row[59:76].lstrip('0')
        self.column_i = row[76:86]

        if self.column_h.replace('0', '').strip() == '.':
            self.column_h = '$-'

        row_copy = row[:]
        row_copy = row_copy.replace(' ', '')
        chunks = row_copy.split('/')[-3:]
        self.column_j = '/'.join([chunks[0][-2:], chunks[1],chunks[2][:4]])

    def to_csv_row(self):
        return [self.unique_key, 'A', self.column_c, self.column_d,
                self.column_e, self.column_f, self.column_g, self.column_h,
                self.column_i, self.column_j]


class PartyParser(CommonParser):
    unique_key = None
    party_type = None
    name = None
    address1 = None
    address2 = None
    country = None
    state = None
    zip_code = None

    def process(self, row):
        self.party_type = row[17]

    def to_csv_row(self):
        return [self.unique_key, 'P', self.party_type, self.name,
                self.address1, self.address2, self.country, self.state,
                self.zip_code]


def get_record_type(value):
    return value[16]


PARSERS = {
    'A': MasterParser,
    'P': PartyParser,
}


def run(source):
    unique_key = None

    master_parsers = []
    party_parsers = []

    for row in open(source, 'rb').readlines():
        report_type = get_record_type(row)
        try:
            parser = PARSERS[report_type]()
            parser.process(row)

            if report_type == 'A':
                unique_key = parser.unique_key
                # print unique_key
                master_parsers.append(parser)

            if report_type == 'P':
                parser.unique_key = unique_key
                party_parsers.append(parser)
        except KeyError:
            pass

    if master_parsers:
        MasterParser.write_to_csv('master.my.csv', master_parsers)

    if party_parsers:
        PartyParser.write_to_csv('party.my.csv', party_parsers)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        run(sys.argv[1])