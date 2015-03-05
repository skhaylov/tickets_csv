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
            writer = csv.writer(csvfile, delimiter='|',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for instance in instances:
                writer.writerow(instance.to_csv_row())


class MasterParser(CommonParser):
    date_file = None
    crfn = None
    recorded_borough = None
    doc_type = None
    document_date = None
    document_amount = None
    recorded_date = None
    modified_date = None

    def process(self, row):
        self.unique_key = row[55:76]
        self.date_file = row[17:27]
        self.crfn = row[27:40]
        self.recorded_borough = row[40]
        self.doc_type = row[41:49]
        self.document_date = row[49:59]
        self.document_amount = row[59:76].lstrip('0')
        self.recorded_date = row[76:86]

        if self.document_amount.replace('0', '').strip() == '.':
            self.document_amount = '$-'

        row_copy = row[:]
        row_copy = row_copy.replace(' ', '')
        chunks = row_copy.split('/')[-3:]
        self.modified_date = '/'.join([chunks[0][-2:], chunks[1], chunks[2][:4]])

    def to_csv_row(self):
        return [self.unique_key, 'A', self.date_file, self.crfn,
                self.recorded_borough, self.doc_type, self.document_date,
                self.document_amount, self.recorded_date,
                self.modified_date]


class PartyParser(CommonParser):
    unique_key = None
    party_type = None
    name = None
    address1 = None
    address2 = None
    country = None
    city = None
    state = None
    zip_code = None

    def process(self, row):
        import re

        def clean_text(value):
            value = value.strip()

            if value.startswith('|'):
                value = value[1:]

            if value.endswith('|'):
                value = value[:-1]

            return value

        self.party_type = row[17]
        items = [item.strip() for item in re.split('\s{3}', row)
                 if item.strip()]

        self.name = clean_text(items[0][18:])

        if not items or len(items) < 3:
            return

        i = 1
        self.address1 = clean_text(items[i])

        if len(items) > 4:
            i += 1
            self.address2 = clean_text(items[i])
        else:
            self.address2 = ''


        try:
            i += 1
            country_city = items[i]
            self.country = clean_text(country_city[:2])
            self.city = clean_text(country_city[2:])

            i += 1
            state_zip = items[i]
            self.state = clean_text(state_zip[:-5])
            self.zip_code = clean_text(state_zip[-5:])

        except IndexError:
            # import pdb;pdb.set_trace()
            return

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