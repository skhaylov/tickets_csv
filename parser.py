# coding=utf-8;


__author__ = 'skhaylov'

import re
import csv


split_row = lambda row: [s.strip() for s in re.split('\s{3}', row) if s.strip()]
get_record_type = lambda value: value[16]

COLUMN_NAMES = {
    'A': ["Unique_Key",
          "Record_Type",
          "Date_File",
          "CRFN",
          "Recorded_Borough",
          "Doc_type",
          "Document_date",
          "Document_amount",
          "Recorded_date",
          "Modified_date"],

    'P': ["Unique_Key",
          "Record_Type",
          "Party_type",
          "Name",
          "Address_1",
          "Address2",
          "Country",
          "City",
          "State",
          "Zip"],

    'L': ["Unique_Key",
          "Record_Type",
          "Borough",
          "Block",
          "Lot",
          "Easement",
          "Partial_Lot",
          "Air_Rights",
          "Subterranean_Rights",
          "Property_Type",
          "Street_number",
          "Address",
          "Unit"],
    }


def write_to_csv(filename, rows, rows_heads):
    if not isinstance(rows, (list, tuple, set)):
        rows = list(rows)

    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter='|',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)

        writer.writerow(rows_heads)

        for row in rows:
            writer.writerow(row)


def parse_master_row(row, unique_key=None):
    # Unique_key arg NOT USED!
    unique_key = row[55:76]
    date_file = row[17:27]
    crfn = row[27:40]
    recorded_borough = row[40]
    doc_type = row[41:49]
    document_date = row[49:59]
    document_amount = row[59:76].lstrip('0')
    recorded_date = row[76:86]

    if document_amount.replace('0', '').strip() == '.':
        document_amount = '$-'

    row_copy = row[:]
    row_copy = row_copy.replace(' ', '')
    chunks = row_copy.split('/')[-3:]
    modified_date = '/'.join([chunks[0][-2:], chunks[1], chunks[2][:4]])

    return [unique_key, 'A', date_file, crfn, recorded_borough, doc_type,
            document_date, document_amount, recorded_date, modified_date]


def parse_party_row(row, unique_key):
    def clean_text(value):
        value = value.strip()

        if value.startswith('|'):
            value = value[1:]

        if value.endswith('|'):
            value = value[:-1]

        return value

    party_type = row[17]
    items = split_row(row)

    name = clean_text(items[0][18:])

    if not items or len(items) < 3:
        return

    i = 1
    address1 = clean_text(items[i])

    if len(items) > 4:
        i += 1
        address2 = clean_text(items[i])
    else:
        address2 = ''

    try:
        i += 1
        country_city = items[i]
        country = clean_text(country_city[:2])
        city = clean_text(country_city[2:])

        i += 1
        state_zip = items[i]
        state = clean_text(state_zip[:-5])
        zip_code = clean_text(state_zip[-5:])

    except IndexError:
        return

    return [unique_key, 'P', party_type, name,
            address1, address2, country, city, state,
            zip_code]


def parse_lot_row(row, unique_key):
    items = split_row(row)

    borough = items[0][17]
    block = items[0][18:23].lstrip('0')
    lot = items[0][23:27].lstrip('0')
    easement = items[0][27]
    partial_lot = items[0][28]
    air_rights = items[0][29]
    subterranean_rights = items[0][30]
    property_type = items[0][31:33]
    street_number = items[0][33:]

    address, unit = '', ''

    try:
        address = items[1]
        if items[2].strip().lower() == 'street':
            address += ' ' + items[2]
        else:
            unit = items[2]
    except IndexError:
        pass

    return [unique_key, 'L', borough, block, lot,
            easement, partial_lot, air_rights,
            subterranean_rights, property_type,
            street_number, address, unit]


PARSERS = {
    'A': (parse_master_row, 'master.my.csv'),
    'P': (parse_party_row, 'party.my.csv'),
    'L': (parse_lot_row, 'lot.my.csv'),
}


def run(source_file):
    unique_key = None

    master_parsed_rows = []
    party_parsed_rows = []
    lot_parsed_rows = []

    for row in open(source_file, 'r').readlines():
        report_type = get_record_type(row)
        parser_list = None

        try:
            parse_func = PARSERS[report_type][0]
            result = parse_func(row, unique_key)

            if not result:
                continue

            if report_type == 'A':
                unique_key = result[0]
                parser_list = master_parsed_rows

            if report_type == 'P':
                parser_list = party_parsed_rows

            if report_type == 'L':
                parser_list = lot_parsed_rows

            parser_list.append(result)
        except KeyError:
            pass

    write_lists = (
        (PARSERS['A'][1], master_parsed_rows, COLUMN_NAMES['A']),
        (PARSERS['P'][1], party_parsed_rows, COLUMN_NAMES['P']),
        (PARSERS['L'][1], lot_parsed_rows, COLUMN_NAMES['L']),
    )

    for item in write_lists:
        rows = item[1]
        filename = item[0]
        rows_heads = item[2]

        if rows:
            write_to_csv(filename, rows, rows_heads)


if __name__ == '__main__':
    run('3.3.2015.txt')
