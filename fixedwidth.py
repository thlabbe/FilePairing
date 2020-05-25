from itertools import accumulate
from itertools import zip_longest


def make_record_parser(fieldwidths):
    cuts = tuple(cut for cut in accumulate(abs(fw) for fw in fieldwidths))
    pads = tuple(fw < 0 for fw in fieldwidths)  # bool values for padding fields
    flds = tuple(zip_longest(pads, (0,) + cuts, cuts))[:-1]  # ignore final one
    parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
    # optional informational function attributes
    parse.size = sum(abs(fw) for fw in fieldwidths)
    parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                               for fw in fieldwidths)
    return parse

def map_names(a_tuple, names):
    return tuple(zip(a_tuple, names))

def process(file_description: dict):
    parse = make_record_parser(file_description['fieldswith'])
    with open(file_description['filename'], 'r') as f:
        for line in iter(f.readline, ""):
            yield (map_names(parse(line), file_description['fieldsnames']))
    # for line in lines:
    #     fields = parse(line)
    #     print(f'{line!r} => {fields}')


line = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n'
line = '1234500002TITI\n'
fieldwidths = (2, -10, 24)  # negative widths represent ignored padding fields
fieldwidths = (5,5, 4)
parse = make_record_parser(fieldwidths)
fields = parse(line)
print('format: {!r}, rec size: {} chars'.format(parse.fmtstring, parse.size))
print(f'format {parse.fmtstring!r}, record size {parse.size} chars')
print('fields: {}'.format(fields))

fileA = {
    'filename': './data/test2.txt',
    'fieldswith': (5, 5, 4),
    'fieldsnames': ('number-a', 'identifier', 'label')
}
for i, l in enumerate(process(fileA)):
    print(i, ': ', l)