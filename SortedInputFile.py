import itertools as it

class SortedInputFile():
    def __init__(self, filename, key_desc=(), fields_desc=(),fields_names=(), logical_name='noName'):
        self._maxi_key_value = ''
        self._filename = filename
        self._key_description = key_desc
        self._fields_desc = fields_desc
        self._fields_names = fields_names
        self._name = logical_name
        self.parser = None
        if len(fields_desc) != len(fields_names):
            raise UnmatchedNamesToDescriptionsException(f'Descriptions and Names ne correspondent pas\nnames ({len(fields_names)}): {fields_names}\ndescriptions ({len(fields_desc)}): {fields_desc}' )
        elif len(fields_desc) > 0:
            self.parser = self.make_record_parser()

        # compteurs
        self._read_count = 0
        self._keys_count = 0
        self._maxi_found = 0
        self._nb_occurs = 0

    def __enter__(self):
        self.file = open(self.filename)
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
        self.print_resume()

    def print_resume(self):
       print(f"FERMETURE fichier {self.filename}")
       print(f" - Nombre de lecture :        {self.read_count:>10}")
       print(f" - Nombre de clefs trouvées : {self.key_count:>10}")
       print(f" - Maxi par clef :            {self.maxi:>10} ( clef: {self.maxi_key_value})")

    def readline(self):
        return self.file.readline()

    def iterator(self, sentinel=''):
        return iter(self.file.readline, sentinel)

    def extract_key(self, data):
        key = ''
        for key_item in self.key_description:
            key += data[key_item[0]: key_item[1]]
        return key
    def mapping(self, data):
        if len(self._fields_names) > 0:
            return dict(zip(self._fields_names, self.parser(data)))
        else:
            return data

    def process_iterator(self):
        data = next(self.iterator())
        key = self.extract_key(data)
        chunk = Chunk(key, self.filename)
        chunk.append(self.mapping(data))
        self._read_count += 1

        for line_num, data in enumerate(self.iterator()):
            key_prev = key
            key = self.extract_key(data)
            if key < key_prev:
                raise WrongKeyException(
                    f"Fichier {self.filename} non trié ligne {line_num + 2} key:{key} < {key_prev}")
            elif key != key_prev:
                # a rupture de cle on renvoie le 'chunk'

                self._nb_occurs = len(chunk.data)
                if self.nb_occurs > self.maxi:
                    self.maxi_key_value = chunk.key

                self.maxi = len(chunk.data)
                self._keys_count += 1
                yield chunk

                # et on commence a preparer le suivant
                chunk = Chunk(key, filename=self.filename)
            chunk.append(self.mapping(data))
            self._read_count += 1
        self._nb_occurs = len(chunk.data)
        self.maxi = len(chunk.data)
        self._keys_count += 1
        yield (chunk)  # last chunk

    @property
    def read_count(self):
        return self._read_count

    @property
    def key_count(self):
        return self._keys_count

    @property
    def nb_occurs(self):
        return self._nb_occurs

    @property
    def maxi(self):
        return self._maxi_found

    @maxi.setter
    def maxi(self, v):
        self._maxi_found = max(self._maxi_found, v)
    @property
    def maxi_key_value(self):
        return self._maxi_key_value
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        self._name = v

    @property
    def filename(self):
        return self._filename

    @property
    def key_description(self):
        return self._key_description

    @property
    def fields_description(self):
        return self._fields_desc

    @property
    def mode(self):
        return 'r'

    @maxi_key_value.setter
    def maxi_key_value(self, value):
        self._maxi_key_value = value

    def make_record_parser(self):
        cuts = tuple(cut for cut in it.accumulate(abs(fw) for fw in self.fields_description))
        pads = tuple(fw < 0 for fw in self.fields_description)  # bool values for padding fields
        flds = tuple(it.zip_longest(pads, (0,) + cuts, cuts))[:-1]  # ignore final one
        parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
        # optional informational function attributes
        parse.size = sum(abs(fw) for fw in self.fields_description)
        parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                                   for fw in self.fields_description)
        return parse


class Chunk():
    """
    agregates a key value with the list of corresponding lines
    """

    def __init__(self, key_value='', filename='noname'):
        self.key = key_value
        self.data = list()
        self.filename = filename

    def append(self, data):
        self.data.append(data)

    def __repr__(self):
        return dict({'filename': self.filename, 'key': self.key, 'data': self.data}).__repr__()


class WrongKeyException(Exception):
    pass

class UnmatchedNamesToDescriptionsException(Exception):
    pass