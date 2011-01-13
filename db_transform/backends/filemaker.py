from db_migration.backends import MigrationBackend

from StringIO import StringIO
from xml.sax.saxutils import unescape
import xml.sax.handler
import xml.sax
import codecs


class FileMakerProParseLimitExceededError(xml.sax.SAXException):
    def get_message(self):
        return u"The specified number of records have been parsed"

class FileMakerProContentHandler(xml.sax.handler.ContentHandler):
    """
    A simple xml.sax.handler.ContentHandler to load exported data created
    by FileMaker Pro (v6).

    """
    def __init__(self, parse_limit=0):
        self.fields = []
        self.data = []

        self.parse_limit = parse_limit
        self.colctr = 0
        self.rowctr = 0

        self.col_name = None
        self.col_type = None

        self.row = None
        self.content = ''

    def endDocument(self):
        print "Loaded %d records." % len(self.data)

    def startElement(self, name, attrs):
        if name == "DATABASE":
            print "Reading XML export of database '%s' containing %d records." % \
                (attrs.get('NAME'),long(attrs.get('RECORDS')))
        elif name == "FIELD":
            self.fields.append( (attrs.get('NAME'),attrs.get('TYPE')) )
        elif name == "RESULTSET":
            print "Document contains %d records" % ( long(attrs.get('FOUND')) )
        elif name == "ROW":
            # self.row = {'_source':u'%s'%(attrs.items())}
            self.row = {}
            self.colctr = 0
        elif name == "DATA":
            self.col_name = self.fields[self.colctr][0]
            self.col_type = self.fields[self.colctr][1]
            self.in_data = True

    def characters(self, content):
        for c,r in REMOVE_CHARS:
            content = content.replace(c,r)
        self.content = ''.join([ self.content,content ])

    def endElement(self, name):
        if name == "ROW":
            self.data.append( self.row )
            self.rowctr += 1

            if self.parse_limit and self.rowctr > self.parse_limit:
                raise FileMakerProParseLimitExceededError( \
                    "Finished processing (%d records)" % (self.rowctr)
                    )
        elif name == "COL":
            self.colctr += 1
        elif name == "DATA":
            try:
                self.row[self.col_name] = self.content
            except ValueError:
                print "Encountered improper value for col %s on row %d: %s" % \
                    (self.col_name, self.rowctr, self.row)
            self.content = ''

REMOVE_CHARS = [
    ('\x0b',''),
    ('\x0c',''),
]

# class FileMakerProMigrationBackend(MigrationBackend):
class Backend(MigrationBackend):
    """ """
    def __init__(self, max_records, **kwargs): 
        # super(FileMakerProMigrationBackend,self).__init__(max_records)
        super(Backend,self).__init__(max_records)
        content_handler_cls = kwargs.pop('content_handler_cls',FileMakerProContentHandler)
        self.content_handler = content_handler_cls(parse_limit=self.max_records)

    def parse(self, **kwargs):
        datafile = kwargs.pop('datafile')
        fp = open(datafile, mode='rU')
        buf = fp.read()
        fp.close()

        for c,r in REMOVE_CHARS:
            buf = buf.replace( c,r )

        try:
            xml.sax.parseString( buf, self.content_handler )
        except FileMakerProParseLimitExceededError, e:
            print str(e)

    def get_data(self, **kwargs):
        return self.content_handler.data

    def get_fields(self, **kwargs):
        return self.content_handler.fields
