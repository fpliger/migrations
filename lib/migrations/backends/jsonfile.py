import os
import json
import csv, codecs, cStringIO
from sqlalchemy import types
import sqlalchemy as sa

from .base import MigratorBase


class Migrator(MigratorBase):
    """ 
    Represent data migration from/to one or more CSV files
    """
    # CSV custom options:
    default_options = dict(MigratorBase.default_options)
    # csv default limiter
    default_options["csv_delimiter"] = ";"
    # csv default file encoding
    default_options["encoding"] = "utf-8"

    
    #def initialize(self):
        #MigratorBase.__init__(self, path, log_cb = None, **options)

        
    def initialize(self):
        """
        Initializes instance database engine and database cached data
        """
        self.done = set()
        if os.path.isdir(self.path):
            self.path_dir = self.path
        else:
            self.path_dir = os.path.dirname(os.path.abspath(self.path))


    def _migrate(self, destination, tables=None, paquet=10000, exclude=None):
        if tables is None:
            # No tables specified so as we are migrating information inside csv
            # files we assume that we can consider all the *.csv files into 
            # self.path_dir as a "table". 
            tables = []
            tables = [ filename.replace(".csv", "") for filename in \
                                   search_files(".csv", self.path_dir, False) ]     

        for table_name in tables:
            self.log_cb("\n\nmigrating %s" % table_name)
            records = []
            table = None
            
            try:

                with open(os.path.join(self.path_dir, '%s.json'%table_name),'rb') as fin:
                    records = json.load(fin)#, default=json_handler)
                    if records:
                        columns = records[0].keys()
                        table = CSVTable(table_name, columns)
                        #reader = UnicodeReader(fin,
                        #                delimiter=self.options["csv_delimiter"],
                        #                encoding=self.options["encoding"])
                        #
                        #self.log_cb('Transferring records')
                        #for i, row in enumerate(reader):
                        #    print i, row
                        #    if i == 0:
                        #        columns = row
                        #
                        #        print "COLUMS", columns, table
                        #    else:
                        #        record = dict(zip(columns, row))
                        #        records.append(record)
                        #
                        #    # Let's check if the records are ready to be dumped (as we
                        #    # don't want it to be bigger to the paquet size set
                        #    if len(records) >= paquet:
                        #        destination.dump(records, table)
                        #        records = []

                        # Finally we have finished looping all the records and just need
                        # to dumpe those left in records
                        destination.dump(records, table)
                    else:
                        print "NO RECORDS"
            except Exception, e:
                #err_msg = u"""Error dumping table [%s] on destination. \
                #Error details: %s"""%(table.name, e.message)
                ##self.exceptions.append(err_msg)
                #self.log_cb(err_msg)
                raise            


    def _dump(self, records, table):
        """
        Dumps all the records into the file named <table.name>.csv into the
        self.path_dir folder. If the the file does not exist a new one will be
        created.
        
        see .base.MigratorBase.dump for further details
        """
        # get the csv file path
        filepath = os.path.join(self.path_dir, "%s.json"%table.name)
        
        transfer_mode = self.options.get(
            "transfer_mode", self.default_options.get("transfer_mode")
        )        
        print "DUMPING", table, records
        tab_stats = self.get_table_stats(table.name)
        if table.name in self.done:
            mode = "a"
        else:
            self.done.add(table.name)
            mode = "w"

        records = [dict(r) for r in records]
        # open the file
        with open(filepath, mode) as jsonfile:
            ## define a csvfile writer that supports unicode
            #writer = UnicodeWriter(csvfile,
            #                    delimiter=self.options["csv_delimiter"],
            #                    encoding=self.options["encoding"])
            #
            ## if it's the first dump called into this table we want to write
            ## also the column names on the first wor of the file
            #if not table.name in self.stats["tables"]:
            #    colnames = [col.name for col in table.columns]
            #    writer.writerow(colnames)
            print "DUMPING....", records

            #json_content = json.dumps(records, default=json_handler)
            json.dump(records, jsonfile, default=json_handler)
            #jsonfile.write(json_content)
            #jsonfile.close()
            #for paquet in records:
            #    row = [value_factory(paquet[colname])  for colname in colnames]
            #    writer.writerow(row)
            
            tab_stats = self.update_dump_stats(tab_stats, records, transfer_mode)
            
        print "DONE"
        #import pdb
        #pdb.set_trace()
        self.log_cb("table dump finished")
        raw_input("done")

def json_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    #elif isinstance(obj, ...):
    #    return ...
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

class CSVTable(object):
    """
	Represent a CSV file as 'table' object with some of the SQAlchemy table
	object attributes that are shared as a common interface for all the Migrator
	objects having to deal with table objects with their dump/migrate methods
	"""
    def __init__(self, name, columns, ):
        self.name = name
        self.columns = [CSVColumn(col) for col in columns]
        
    
    def exists(self, engine):
        """
		Simulates a SQLAlchemy Table.exists method. Receives a SQLAlchemy db
		engine and checks if the database already have a table named as the
		CSV filename binded to the class instance
        """
        return self.name in engine.table_names()
    
    
    @property
    def c(self):
        """
        Simulates the SQLAlchemy table c property. Returns the table columns
        collection
        """        
        return self.columns
    
    
    @property
    def primary_key(self):
        """
        Simulates the SQLAlchemy table primary key property. Returns the table columns
        collection
        """        
        return None


    def create(self, engine):
        """
		Simulates a SQLAlchemy create method. Receives a SQLAlchemy db
		engine and checks if the database already have a table named as the
		CSV filename binded to the class instance
        """
        columns = [sa.Column(col.name, sa.Unicode) for col in self.columns]
        metadata = sa.MetaData()
        table_object = sa.Table(self.name,
                                metadata,
                                *columns)
        
        metadata.create_all(engine)
        
class CSVColumn(object):
    """
	Represent a CSV 'table' 'column' object with some of the SQAlchemy table 
    column object attributes that are shared as a common interface for all the
    Migrator objects having to deal with table objects with their dump/migrate 
    methods
	"""    
    
    def __init__(self, name, type_ = None):
        if type_ is None:
            type_ = types.NullType()
            
        self.name = name
        self.type = type_
        

###############################################
# From the CSV documentation on how to handle with UNICODE strings
# see http://docs.python.org/2/library/csv.html
###############################################
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

##############################
# END OF Unicode Handlers definitions from the Python Docs
##############################

def value_factory(val):
    """
    simple function that handles non supported csv datatypes and
    convert them to str
    """
    if val is None:
        return ""
    elif isinstance(val, basestring):
        return val
    else:
        return str(val)
    
def search_files( file_ext, path = None, recursive=True ):
    """ Search for files matching the regex pattern 
    
    INPUTS:
    
    pattern ::: regex string defining a pattern that must be matched
    
    path ::: path of the folder where to search the files
    
    recursive ::: if True searches recursively also into the path subfolders
    
    
    OUTPUT:
    
    Returns a list of the file paths inside path that matches the regex pattern
    
	>> search_files(["*.csv"], "/")
    ["export.csv", "test.csv"]

	"""
    # If no path is specified we take the current folder
    path = path or os.getcwd()
    nFile =0
    output = []
    
    #if path[-1] != "\\" or path[-1]!="/": path+="/"
    # Loop over all path children
    for item in os.listdir(path):
        print "item", item
        item_path = os.path.join(path, item)
        print item_path, os.path.isfile(item_path), item.endswith(file_ext), file_ext
        if os.path.isfile(item_path):
            # if it's a file...
            if item.endswith(file_ext):
                # ... and matches the pattern I take it
                output.append(item)
            
        elif recursive: 
            # It's not a file and have search for matches into the subfolder
                output += search_files(file_ext, item_path, recursive)
                
    print "returning", output
    return output
