"""
Migrator Base Classes and Functions
"""
from migrations import reporting
from migrations import loggers

class MigratorBase(object):
    """ 
    Represent a migration actor (source or destination)
    
    """
    default_options = {
        "transfer_mode": "FULL",  # Available: FULL, DIFF
        "compare_mode": "PK",   # Available: FULL, PK
        }

    def __init__(self, path, log_cb = None, **options):
        """
        Constructor
        
        path ::: a string that indicates a data source/destination URL. The 
                string form of the URL is:
                <backend>:::<backend-URL-string>
                
                WHERE
                
                <backend> is a supported backend (a module of the .backends
                package). For instance: dbms, csvfile, jsonfile, ..
                
                <backend-URL-string> is a connection string or URL specific for
                the selected backend. For instance:
                
                dbms backend accepts a sqlalchemy connection string or a 
                direct odbc connection string using the prefix "creator://"
                [for further details check the dbms module documentation],
                csvfile or jsonfile accept a string path pointing to a csv 
                file or folder
                
                
        echo_cb ::: callback function called with messages during the code 
                    execution

        OPTIONS:

        transfer_mode ::: string defining the transfer mode. Available modes:

                        * FULL -> (DEFAULT) Transfer all data without any check
                        * DIFF -> Transfer only data that is not alread into the
                                  the destination
                                  
        compare_mode ::: string defining the transfer mode. Available modes:
        
                        * CACHED_PK -> (DEFAULT) When transfer_mode==DIFF checks if the
                        data is not into the destination DB by checking only the
                        primary key (FASTEST MODE)
                        
                        * FULL_CACHE -> When transfer_mode==DIFF checks if the data is
                        not into the destination DB by loading (pre-caching) all
                        the destination records in memory and checking all the
                        record values
                                NOTE: This method could load really huge datasets
                                to memory! Be aware of that before using it.
                                
                        * FULL_CACHE_NO_PK -> When transfer_mode==DIFF checks if the data is
                        not into the destination DB by loading (pre-caching) all
                        the destination records in memory and checking all the
                        record values excluding the primary key value. 
                        This mode is designed for tables with an autoincrement 
                        pk
                                NOTE: This method could load really huge datasets
                                to memory! Be aware of that before using it.
                                
                        * FULL -> When transfer_mode==DIFF checks if the 
                        data is not into the destination DB by checking all the
                        row values of the row excluding the primary key value. 
                        This mode is designed for tables with an autoincrement 
                        pk
                                NOTE: This method could load really huge datasets
                                to memory! Be aware of that before using it.
                                
                        * FULL_NO_PK -> When transfer_mode==DIFF checks if the
                        data is not into the destination DB by checking all the
                        row values of the row excluding the primary key value.
                        This mode is designed for tables with an autoincrement
                        pk
                                NOTE: This method could load really huge datasets
                                to memory! Be aware of that before using it.

        """
        self.path = path
        self.options = dict(self.default_options)
        self.options.update(options)

        if not callable(log_cb):
            self.log_cb = loggers.default_logger
        else:
            self.log_cb = log_cb
            
        # last migration statistics dictionary
        self.stats = {"tables": {}}
        
        # call extra initialization hook
        self.initialize()
        
        
    def initialize(self):
        """ 
        Initialize the Migrator. It's automatically called during the Migrator
        instantiation. If override it can be used as hook for post instantiation
        operations
        """
        pass


    def migrate(self, destination, tables=None, paquet=10000, exclude=None):
        """
        Migrates tables data between the source and the destination.
        
        INPUTS:
        
        destination ::: Migrator object that implements a dump method that 
                        receives table records and dump them into a destination
                        backend
        
        tables ::: a sequence of the strings where every string specifies the
                    name of a table with data records to be trasferred from
                    self.source to self.destination
                    
                    IF tables == None all the source tables will be collected
                    and transferred
                    
        paquet ::: integer that specifies the lenght of the maximum records to
                    be transferred on every transferring cycle. This parameter
                    is important when dealing with big that tables in order to
                    avoid loading big chunks of data that could overload system
                    memory. 
                    I.e, if a table have 1.000.000.....000 records loading all 
                    the data to be transferred all in once would be a bad idea.
                    Setting the paquet to 10000 means that all the tabel data
                    will be transferred looping 10000 records at time.
                    
        exclude ::: a sequence of the strings where every string specifies the
                    name of a table that will be excluded and it's data will
                    not be transferred.
                    
                    IF exclude == None no table will be excluded
        """        
        self.init_migration(destination)
        return self._migrate(
                    destination,
                    tables = tables, 
                    paquet = paquet, 
                    exclude = exclude
        )


    def _migrate(self, destination, tables=None, paquet=10000, exclude=None):
        """
        Overwrite me to define Migrator migration source behaviour
        """
        raise NotImplementedError


    def dump(self, records, table):
        """
        Receive a collections of data records to be dumped into the migrator
        table backend.
        
        records ::: sequence of data records to be transferred to the Migrator
                    data destination path (self.path)
                    
        table ::: table object that is being dumped and to which records have
                    been collected from
        """
        return self._dump(records, table)


    def _dump(self, paquets, table):
        """
        Overwrite me to define Migrator migration destination behaviour
        """        
        raise NotImplementedError
    
    
    def compare(self, destination, tables, paquet=10000, exclude=None):
        return self._compare(
            destination,
            tables = tables, 
            paquet = paquet, 
            exclude = exclude
        )
    
    
    def _compare(self, destiantion, tables, paquet=10000, exclude=None):
        raise NotImplementedError


    def init_migration(self, destination):
        """
        Initializes the migration stats reseting all the statistics and sets
        destination.stats = self.stats
        """
        destination.stats = self.stats = {
            "transfer_mode": self.options["transfer_mode"],
            "source": self.path,
            "destination": destination.path,
            "tables":{},
            "tables_graph": {},
            "exceptions": [],
            "messages" : [],
            "total_records_transferred": 0,
            "total_records_skipped": 0,
        }


    def report_last_migration(self, filepath, templatepath=None):
        """ 
        Generates a statistics report of the last migration performed
        """
        reporting.html(filepath, self.stats, templatepath=None)
        
    
    def get_table_stats(self, table_name):
        """ returns the table migration statistics """
        if not table_name in self.stats["tables"]:
            self.stats["tables"][table_name] = get_table_default_stats(table_name)

        return self.stats["tables"][table_name]

    def update_dump_stats(self, tab_stats, records, transfer_mode):
        if transfer_mode == "DIFF":
            tab_stats["lst_records_transferred"] += records        
        tab_stats["records_transferred"] += len(records)
        self.stats["total_records_transferred"] += len(records)
        self.stats["total_records_skipped"] += tab_stats["records_skipped"]
        msg = "%s paquets transferred/skipped %s, %s --- %s / %s" % (
            tab_stats["name"],
            tab_stats["records_transferred"],
            tab_stats["records_skipped"],
            self.stats["total_records_skipped"],
            self.stats["total_records_transferred"]
        )
        self.log_cb(msg)

        return tab_stats
    
    
def get_table_default_stats(table_name):
    """ Returns a table default statistics dictionary"""
    return {"pk_map":{},
            "skipped": False,
            "records_transferred": 0,
            "records_skipped": 0,
            "total_records": 0,
            "exceptions": [],
            "messages": [],
            "lst_records_transferred": [],
            "name": table_name,
            }