# -*- coding: utf-8 -*-
# migrations/migrator.py
# Copyright (C) 2013 Fabio Pliger
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Provide 'migration' generic feature that can be produce data migrations from
a 'source' and a 'destination' that implements the migrations.backends.base.MigratorBase
interface.
"""

import getopt
import sys
import backends
import importlib

from functools import partial


def get_migrator(path, **options):
    backend, path = path.split(":::")
    backend = importlib.import_module("migrations.backends.%s" % backend.strip())
    return backend.Migrator(path, **options)

class Migration(object):
    """
    Migration class that handles data migrations between a source and a \
    destination
    """
    def __init__(self, source, destination, log_cb = None, **options):
        """
        Initializes a new Migration class.
        
        INPUTS:

            source ::: connection string to the source (see the documenation \
                for more information about migrations connection strings)
                
            destination ::: connection string to the destination (see the
                documenation for more information about migrations connection \
                strings)
                
                
            log_cb ::: function that will be called to log messages during the
                        data trasfer execution. The function will be called with
                        a single string parameter (the message of the operation
                        that is being executed). The module migrations.loggers
                        already defines common usage convenient logging 
                        functions.
                        
                        If log_cb == None the migrations.loggers.default_logger
                        will be used.
                
            options ::: a set of options that defines and modifies the behaviour
                        of the data migration. Available options:
                        
                        - transfer_mode  "DIFF"|"FULL"
                                
                                DIFF -> check the destination and only transfer 
                                        records that are not already present
                                        
                                FULL -> no check is performed and all the source
                                        data records are dumped to the 
                                        destination
                                        
                        
            
        """
        self.options = options
        self.source = get_migrator(source, log_cb = log_cb, **self.options)
        self.destination = get_migrator(destination,
                                         log_cb = log_cb, 
                                         **self.options)
        self._last_migration_args = []


    def migrate(self, tables=None, paquet=10000, exclude = None):
        """
        Migrates tables data between the source and the destination.
        
        INPUTS:
        
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
        # set the Migration statistics to the source stats which is already
        # shared with the destination (if the source and the destination 
        # objects implement the migration.backends.BaseMigrator interface)
        self.stats = self.source.stats
        
        # save last migration arguments if needed later
        self._last_migration_args = {"tables": tables, 
                                     "exclude": exclude,
                                     "paquet": paquet}
        
        # start the migration from source to destination
        self.source.migrate(self.destination,
                            tables = tables,
                            paquet = paquet,
                            exclude = exclude)


    def report_last_migration(self, filepath, templatepath=None):
        """ 
        Generates a statistics report of the last migration performed
        delegating the report generation to self.source
        """
        return self.source.report_last_migration(filepath, templatepath)
        
        
    
    def check_last_migration(self):
        """
        Compares the source and destination migration to check if all the data
        have been migrated correctly
        """
        return self.source.compare(self.destination, **self._last_migration_args)



def print_usage():
    args = {"arg":sys.argv[0]}
    print """
    Usage: %{arg}s -v -f source_server -t destination_server table [table ...]

    WHERE:
    -f, -t = driver://user[:password]@host[:port]/database
    -v = verbose mode

    Examples:

    %{arg}s -f oracle://someuser:PaSsWd@db1/TSH1 \\
    -t mysql://root@db2:3307/reporting table_one table_two

    or

    Example: %{arg}s -v -f oracle://someuser:PaSsWd@db1/TSH1 \\
    -t "creator://Driver={SQL Server Native Client 10.0};\
        Server=localhost;Database=DBNAME;UID=USERNAME;PWD=pwd;"
    """ % (args,)


def default_logger(msg):
    print msg

if __name__ == '__main__':
    # TODO: Move all this to the tools package
    optlist, intables = getopt.getopt(sys.argv[1:], 'f:t:ve')

    intables = [tab for tab in intables if tab]

    options = dict(optlist)
    if '-f' not in options or '-t' not in options:
        print_usage()
        raise SystemExit, 1

    verbose = "-v" in options
    verbose_sql = "-e" in options
    
    migrator = Migration( options['-f'],
                          options['-t'],
                          intables,
                          echo_cb={True: default_logger}.get(verbose, None)
                          )

    if exceptions:
        print "\n\n\n------------------------------------"
        print len(exceptions), "exceptions occurred"
        print "------------------------------------"

        for exc in exceptions:
            print exc
            print "-----"
