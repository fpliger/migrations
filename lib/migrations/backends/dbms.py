import operator
import copy
import re
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import mssql

from .base import MigratorBase

class Migrator(MigratorBase):
    """ 
    Represent data migration from/to a SQL Database
    """    
        
    def initialize(self):
        """
        Initializes instance database engine and database cached data
        """
        self.engine = build_engine(self.path)
        
        # Dict to save destination data in case of diff dumpings
        self._db_data = {}        


    def _migrate(self, destination, tables=None, paquet=10000, exclude=None):
        """
        Receives all inputs as described in .base.MigratorBase.migrate,
        gets the tables data from the instance binded database to and deliver
        it to the destination.dump method
        """
        if exclude is None:
            exclude = []

        # some boilerplate code to create the source and destinations sessions
        source = sessionmaker(bind=self.engine)()
        meta = MetaData()

        self.exceptions = []  # We can save the exections for debug reasons

        # Now we should have a clear view of what are the links and constraints
        # So we can define the order of the data dumping
        tables_order = self.get_tables_dump_order(tables)

        # Now we can look over the tables in the right order to avoid having
        # FKs troubles
        source = sessionmaker(bind=self.engine)()

        for table_name in tables_order:
            tab_stats = self.get_table_stats(table_name)

            if exclude:
                exclusions_regex = "(" + ")|(".join(exclude) + ")"
                matches = re.match(exclusions_regex, table_name)
                if matches: #table_name in exclude:
                    msg = "skipped table %s   as requested on %s" % (table_name,
                                                                     exclude)
                    self.log_cb(msg)
                    self.stats["messages"].append(msg)
                    tab_stats["messages"].append(msg)
                    tab_stats["skipped"] = True
                    self.stats["tables"][table_name] = tab_stats
                    continue

            self.log_cb("\n\nmigrating %s" % table_name)
            table = Table(table_name, meta, autoload=True,
                          autoload_with=self.engine)

            # Let's prepare the sql results
            result = source.execute(sa.select([table]))
            self.log_cb('Transferring records')

            # Let's start transfering the data dividing it in paquet sized
            # chuncks
            done = False
            while not done:
                try:
                    # Let's fetch the chunks from the prepared sql select
                    paquets = result.fetchmany(paquet)
                    self.log_cb("records to transfer: %s" % len(paquets))

                    if len(paquets) == 0:
                        done = True
                    else:
                        destination.dump(paquets, table)

                except Exception, e:
                    err_msg = u"""Error dumping table [%s] on destination. \
                    Error details: %s"""%(table.name, e.message)
                    self.exceptions.append(err_msg)
                    self.log_cb(err_msg)
                    raise

        del source
        return self.exceptions
    
    
    def _compare(self, destination, tables=None, paquet=10000, exclude=None):
        
        # If not tables specified we grab all tables from the source
        if not tables:
            tables = self.engine.table_names()
            
        if exclude is None:
            exclude = []

        # some boilerplate code to create the source and destinations sessions
        source = sessionmaker(bind=self.engine)()
        meta = MetaData()

        # Now we should have a clear view of what are the links and constraints
        # So we can define the order of the data dumping
        tables_order = self.get_tables_dump_order(tables)

        
        out = {}  # output dictionary

        conn = self.engine.connect()
        for table_name in tables_order:
            #tab_stats = self.get_table_stats(table_name)
            out[table_name] = []
            
            if exclude:
                exclusions_regex = "(" + ")|(".join(exclude) + ")"
                matches = re.match(exclusions_regex, table_name)
                if matches: #table_name in exclude:
                    msg = "skipped table %s   as requested on %s" % (table_name,
                                                                     exclude)
                    self.log_cb(msg)
                    continue

            #self.log_cb("\n\nmigrating %s" % table_name)
            table = Table(table_name, meta, autoload=True,
                          autoload_with=self.engine)
            
            fk_mappings = self.get_table_fks_mapping(table
                                                     )
            # Let's prepare the sql results
            result = conn.execute(sa.select([table]))
            self.log_cb('checking records %s' % table.name)

            # Let's start transfering the data dividing it in paquet sized
            # chuncks
            done = False
            records_checked = 0
            while not done:
                try:
                    # Let's fetch the chunks from the prepared sql select
                    paquets = result.fetchmany(paquet)
                    self.log_cb("records to check: %s" % len(paquets))
                    out[table_name] = skip_records_full_in_memory(paquets, table,conn)
                    records_checked += len(paquets)
                    self.log_cb("checked: %s" % records_checked)
                    #destination.dump(paquets, table)
                    if len(paquets) == 0:
                        done = True

                except Exception, e:
                    err_msg = u"""Error dumping table [%s] on destination. \
                    Error details: %s"""%(table.name, e.message)
                    self.exceptions.append(err_msg)
                    self.log_cb(err_msg)
                    raise

        del source
        conn.close()
        return out
            
        
        
    def _dump(self, records, table):
        """ Dumps all the records into the table on the instance binded
        DB all in one block
        
        see .base.MigratorBase.dump for further details
        """
        meta = MetaData()

        # First let's be sure that we have the table on our database
        self.check_table(table)
        tab_stats = self.get_table_stats(table.name)
        
        if table.exists(self.engine) and records:
            table = Table(table.name, meta, autoload=True, autoload_with=self.engine)

            dest = self.engine.connect()

            pk = table.primary_key
            pk_col = None
            if pk is not None and len(pk.columns):
                pk_col = list(pk.columns)[0]

            # Let's check the trasfer mode and clean some data if needed
            transfer_mode = self.options.get(
                "transfer_mode", self.default_options.get("transfer_mode")
            )


            # ----------------------------------------------------------------
            # We need to check if this tables has foreign keys that point to
            # other tables that have had their PKs re-mapped!
            # If this is the case we need to re-map the rows kf keys to the new
            # ones
            fk_mappings = self.get_table_fks_mapping(table)
            

            # in case of a "DIFF" transfer mode we need to take only the records
            # that are not in the destination            
            records = self.prepare_records(
                records,
                table,
                pk_col,
                tab_stats,
                fk_mappings,
                dest
            )
            

            # ----------------------------------------------------------------
            # we need to check if this tables has a autoincrement numerical
            # primary key and in this case pop it from the paquets I'm dumping
            # then I have to re-map the old keys with the new ones after the
            # insert statement
            records_id = []

            if is_numeric_column(pk_col) and pk_col.autoincrement:
                _records = []
                for row in records:
                    if row:
                        row = dict(row)
                        records_id.append(row.pop(pk_col.name))
                        _records.append(clean_record(table, row))
            else:
                _records = [clean_record(table, paq) for paq in records if paq]


            if _records:
                try:
                    res = dest.execute(table.insert(), _records)
                except sa.exc.IntegrityError, e:
                    # In this case the table primary key was marked as autoincrement but
                    # not on the server side.... So we can try to insert with the old
                    # values
                    if "%s.%s may not be NULL"%(table.name, pk_col.name) in e.message:
                        res = dest.execute(table.insert(), records)
                    self.log_cb("UNHANDLED ERROR %s: %s" %(e, e.message))

                if is_numeric_column(pk_col) and pk_col.autoincrement:
                    pk_map = tab_stats["pk_map"]
                    try:
                        last_ids = res.last_inserted_ids()
                    except AttributeError:
                        # SQLite may not help sometimes... :(
                        # So let's try to get the last inserted ids directly from
                        # the database
                        stmnt = sa.select([pk_col]).limit(len(_records)).order_by(pk_col.desc())
                        last_ids = reversed(dest.execute(stmnt).fetchall())
                        for i, last_id in enumerate(last_ids):
                            pk_map[records_id[i]] = last_id[0]

                    tab_stats["pk_map"] = pk_map

            tab_stats = self.update_dump_stats(tab_stats, _records, transfer_mode)

            dest.close()

        self.stats["tables"][table.name] = tab_stats


    def get_tables_dump_order(self, tables):
        """
        Connects with database binded to the migrator instance and defines
        a 'safe' data dump tables order resolving tables cross reference 
        issue that might come from tables relations and foreign keys definition
        
        INPUTS: 
        
        tables ::: list of the table names (strings) that will be copied
        
                (IF NOT TABLE IS SPECIFIED ALL TABLES DATA WILL BE DUMPED)
                
        OUTPUT:
        
        list of the table names (strings) in a safe order
        """
        source = sessionmaker(bind=self.engine)()
        meta = MetaData()

        # graph that stores tables dependencies information
        autoincrement_tables = {}  # Tables witu an autoincremental pk

        # Lists all the tables that are linked by another tables pointing to them
        fks_links = {}

        # Lists all the tables that have a foreign key linking to other tables
        tables_fks = {}

        # If not tables specified we grab all tables from the source
        if not tables:
            tables = self.engine.table_names()

        # First we need to loop over the tables to check tables links and
        # relations
        tlb_msg = "checking table %s already exists in destination database %s"
        for table_name in tables:
            self.log_cb(u'Processing %s'%table_name)
            self.log_cb(u'Pulling schema from source server')

            table = Table(table_name,
                          meta,
                          autoload=True,
                          autoload_with=self.engine)

            # We first need to check the table PKs to track auto-incremental
            # keys and deal with them..
            pks = list(table.primary_key.columns)
            autoincrement_pk = False

            self.log_cb(u'checking table Primary Keys...')
            for pk in pks:
                if len(pks) > 1 and pk.autoincrement:
                    self.exceptions.append(
                        "Composite PKs with an autoincremental column \
                        is not supported (YET)")
                elif pk.autoincrement:
                    autoincrement_pk = True

            fks = list(table.foreign_keys)
            for fk in fks:
                tab_links = fks_links.get(fk.column.table.name, {})
                tab_links_rev = tables_fks.get(table.name, {})
                for column in fk.constraint.columns:
                    tab_links[fk.column.name] = (table.name, column)
                    tab_links_rev[column] = (fk.column.table.name, fk.column.name)

                fks_links[fk.column.table.name]  = tab_links
                tables_fks[table.name]  = tab_links_rev



        tables_order = [] # Precedence list of tables

        # Now let's add all the tables that have no links to others
        tables_w_links = set(fks_links.keys()).union(
            set(tables_fks)
        )

        # and the tables that are linked by others by have no FKs
        first_order_tables = list(set(fks_links.keys()).difference(
            set(tables_fks)))


        tables_order += first_order_tables
        tables_order += list(set(tables).difference(tables_w_links))

        fks_links = dict([(k, fks_links[k]) for k in \
                          list(set(fks_links.keys()).difference(
                              set(first_order_tables)))])

        # Now there's the tricky part. We need to define a order to inser the
        # tables with FKs. We need to care about their order because they could
        # have constraings among them

        # So first we take the tables that are missing
        missing_tables = list(set(fks_links))

        # Let's add the tables dependencies information to the globals
        # stats
        self.stats["tables_graph"]["fks_rev"] = copy.deepcopy(fks_links)
        self.stats["tables_graph"]["fks"] = copy.deepcopy(tables_fks)

        # We take only those that have no links with tables that were not
        # dumped yet
        while missing_tables:
            # Let's check the next table on the queue
            table = missing_tables.pop()

            if table in tables_order:
                continue

            # check if still have "connections"
            if table not in tables_fks:
                # No connections, we can include the table
                tables_order.append(table)
            else:
                # Oh no.. still connected. We need to check if the link that
                # were locking this tables were resolved or still open
                tab_links = tables_fks[table]

                if not tab_links:
                    # ooops... seems that there are no more links locking this
                    # table. We can add it...
                    tables_order.append(table)

                else:
                    # Let's check if we have dumped the information that this
                    # tables was waiting to be released
                    for column, constr in tables_fks[table].items():
                        lnk_table = constr[0] # 0 = TABLE, 1 = COLUMN
                        if lnk_table in tables_order or lnk_table==table:
                            tab_links.pop(column)

                    if not tab_links:
                        # If all the constraints were removed we can add table
                        tables_order.append(table)
                    else:
                        # Still have constraints. Let's just update the global
                        # table information
                        tables_fks[table] = tab_links

                        # So we need to put the table back in the queue of tables
                        # locked
                        missing_tables = [table] + missing_tables

        for tab in tables_fks:
            if tab not in tables_order:
                tables_order.append(tab)

        del source
        #tables_order.reverse()
        return tables_order


    def check_table(self, table):
        """ 
        Checks if table exists in the database linked to the instance binded
        database engine and creates it if not.
        
        INPUTS:

        table    ::: sqlalchemy table
        dbengine ::: sqlalchemy database engine
        
        OUTPUT:
        
        None
        """
        # We need to check if the tables exists and if not we create it
        try:
            if not table.exists(self.engine):
                for col in table.columns:
                    # We need to do little type convertions to handle SQL Server
                    # specific types
                    if isinstance(col.type, sa.types.NullType):
                        col.type = sa.Text()
                    
                    if isinstance(col.type, (mssql.base.BIT)):
                        col.type = sa.Boolean()
                    

                self.log_cb("creating table %s"%table.name)
                table.create(self.engine)
                self.log_cb("created table %s"%table.name)

        except Exception, e:
            return u"""Error creating table [%s] on destination.
            Error details: %s""" % (table.name, e.message)
        
        
    def get_table_fks_mapping(self, table):
        # ----------------------------------------------------------------
        # We need to check if this table has foreign keys that point to
        # other tables that have had their PKs re-mapped!
        # If this is the case we need to re-map the rows kf keys to the new
        # ones
        fks = list(table.foreign_keys)
        fk_mappings = {}
        for fk in fks:
            fk_col = fk.column
    
            for column in fk.constraint.columns:
                fk_mappings[column] = self.stats["tables"].get(fk_col.table.name, {})
        return fk_mappings 
    
    
    def get_db_data(self, table, pk_col, transfer_mode, conn):
        # We define a set of pks that we want the migrator to skip from the
        # dump. To do that we need the data in the db
        db_data = set()
        if transfer_mode == "DIFF":
            # We have to pre-check data in the destination database to dump
            # only data that is not already there
            db_data = self._db_data.get(table.name, None)
            if db_data is None:
                # Let's repleace the dict entirely so we don't overload memory
                # with huge tables that were already dumped in memory..
                db_data = conn.execute(sa.select([pk_col])).fetchall()
                db_data = set(d[0] for d in db_data)

                self._db_data = {table.name: db_data}

        return db_data        


    def prepare_records(self, records, table, pk_col, tab_stats, 
                        fk_mappings, conn):
        """
        Prepare a collection of records that needs to be dumped skipping the
        records that are already in the database (db_data) and remapping the
        foreign keys that were mapped to a new id (because the primary key they
        point to is an autoincremental identity column
        
        TODO: BETTER DOCUMENT ME PLEASE
        """
        transfer_mode = self.options.get(
                        "transfer_mode", self.default_options.get("transfer_mode")
                    )

        if transfer_mode =="DIFF":
            compare_mode = self.options.get(
                        "compare_mode", self.default_options.get("compare_mode")
                    )
            records = skip_records(
                records,
                table, pk_col,
                compare_mode,
                transfer_mode,
                tab_stats,
                conn
            )
        else:
            records = [prepare_record(record, fk_mappings) for record in records] 

        return records
    
def prepare_record(record, fk_mappings):
    record = dict(record)
    
    for column, mapping in fk_mappings.items():
        value = record[column]
        record[column] = mapping["pk_map"].get(value, value)    
    
    return record

def skip_records(records, table, pk_col, compare_mode, transfer_mode, 
                 tab_stats, conn):
    """
    Skip all the records that are already present in table.
    
    Different compare modes will be applied depending on the option["compare_mode"]
    """
    if compare_mode == "PK_IN_CACHE":     
        records = skip_records_pk(records, 
                                  table,
                                  pk_col, 
                                  transfer_mode,
                                  tab_stats,
                                  conn)
            
    elif compare_mode == "FULL":
        records = skip_records_full(records, table,  conn)
        
    elif compare_mode == "FULL_NO_PK":
        records = skip_records_full_no_pk(records, table, pk_col)
        
    else:
        records = [prepare_record(record) for record in records]  
    return records
    
def skip_records_pk(records, table, pk_col, transfer_mode, tab_stats, conn):
    """
    checks if the data is not into the destination DB by checking only the
    primary key (FAST CHECK)
    
    record ::: data record that needs to be checked if is already present in db_data
    
    
    """
    db_data = self.get_db_data(table, pk_col, transfer_mode, conn)
    for i, record in enumerate(records):
        if pk_col is not None and getattr(record, pk_col.name) in db_data:
            # if the record pk value is one of those we have to skip
            # we replace it with an empty dict so we can remove it later                    
            records[i] = {}
            tab_stats["records_skipped"] += 1
        else:
            # otherwise we can simply prepare the record dictionary
            records[i] = prepare_record(record) 
    return records


def skip_records_full_in_memory(records, table, conn):
    """
    When transfer_mode==DIFF checks if the data is not into the destination
    DB by checking all the column values of the row. Return only the records
    that are not in the "conn" database already.
    
    see skip_record_pk definition for input parameters details
    """
    
    output = []
    
    stmt = table.select()
    temp_res = conn.execute(stmt).fetchall()
    columns = [col.name for col in table.c]
    
    dest_rows = set([tuple(row)  for row in temp_res])
    recs_set = set([tuple(row)  for row in records])
    
    diff_set = recs_set.difference(dest_rows)
    print "GOT", 
    print "checking", len(diff_set)
    
    ## first let's get a "clean" record without autoincremet columns
    #for i, record in enumerate(records):
        #if i%1000 == 0:
            #print ".", i       
        #cleaned_record = clean_record(table, record)
        
        #row = tuple([row[col] for row in temp_res])
        #if not row in dest_rows:
            #output.append(record)
            
    return diff_set


def skip_records_full(records, table, conn, exclude_pk = False):
    """
    When transfer_mode==DIFF checks if the data is not into the destination
    DB by checking all the column values of the row. Return only the records
    that are not in the "conn" database already.
    
    see skip_record_pk definition for input parameters details
    """
    
    output = []
    # first let's get a "clean" record without autoincremet columns
    for i, record in enumerate(records):
        if i%1000 == 0:
            print ".", i       
        cleaned_record = clean_record(table, record)
    
        # build che where clause
        pairs = [getattr(table.c, colname)==value for colname, value in cleaned_record.items()]
        clause = reduce(operator.and_, pairs)
        
        # build the select statment
        stmt = table.select(clause)
        result = conn.execute(stmt).fetchall()
        print "COMPARING...", cleaned_record, "---", result
        if len(result) == 1:
            pass #output.append(record)
        elif len(result) > 1:
            # TODO: Not really sure if it should be considered as an error
            pass
            #raise NotImplementedError()
        else:
            output.append(record)

    return output


def skip_records_full_no_pk(records, table, pk_col, db_data):
    """
    When transfer_mode==DIFF checks if the data is not into the destination DB
    by checking all the row values of the row excluding the primary key value. 
    This mode is designed for tables with an autoincrement pk.
    """
    pass


def check_record_in_db(record, table, engine):
    dest = engine.connect() # connect to db
    stmt = [ getattr(table.c, col) == val for col, val in record.keys() ]
    
    res = dest.execute(table.select, _records)    
    
    dest.close()

# METHODS
def mssql_creator_factory(connstr):
    """ pyodbc connection factory to handle mssql connection as
    default pyodbc-sqlalchemy is bogus and does not handle some data
    types correctly (NVARCHAR, VARCHAR seen as TEXT fields with causes
    several querying problems.
    The function accepts a standed ODBC connection string which will be used
    directly to create the connection without using SQLAlchemy internal
    connection creator factory.

    Returns a creator factory function.

    INPUTS:

    connstr ::: odbc connection string 
                
    >>> connstr = "Driver={SQL Server Native Client 10.0};Server=localhost;\
    Database=DBNAME;UID=USERNAME;PWD=pwd;"
    >>> creator = mssql_creator_factory(connstr)
    
    [for further details check the dbms module documentation],
    
    """

    def creator():
        """ factory function that creates new connections """
        import pyodbc
        return pyodbc.connect(connstr)

    return creator


def build_engine(conn_string, echo=False, encoding="utf-8"):
    """ Retuns a SQLAlchemy engine connected to the DB specified in
    conn_string.

    INPUTS:

    conn_string ::: database connection string following the sqlalchemy
    connection string format specification or the following:

        creator://<STANDARD ODBC CONNECTION STRING>

        This second option should be used to connect SQL Server Databases
        as it bypass the SQLAlchemy pyodbc connection creation that (in rare
        cases) have query problems due to a pyodbc know bug.

        See:
            https://code.google.com/p/pyodbc/issues/detail?id=13
            https://code.google.com/p/pyodbc/issues/detail?id=159

    echo ::: (boolean) indicates the echo setting of the returned engine
    
    encoding ::: (string) unicode encoding to be used to the sqlalchemy db
    engine or the string 'creator://<odbc connection string>' where 
    <odbc connection string> is a plain odbc connection string. For example:

    >>> connstr = "creator://Driver={SQL Server Native Client 10.0};\
    Server=localhost;Database=DBNAME;UID=USERNAME;PWD=pwd;"
    >>> engine = build_engine(connstr, True)
    
    or 
    
    >>> engine = build_engine("dbms:::sqlite:///data/test.db", True)
    """
    # Check if the creatir prefix was specified
    if conn_string.startswith("creator://"):
        conn_string = conn_string.replace("creator://", "")
        engine = create_engine(
            "mssql://",
            creator=mssql_creator_factory(conn_string),
            echo=echo,
            convert_unicode=True,
            encoding=encoding
        )

    else:
        engine = create_engine(
            conn_string,
            echo=echo,
            convert_unicode=True,
            encoding=encoding
        )

    return engine


def is_numeric_column(column):
    """ 
    check if the columns is an instance of a numeric sqlalchemy columns 
    """
    col_types = (sa.Numeric, sa.Integer, sa.Float)
    return column is not None and isinstance(column.type, col_types)


def clean_record(table, row):
    """ Standard record cleaning function. It simply drops autoincremental
    primary key columns from row. The useful scenario where to use this
    function is when migrating rows between 2 datbases and the table primary
    key is autoincremental. This means that certain database would raise an
    error as it's not possible to manually specify the primary key value.
    
    INPUT:
    
    table ::: sqlalchemy table object
    
    row ::: data row to be cleaned. Can be a SQLAlchemy resultset row or a 
    dictionary-like object
    
    """
    # TODO: this way of "cleaning" the records where we exclude autoincremental
    #       primary keys should be reviewed in further version. It should be 
    #       easier to give more choices of "managing" this kind of issues
    
    out = {}
    for col in table.columns:
        if is_numeric_column(col) and col.autoincrement and col.primary_key:
            pass  # exclude the column
        
        else:
            out[col.name] = row[col.name]
            
    return out