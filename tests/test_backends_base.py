import copy
from mock import Mock

from migrations.loggers import default_logger
from migrations.backends import base
from migrations import reporting

def mytest_logger(msg):
    return msg
    

def check_default_init(self, monkeypatch, migration_class):
    test_options = {"A":"a", "B": "b"}
    monkeypatch.setattr( migration_class, "initialize", Mock())
    
    # Test initialization with custom logger callback and default options
    migrator = migration_class(self.path, log_cb = mytest_logger)        
    assert migrator.path == self.path
    assert migrator.log_cb == mytest_logger
    assert migrator.options == migration_class.default_options
    assert migrator.stats == {"tables": {}}
    assert migrator.initialize.call_count == 1
    
    migration_class.initialize.reset_mock()
    
    # Test initialization with default logger callback and custom options
    migrator = migration_class(self.path, **test_options)
    assert migrator.path == self.path
    assert migrator.log_cb == default_logger
    options = copy.deepcopy(migration_class.default_options)
    options.update(test_options)
    assert migrator.options == options
    assert migrator.stats == {"tables": {}}   
    assert migrator.initialize.call_count == 1
    

def check_api_delegation(migrator, monkeypatch, migration_class):
    # monkeypatch all methods
    for meth in ["_migrate", "_dump", "_compare", "init_migration"]:
        monkeypatch.setattr( migration_class, meth, Mock())
        
    # prepare some test data
    tables = ["a", "b"]
    records = [1, 2, 3]
    fake_destination = Mock()
    paquet = 2
    exclude = ["c", "b"]
    migrate_compare_kws = dict(
        tables = tables, 
        paquet = paquet,
        exclude = exclude
    )
    
    # call the migrator methods and check if they are calling the inner methods
    # they should be calling
    migrator.migrate(
        fake_destination, 
        **migrate_compare_kws
    )
    migrator.init_migration.assert_called_once_with(
            fake_destination
    )    
    migrator._migrate.assert_called_once_with(
        fake_destination,
        **migrate_compare_kws
    )
    
    migrator.dump(
            records, 
            tables[0]
    )
    
    migrator.compare(
        fake_destination,
        **migrate_compare_kws
    )
    migrator._compare.assert_called_once_with(
            fake_destination,
            **migrate_compare_kws
        )    


def check_init_migration(migrator):
    destination = Mock()
    destination.path = "path_to_dest"
    migrator.init_migration(destination)
    
    assert  destination.stats == migrator.stats
    
    for key, val in {
        "transfer_mode": migrator.options["transfer_mode"],
        "source": migrator.path,
        "destination": destination.path,
        "tables":{},
        "tables_graph": {},
        "exceptions": [],
        "messages" : [],
        "total_records_transferred": 0,
        "total_records_skipped": 0
        }.items():
        assert destination.stats[key] == migrator.stats[key] == val
        

def test_table_default_dict():
    expected = {
        "pk_map":{},
        "skipped": False,
        "records_transferred": 0,
        "records_skipped": 0,
        "total_records": 0,
        "exceptions": [],
        "messages": [],
        "lst_records_transferred": [],
        "name": "A",
    }
    assert base.get_table_default_stats("A") == expected
    
    
def check_report_last_migration(migrator, monkeypatch):
    monkeypatch.setattr( reporting, "html", Mock(return_value="mocked_engine"))
    
    migrator.report_last_migration("filepath", "templatepath")
    
    reporting.html.called_once_with("filepath", migrator.stats, "templatepath")
    
    
def check_update_dump_stats(migrator, monkeypatch, migrator_class):
    monkeypatch.setattr( reporting, "html", Mock(return_value="mocked_engine"))
    monkeypatch.setattr( migrator, "log_cb", Mock())

    records = range(100)
    
    tab_stats = _check_std_update_dump_stats(migrator, "FULL", records)
    assert tab_stats["lst_records_transferred"] == [] #records
    
    migrator.log_cb.reset_mock()
    tab_stats = _check_std_update_dump_stats(migrator, "DIFF", records)
    assert tab_stats["lst_records_transferred"] == records
    
    
def _check_std_update_dump_stats(migrator, transfer_mode, records):
    tab_stats = base.get_table_default_stats("A")
    
    migrator.init_migration(Mock())
    
    stats = migrator.update_dump_stats(tab_stats, records, transfer_mode)
    
    assert stats == tab_stats

    assert tab_stats["records_transferred"] == len(records)
    
    assert migrator.stats["total_records_transferred"] == len(records)
    assert migrator.stats["total_records_skipped"] == tab_stats["records_skipped"]
    assert migrator.log_cb.call_count == 1

    return tab_stats