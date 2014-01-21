from mock import Mock

from migrations.backends import base, dbms
import sqlalchemy as sa
import test_backends_base
from test_backends_base import check_default_init, check_api_delegation, \
     check_init_migration


def get_migrator(path, monkeypatch):
    monkeypatch.setattr( dbms, "build_engine", Mock(return_value="mocked_engine"))
            
    migrator = dbms.Migrator(path)
    return migrator

    
class TestMigrator:

    def setup(self):
        self.path = "dbms:::sqlite:///data/testdb.db"

          
    #************************************************
    #
    # STANDARD MIGRATOR METHODS TESTS
    #
    #************************************************
    
    def test_constructor(self, monkeypatch):
        check_default_init(self, monkeypatch, dbms.Migrator)
        
        
    def test_initialize(self, monkeypatch):
        migrator = get_migrator(self.path, monkeypatch)
        
        # assert the database engine creation function has been called
        assert migrator.engine == "mocked_engine"
        dbms.build_engine.assert_called_with(self.path)
        
        # Assert that the database data dict has been initialized
        assert migrator._db_data == {}
        
    
    def test_api_delegation(self, monkeypatch):
        migrator = get_migrator(self.path, monkeypatch)
        check_api_delegation(migrator, monkeypatch, dbms.Migrator)

        
    def test_init_migration(self, monkeypatch):
        migrator = get_migrator(self.path, monkeypatch)
        check_init_migration(migrator)
        
        
    def test_get_table_stats(self, monkeypatch):
        monkeypatch.setattr(
            base, 
            "get_table_default_stats", 
            Mock(return_value="mocked_stats")
        )
        
        migrator = get_migrator(self.path, monkeypatch)
        
        # Let's get the table stats twice and assert that the table stats 
        # creation function gets called only the first time
        for i in range(2):
            table_stats = migrator.get_table_stats("A")
            assert table_stats == "mocked_stats"
            base.get_table_default_stats.assert_called_once_with("A")
            assert base.get_table_default_stats.call_count == 1
        
    
    def test_report_last_migration(self, monkeypatch):
        migrator = get_migrator(self.path, monkeypatch)
        test_backends_base.check_report_last_migration(migrator, monkeypatch)
    
    
    def test_update_dump_stats(self, monkeypatch):
        migrator = get_migrator(self.path, monkeypatch)
        test_backends_base.check_update_dump_stats(migrator, monkeypatch, dbms.Migrator)
        
        
    #************************************************
    #
    # CUSTOM DBMS MIGRATOR METHODS TESTS
    #
    #************************************************    
    def test_get_tables_dump_order(self, monkeypatch):
        raise NotImplementedError
    
    
    def test_check_table(self, monkeypatch):
        raise NotImplementedError
    
    
    def test_get_table_fks_mapping(self, monkeypatch):
        raise NotImplementedError
    
    
    def test_get_db_data(self, monkeypatch):
        raise NotImplementedError
    
    
    def test_prepare_records(self, monkeypatch):
        raise NotImplementedError
    

    def test_prepare_record(self, monkeypatch):
        raise NotImplementedError


    def test_skip_records(self, monkeypatch):
        raise NotImplementedError


    def test_skip_records_pk(self, monkeypatch):
        raise NotImplementedError


    def test_mssql_creator_factory(self, monkeypatch):
        raise NotImplementedError


    def test_build_engine(self, monkeypatch):
        raise NotImplementedError


    def test_is_numeric_column(self, monkeypatch):
        for col in [
            sa.Column("A", sa.Numeric),
            sa.Column("B", sa.Float),
            sa.Column("C", sa.Integer),
        ]:
            assert dbms.is_numeric_column(col) == True

        for col in [
            sa.Column("A", sa.Unicode),
            sa.Column("B", sa.Boolean),
            sa.Column("C", sa.String),
            sa.Column("C", sa.DateTime),
            sa.Column("C", sa.Date),
        ]:
            assert dbms.is_numeric_column(col) == False

    def test_clean_record(self, monkeypatch):
        monkeypatch.setattr(dbms, "is_numeric_column", Mock(return_value=True))
        table_mock = Mock(
            name="test_table",
            columns = [
                Mock(
                    autoincrement = True,
                    primary_key = True, ),
                Mock(
                    autoincrement = False,
                    primary_key = False,),
                Mock(
                    autoincrement = True,
                    primary_key = False
                ),
            ]
        )
        for i, col in enumerate(["PK", "col1", "col2"]):
            table_mock.columns[i].name = col

        record = {"PK": 1, "col1": "hi", "col2": 9}
        res = dbms.clean_record(table_mock, record)

        assert res == {"col1": "hi", "col2": 9}

        monkeypatch.setattr(dbms, "is_numeric_column", Mock(return_value=False))
        res = dbms.clean_record(table_mock, record)

        assert res == record

