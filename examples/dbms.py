import os
import datetime 

from migrations.migrator import Migration
from migrations.loggers import verbose_logger

here = os.path.dirname(os.path.abspath(__file__))

report_path = os.path.join(here, "last_migration_rpt.html")    

# Let's start playing with migrations
migrator = Migration(
    #"dbms:::creator://Driver={SQL Server Native Client 10.0};Server=PRMSAWN126;Database=FIALE_AUT_PASS;UID=sa;PWD=revihcra;",
    "dbms:::sqlite:///data/test.db",
    "dbms:::sqlite:///data/test_2.db",
    #"csvfile:::%s" % csv_folder_path,
    log_cb = verbose_logger,
    #transfer_mode = "DIFF",
    encoding='cp1252'
    )
migration_res = migrator.migrate(exclude = ["request*", #"tags", 
                            "hda*"])#tables = ["tags"],
                                                    
# Finally we check the migration results 
migrator.report_last_migration(report_path)

result = migrator.check_last_migration()
print "\n\n\n******\nsSTARTING MIGRATION BACK********\n\n"
