import os
import datetime

from migrations.migrator import Migration
from migrations.loggers import verbose_logger

intables = [] #tab for tab in intables if tab]
here = os.path.dirname(os.path.abspath(__file__))

# Let's first simulate a migration from a sql database to N csv files
folder_name = "getzendoc_static_config"
examples_folder = "%s/data/%s" % (here, folder_name)
# Let's create the example csv destination folder
if not os.path.exists(examples_folder):
    os.mkdir(examples_folder)

json_folder_path = "%s/json" % (examples_folder)
report_path = json_folder_path.replace("/json", "/last_migration_rpt.html")

# Let's create the example csv destination folder
if not os.path.exists(json_folder_path):
    os.mkdir(json_folder_path)

# Let's start playing with migrations
migrator = Migration(
    "dbms:::postgresql://fpliger:@localhost:5432/pyzendoc",
    "jsonfile:::%s" % json_folder_path,

    log_cb = verbose_logger,
    #transfer_mode = "DIFF",
    encoding='cp1252'
    )
migration_res = migrator.migrate(
    tables = ["auditable_actions", "user_groups",
              "document_usergroups", "document_types",
              "document_status", "document_status_flow",
              "document_status_flow_authorization",
              "auditable_actions"])

# Finally we check the migration results
migrator.report_last_migration(report_path)

result = migrator.check_last_migration()
print "\n\n\n******\n MIGRATION FINISHED********\n\n"
#
## Now that we have finished we can start a new Migration from the csv
## files to a new sqlite empty database
#migrator = Migration(
#    "csvfile:::%s" % csv_folder_path,
#    "dbms:::sqlite:///%s/csv_to_sql.db" % examples_folder,
#    log_cb = verbose_logger,
#    encoding='cp1252'
#    )
#migrator.migrate()
#
## Finally we check the migration results
#migrator.report_last_migration(report_path)
