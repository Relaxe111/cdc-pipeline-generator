# CDC Command Usage Stats

This report counts normalized `cdc ...` command occurrences by git user.
Normalization keeps command path and ignores option/argument ordering.

## Machine Readable

```json
{
  "generated_at_utc": "2026-02-22T19:35:01+00:00",
  "total": {
    "cdc generate": 15,
    "cdc generate | flags: --environment=dev, --service=my-service": 2,
    "cdc generate | flags: --environment=local, --service=adopus": 1,
    "cdc init": 3,
    "cdc init | flags: --git-init, --name=my-project, --type=adopus": 1,
    "cdc init | flags: --name=PROJECT_NAME": 1,
    "cdc init | flags: --name=adopus-cdc, --type=adopus": 2,
    "cdc init | flags: --name=asma-cdc, --type=asma": 2,
    "cdc init | flags: --name=my-project, --target-dir=/path/to/project, --type=adopus": 1,
    "cdc init | flags: --name=my-project, --type=adopus": 1,
    "cdc manage-column-templates | flags: --add=sync_timestamp": 1,
    "cdc manage-column-templates | flags: --add=tenant_id": 4,
    "cdc manage-column-templates | flags: --edit=tenant_id": 3,
    "cdc manage-column-templates | flags: --list": 3,
    "cdc manage-column-templates | flags: --remove=tenant_id": 3,
    "cdc manage-column-templates | flags: --remove=tenant_id\",": 1,
    "cdc manage-column-templates | flags: --show=tenant_id": 3,
    "cdc manage-column-templates | flags: --show=tenant_id\",": 1,
    "cdc manage-pipelines stress-test": 1,
    "cdc manage-pipelines verify-sync": 1,
    "cdc manage-server-group": 5,
    "cdc manage-server-group | args: 2 | flags: --remove-extraction-pattern=prod": 2,
    "cdc manage-server-group | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1,
    "cdc manage-server-group | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1,
    "cdc manage-server-group | flags: --add-group=adopus": 1,
    "cdc manage-server-group | flags: --add-server=analytics, --source-type=postgres": 1,
    "cdc manage-server-group | flags: --add-to-ignore-list\",": 1,
    "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern": 1,
    "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern_to_ignore": 1,
    "cdc manage-server-group | flags: --add-to-ignore-list=\"test_pattern": 1,
    "cdc manage-server-group | flags: --add-to-schema-excludes\",": 1,
    "cdc manage-server-group | flags: --add-to-schema-excludes=\"schema_to_exclude": 1,
    "cdc manage-server-group | flags: --add-to-schema-excludes=\"test_schema": 1,
    "cdc manage-server-group | flags: --all, --update": 1,
    "cdc manage-server-group | flags: --create=asma, --pattern=db-shared": 1,
    "cdc manage-server-group | flags: --create=my-group": 6,
    "cdc manage-server-group | flags: --create=test-group": 1,
    "cdc manage-server-group | flags: --info": 2,
    "cdc manage-server-group | flags: --info\",": 1,
    "cdc manage-server-group | flags: --list": 3,
    "cdc manage-server-group | flags: --list-extraction-patterns": 1,
    "cdc manage-server-group | flags: --list-extraction-patterns=SERVER": 1,
    "cdc manage-server-group | flags: --list-extraction-patterns=prod": 3,
    "cdc manage-server-group | flags: --list-ignore-patterns": 2,
    "cdc manage-server-group | flags: --list-schema-excludes": 1,
    "cdc manage-server-group | flags: --refresh": 1,
    "cdc manage-server-group | flags: --server-group=adopus, --update": 1,
    "cdc manage-server-group | flags: --server-group=asma, --update": 1,
    "cdc manage-server-group | flags: --update": 11,
    "cdc manage-server-group | flags: --update\",": 1,
    "cdc manage-server-group | flags: --update=default": 1,
    "cdc manage-server-group | flags: --update=prod": 1,
    "cdc manage-service": 3,
    "cdc manage-service directory | flags: --, --inspect": 1,
    "cdc manage-service directory | flags: --, --sink=sink_asma.proxy": 1,
    "cdc manage-service | args: dbo.Fraver | flags: --add-source-tables=dbo.Actor, --service=adopus": 1,
    "cdc manage-service | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1,
    "cdc manage-service | flags: --, --add-sink-table=pub.A, --sink=asma": 1,
    "cdc manage-service | flags: --, --add-sink-table=pub.Actor": 1,
    "cdc manage-service | flags: --, --add-sink-table=pub.Actor, --service=dir": 1,
    "cdc manage-service | flags: --, --add-source-table=Actor": 1,
    "cdc manage-service | flags: --, --add-source-table=Actor, --inspect": 1,
    "cdc manage-service | flags: --, --modify-custom-table=tbl": 1,
    "cdc manage-service | flags: --, --sink-table=pub.Actor, --sink=asma": 1,
    "cdc manage-service | flags: --, --sink-table=t, --sink=a": 1,
    "cdc manage-service | flags: --, --sink=sink_asma.proxy": 3,
    "cdc manage-service | flags: --, --source-table=Actor": 1,
    "cdc manage-service | flags: --add-column-template=tmpl, --sink-": 1,
    "cdc manage-service | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1,
    "cdc manage-service | flags: --add-sink-table=pub.Actor, --map-": 1,
    "cdc manage-service | flags: --add-sink-table=pub.Actor, --sink-": 1,
    "cdc manage-service | flags: --add-sink=sink_asma.chat\",, --service=directory": 1,
    "cdc manage-service | flags: --add-sink=sink_asma.chat, --service=directory": 1,
    "cdc manage-service | flags: --add-source-table=dbo.Actor\",, --service=adopus": 1,
    "cdc manage-service | flags: --add-source-table=dbo.Actor, --service=adopus": 1,
    "cdc manage-service | flags: --add-source-table=dbo.Orders, --service=myservice": 1,
    "cdc manage-service | flags: --add-source-table=dbo.Users, --service=myservice": 3,
    "cdc manage-service | flags: --add-source-table=public.users, --service=proxy": 2,
    "cdc manage-service | flags: --add-table=Actor, --primary-key=actno, --service=adopus": 5,
    "cdc manage-service | flags: --add-table=Actor, --service=adopus": 2,
    "cdc manage-service | flags: --add-table=Fraver, --primary-key=fraverid, --service=adopus": 1,
    "cdc manage-service | flags: --add-table=MyTable, --primary-key=id, --service=my-service": 1,
    "cdc manage-service | flags: --add-table=Orders, --primary-key=order_id, --service=my-service": 1,
    "cdc manage-service | flags: --add-table=Users, --primary-key=id, --service=my-service": 1,
    "cdc manage-service | flags: --add-validation-database=AdOpusTest\",, --create-service=adopus": 1,
    "cdc manage-service | flags: --all\",, --inspect, --service=adopus": 1,
    "cdc manage-service | flags: --all, --generate-validation, --service=adopus": 1,
    "cdc manage-service | flags: --all, --inspect, --service=adopus": 2,
    "cdc manage-service | flags: --all, --inspect, --service=myservice": 1,
    "cdc manage-service | flags: --all, --inspect-mssql, --service=adopus": 1,
    "cdc manage-service | flags: --all, --inspect-sink=sink_asma.calendar, --service=directory": 1,
    "cdc manage-service | flags: --create-service, --server=analytics, --service=analytics_data": 1,
    "cdc manage-service | flags: --create-service, --service=myservice": 2,
    "cdc manage-service | flags: --create-service=myservice\",": 1,
    "cdc manage-service | flags: --create=adopus, --server-group=adopus": 1,
    "cdc manage-service | flags: --create=my-service": 1,
    "cdc manage-service | flags: --create=my-service, --server-group=my-group": 1,
    "cdc manage-service | flags: --create=my-service, --server-group=my-server-group": 1,
    "cdc manage-service | flags: --env=prod\",, --inspect, --service=adopus": 1,
    "cdc manage-service | flags: --inspect, --save, --schema=dbo, --service=myservice": 1,
    "cdc manage-service | flags: --inspect, --schema=dbo\",, --service=adopus": 1,
    "cdc manage-service | flags: --inspect, --schema=dbo, --service=adopus": 5,
    "cdc manage-service | flags: --inspect, --schema=dbo, --service=my-service": 1,
    "cdc manage-service | flags: --inspect, --schema=dbo, --service=myservice": 1,
    "cdc manage-service | flags: --inspect, --service=myservice": 1,
    "cdc manage-service | flags: --inspect-mssql, --schema=dbo, --service=adopus": 2,
    "cdc manage-service | flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory": 1,
    "cdc manage-service | flags: --list-services\",": 1,
    "cdc manage-service | flags: --list-sinks, --service=directory": 1,
    "cdc manage-service | flags: --remove-service=myservice": 1,
    "cdc manage-service | flags: --remove-service=myservice\",": 1,
    "cdc manage-service | flags: --remove-sink=sink_asma.chat\",, --service=directory": 1,
    "cdc manage-service | flags: --remove-table=Test, --service=adopus": 2,
    "cdc manage-service | flags: --remove-table=dbo.Actor\",, --service=adopus": 1,
    "cdc manage-service | flags: --remove-table=dbo.Actor, --service=adopus": 1,
    "cdc manage-service | flags: --runtime, --service=directory, --validate-bloblang": 2,
    "cdc manage-service | flags: --runtime, --service=directory, --validate-config": 1,
    "cdc manage-service | flags: --service=adopus": 2,
    "cdc manage-service | flags: --service=adopus, --validate-config": 2,
    "cdc manage-service | flags: --service=directory": 18,
    "cdc manage-service | flags: --service=directory, --validate-sinks": 1,
    "cdc manage-service | flags: --service=proxy": 1,
    "cdc manage-service | flags: --source-table=Actor, --track-": 1,
    "cdc manage-service-schema": 3,
    "cdc manage-service-schema | flags: --list": 1,
    "cdc manage-service-schema | flags: --list, --service=chat": 2,
    "cdc manage-service-schema | flags: --list-custom-tables, --service=calendar": 1,
    "cdc manage-service-schema | flags: --list-services": 2,
    "cdc manage-service-schema | flags: --remove-custom-table=public.my_events, --service=calendar": 1,
    "cdc manage-service-schema | flags: --service=calendar": 1,
    "cdc manage-service-schema | flags: --service=calendar, --show=public.my_events": 1,
    "cdc manage-service-schema | flags: --service=chat": 7,
    "cdc manage-services config": 3,
    "cdc manage-services config directory": 11,
    "cdc manage-services config directory | flags: --, --all": 1,
    "cdc manage-services config directory | flags: --, --all, --inspect-sink": 1,
    "cdc manage-services config directory | flags: --, --inspect": 1,
    "cdc manage-services config directory | flags: --, --sink=sink_asma.proxy": 1,
    "cdc manage-services config | flags: --": 3,
    "cdc manage-services config | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1,
    "cdc manage-services config | flags: --, --add-sink-table=pub.A, --sink=asma": 1,
    "cdc manage-services config | flags: --, --add-sink-table=pub.Actor, --service=dir": 1,
    "cdc manage-services config | flags: --, --add-source-table=Actor": 1,
    "cdc manage-services config | flags: --, --add-source-table=Actor, --inspect": 1,
    "cdc manage-services config | flags: --, --modify-custom-table=tbl": 1,
    "cdc manage-services config | flags: --, --sink-table=pub.Actor, --sink=asma": 1,
    "cdc manage-services config | flags: --, --sink-table=t, --sink=a": 1,
    "cdc manage-services config | flags: --, --sink=sink_asma.directory": 1,
    "cdc manage-services config | flags: --, --sink=sink_asma.proxy": 3,
    "cdc manage-services config | flags: --, --source-table=Actor": 1,
    "cdc manage-services config | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1,
    "cdc manage-services config | flags: --add-sink-table, --fr": 1,
    "cdc manage-services config | flags: --add-sink-table=pub.Actor, --map-": 1,
    "cdc manage-services config | flags: --add-sink-table=pub.Actor, --sink-": 1,
    "cdc manage-services config | flags: --add-sink-table=public.": 1,
    "cdc manage-services config | flags: --add-source-table=dbo.": 3,
    "cdc manage-services config | flags: --add-source-table=dbo., --add-source-table=dbo.Address": 1,
    "cdc manage-services config | flags: --create-service=directory": 1,
    "cdc manage-services config | flags: --inspect, --service=myservice": 1,
    "cdc manage-services config | flags: --source-table=Actor, --track-": 1,
    "cdc manage-services schema custom-tables": 6,
    "cdc manage-services schema custom-tables | flags: --service=n": 1,
    "cdc manage-sink-groups": 2,
    "cdc manage-sink-groups | flags: --add-new-sink-group=analytics": 1,
    "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --for-source-group=foo, --type=postgres": 1,
    "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --type=postgres": 3,
    "cdc manage-sink-groups | flags: --add-server=default, --sink-group=sink_analytics": 2,
    "cdc manage-sink-groups | flags: --add-server=prod, --sink-group=sink_analytics": 1,
    "cdc manage-sink-groups | flags: --add-to-ignore-list=temp_%\",": 1,
    "cdc manage-sink-groups | flags: --add-to-schema-excludes=hdb_catalog\",": 1,
    "cdc manage-sink-groups | flags: --create": 3,
    "cdc manage-sink-groups | flags: --create, --source-group=asma\",": 1,
    "cdc manage-sink-groups | flags: --create, --source-group=foo": 4,
    "cdc manage-sink-groups | flags: --info=sink_analytics": 3,
    "cdc manage-sink-groups | flags: --info=sink_asma\",": 1,
    "cdc manage-sink-groups | flags: --info=sink_foo": 1,
    "cdc manage-sink-groups | flags: --introspect-types, --sink-group=sink_analytics": 1,
    "cdc manage-sink-groups | flags: --list": 3,
    "cdc manage-sink-groups | flags: --remove-server=default, --sink-group=sink_analytics": 2,
    "cdc manage-sink-groups | flags: --remove=sink_analytics": 2,
    "cdc manage-sink-groups | flags: --remove=sink_test\",": 1,
    "cdc manage-sink-groups | flags: --sink-group=sink_analytics, --update": 2,
    "cdc manage-sink-groups | flags: --sink-group=sink_asma": 8,
    "cdc manage-sink-groups | flags: --sink-group=sink_asma, --update": 1,
    "cdc manage-sink-groups | flags: --validate": 3,
    "cdc manage-source-groups": 7,
    "cdc manage-source-groups | args: 2 | flags: --remove-extraction-pattern=prod": 3,
    "cdc manage-source-groups | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1,
    "cdc manage-source-groups | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1,
    "cdc manage-source-groups | flags: --, --add-server=srv1": 1,
    "cdc manage-source-groups | flags: --, --introspect-types": 1,
    "cdc manage-source-groups | flags: --add-extraction-pattern=default": 2,
    "cdc manage-source-groups | flags: --add-extraction-pattern=prod": 4,
    "cdc manage-source-groups | flags: --add-server=analytics, --source-type=postgres": 2,
    "cdc manage-source-groups | flags: --add-to-ignore-list\",": 1,
    "cdc manage-source-groups | flags: --add-to-ignore-list=\"pattern_to_ignore": 1,
    "cdc manage-source-groups | flags: --add-to-ignore-list=\"test_pattern": 1,
    "cdc manage-source-groups | flags: --add-to-schema-excludes\",": 1,
    "cdc manage-source-groups | flags: --add-to-schema-excludes=\"schema_to_exclude": 1,
    "cdc manage-source-groups | flags: --add-to-schema-excludes=\"test_schema": 1,
    "cdc manage-source-groups | flags: --all, --update": 1,
    "cdc manage-source-groups | flags: --create=asma, --pattern=db-shared": 1,
    "cdc manage-source-groups | flags: --create=my-group": 4,
    "cdc manage-source-groups | flags: --create=test-group": 1,
    "cdc manage-source-groups | flags: --info": 3,
    "cdc manage-source-groups | flags: --info\",": 1,
    "cdc manage-source-groups | flags: --list": 1,
    "cdc manage-source-groups | flags: --list-extraction-patterns": 1,
    "cdc manage-source-groups | flags: --list-extraction-patterns=SERVER": 1,
    "cdc manage-source-groups | flags: --list-extraction-patterns=prod": 4,
    "cdc manage-source-groups | flags: --list-ignore-patterns": 1,
    "cdc manage-source-groups | flags: --list-schema-excludes": 1,
    "cdc manage-source-groups | flags: --set-extraction-pattern=default": 2,
    "cdc manage-source-groups | flags: --update": 10,
    "cdc manage-source-groups | flags: --update\",": 1,
    "cdc manage-source-groups | flags: --update=default": 1,
    "cdc manage-source-groups | flags: --update=prod": 1,
    "cdc reload-cdc-autocompletions": 2,
    "cdc scaffold adopus": 2,
    "cdc scaffold asma": 2,
    "cdc scaffold my-group": 3,
    "cdc scaffold myproject": 2,
    "cdc scaffold myproject | flags: --pattern=db-shared, --source-type=postgres": 2,
    "cdc scaffold | flags: --implementation=test, --pattern=db-shared": 1,
    "cdc setup-local": 1,
    "cdc setup-local | flags: --enable-local-sink": 2,
    "cdc setup-local | flags: --enable-local-sink, --enable-local-source": 2,
    "cdc setup-local | flags: --enable-local-source": 2,
    "cdc setup-local | flags: --full": 2,
    "cdc test": 2,
    "cdc test tests/cli/test_scaffold.py": 1,
    "cdc test | flags: --all": 2,
    "cdc test | flags: --cli": 2,
    "cdc test | flags: --fast-pipelines": 2,
    "cdc test | flags: --full-pipelines": 2,
    "cdc test | flags: -k=scaffold": 2,
    "cdc test | flags: -v": 2,
    "cdc validate": 3
  },
  "users": {
    "igor.efrem": {
      "cdc generate": 15,
      "cdc generate | flags: --environment=dev, --service=my-service": 2,
      "cdc generate | flags: --environment=local, --service=adopus": 1,
      "cdc init": 3,
      "cdc init | flags: --git-init, --name=my-project, --type=adopus": 1,
      "cdc init | flags: --name=PROJECT_NAME": 1,
      "cdc init | flags: --name=adopus-cdc, --type=adopus": 2,
      "cdc init | flags: --name=asma-cdc, --type=asma": 2,
      "cdc init | flags: --name=my-project, --target-dir=/path/to/project, --type=adopus": 1,
      "cdc init | flags: --name=my-project, --type=adopus": 1,
      "cdc manage-column-templates | flags: --add=sync_timestamp": 1,
      "cdc manage-column-templates | flags: --add=tenant_id": 4,
      "cdc manage-column-templates | flags: --edit=tenant_id": 3,
      "cdc manage-column-templates | flags: --list": 3,
      "cdc manage-column-templates | flags: --remove=tenant_id": 3,
      "cdc manage-column-templates | flags: --remove=tenant_id\",": 1,
      "cdc manage-column-templates | flags: --show=tenant_id": 3,
      "cdc manage-column-templates | flags: --show=tenant_id\",": 1,
      "cdc manage-pipelines stress-test": 1,
      "cdc manage-pipelines verify-sync": 1,
      "cdc manage-server-group": 5,
      "cdc manage-server-group | args: 2 | flags: --remove-extraction-pattern=prod": 2,
      "cdc manage-server-group | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1,
      "cdc manage-server-group | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1,
      "cdc manage-server-group | flags: --add-group=adopus": 1,
      "cdc manage-server-group | flags: --add-server=analytics, --source-type=postgres": 1,
      "cdc manage-server-group | flags: --add-to-ignore-list\",": 1,
      "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern": 1,
      "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern_to_ignore": 1,
      "cdc manage-server-group | flags: --add-to-ignore-list=\"test_pattern": 1,
      "cdc manage-server-group | flags: --add-to-schema-excludes\",": 1,
      "cdc manage-server-group | flags: --add-to-schema-excludes=\"schema_to_exclude": 1,
      "cdc manage-server-group | flags: --add-to-schema-excludes=\"test_schema": 1,
      "cdc manage-server-group | flags: --all, --update": 1,
      "cdc manage-server-group | flags: --create=asma, --pattern=db-shared": 1,
      "cdc manage-server-group | flags: --create=my-group": 6,
      "cdc manage-server-group | flags: --create=test-group": 1,
      "cdc manage-server-group | flags: --info": 2,
      "cdc manage-server-group | flags: --info\",": 1,
      "cdc manage-server-group | flags: --list": 3,
      "cdc manage-server-group | flags: --list-extraction-patterns": 1,
      "cdc manage-server-group | flags: --list-extraction-patterns=SERVER": 1,
      "cdc manage-server-group | flags: --list-extraction-patterns=prod": 3,
      "cdc manage-server-group | flags: --list-ignore-patterns": 2,
      "cdc manage-server-group | flags: --list-schema-excludes": 1,
      "cdc manage-server-group | flags: --refresh": 1,
      "cdc manage-server-group | flags: --server-group=adopus, --update": 1,
      "cdc manage-server-group | flags: --server-group=asma, --update": 1,
      "cdc manage-server-group | flags: --update": 11,
      "cdc manage-server-group | flags: --update\",": 1,
      "cdc manage-server-group | flags: --update=default": 1,
      "cdc manage-server-group | flags: --update=prod": 1,
      "cdc manage-service": 3,
      "cdc manage-service directory | flags: --, --inspect": 1,
      "cdc manage-service directory | flags: --, --sink=sink_asma.proxy": 1,
      "cdc manage-service | args: dbo.Fraver | flags: --add-source-tables=dbo.Actor, --service=adopus": 1,
      "cdc manage-service | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1,
      "cdc manage-service | flags: --, --add-sink-table=pub.A, --sink=asma": 1,
      "cdc manage-service | flags: --, --add-sink-table=pub.Actor": 1,
      "cdc manage-service | flags: --, --add-sink-table=pub.Actor, --service=dir": 1,
      "cdc manage-service | flags: --, --add-source-table=Actor": 1,
      "cdc manage-service | flags: --, --add-source-table=Actor, --inspect": 1,
      "cdc manage-service | flags: --, --modify-custom-table=tbl": 1,
      "cdc manage-service | flags: --, --sink-table=pub.Actor, --sink=asma": 1,
      "cdc manage-service | flags: --, --sink-table=t, --sink=a": 1,
      "cdc manage-service | flags: --, --sink=sink_asma.proxy": 3,
      "cdc manage-service | flags: --, --source-table=Actor": 1,
      "cdc manage-service | flags: --add-column-template=tmpl, --sink-": 1,
      "cdc manage-service | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1,
      "cdc manage-service | flags: --add-sink-table=pub.Actor, --map-": 1,
      "cdc manage-service | flags: --add-sink-table=pub.Actor, --sink-": 1,
      "cdc manage-service | flags: --add-sink=sink_asma.chat\",, --service=directory": 1,
      "cdc manage-service | flags: --add-sink=sink_asma.chat, --service=directory": 1,
      "cdc manage-service | flags: --add-source-table=dbo.Actor\",, --service=adopus": 1,
      "cdc manage-service | flags: --add-source-table=dbo.Actor, --service=adopus": 1,
      "cdc manage-service | flags: --add-source-table=dbo.Orders, --service=myservice": 1,
      "cdc manage-service | flags: --add-source-table=dbo.Users, --service=myservice": 3,
      "cdc manage-service | flags: --add-source-table=public.users, --service=proxy": 2,
      "cdc manage-service | flags: --add-table=Actor, --primary-key=actno, --service=adopus": 5,
      "cdc manage-service | flags: --add-table=Actor, --service=adopus": 2,
      "cdc manage-service | flags: --add-table=Fraver, --primary-key=fraverid, --service=adopus": 1,
      "cdc manage-service | flags: --add-table=MyTable, --primary-key=id, --service=my-service": 1,
      "cdc manage-service | flags: --add-table=Orders, --primary-key=order_id, --service=my-service": 1,
      "cdc manage-service | flags: --add-table=Users, --primary-key=id, --service=my-service": 1,
      "cdc manage-service | flags: --add-validation-database=AdOpusTest\",, --create-service=adopus": 1,
      "cdc manage-service | flags: --all\",, --inspect, --service=adopus": 1,
      "cdc manage-service | flags: --all, --generate-validation, --service=adopus": 1,
      "cdc manage-service | flags: --all, --inspect, --service=adopus": 2,
      "cdc manage-service | flags: --all, --inspect, --service=myservice": 1,
      "cdc manage-service | flags: --all, --inspect-mssql, --service=adopus": 1,
      "cdc manage-service | flags: --all, --inspect-sink=sink_asma.calendar, --service=directory": 1,
      "cdc manage-service | flags: --create-service, --server=analytics, --service=analytics_data": 1,
      "cdc manage-service | flags: --create-service, --service=myservice": 2,
      "cdc manage-service | flags: --create-service=myservice\",": 1,
      "cdc manage-service | flags: --create=adopus, --server-group=adopus": 1,
      "cdc manage-service | flags: --create=my-service": 1,
      "cdc manage-service | flags: --create=my-service, --server-group=my-group": 1,
      "cdc manage-service | flags: --create=my-service, --server-group=my-server-group": 1,
      "cdc manage-service | flags: --env=prod\",, --inspect, --service=adopus": 1,
      "cdc manage-service | flags: --inspect, --save, --schema=dbo, --service=myservice": 1,
      "cdc manage-service | flags: --inspect, --schema=dbo\",, --service=adopus": 1,
      "cdc manage-service | flags: --inspect, --schema=dbo, --service=adopus": 5,
      "cdc manage-service | flags: --inspect, --schema=dbo, --service=my-service": 1,
      "cdc manage-service | flags: --inspect, --schema=dbo, --service=myservice": 1,
      "cdc manage-service | flags: --inspect, --service=myservice": 1,
      "cdc manage-service | flags: --inspect-mssql, --schema=dbo, --service=adopus": 2,
      "cdc manage-service | flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory": 1,
      "cdc manage-service | flags: --list-services\",": 1,
      "cdc manage-service | flags: --list-sinks, --service=directory": 1,
      "cdc manage-service | flags: --remove-service=myservice": 1,
      "cdc manage-service | flags: --remove-service=myservice\",": 1,
      "cdc manage-service | flags: --remove-sink=sink_asma.chat\",, --service=directory": 1,
      "cdc manage-service | flags: --remove-table=Test, --service=adopus": 2,
      "cdc manage-service | flags: --remove-table=dbo.Actor\",, --service=adopus": 1,
      "cdc manage-service | flags: --remove-table=dbo.Actor, --service=adopus": 1,
      "cdc manage-service | flags: --runtime, --service=directory, --validate-bloblang": 2,
      "cdc manage-service | flags: --runtime, --service=directory, --validate-config": 1,
      "cdc manage-service | flags: --service=adopus": 2,
      "cdc manage-service | flags: --service=adopus, --validate-config": 2,
      "cdc manage-service | flags: --service=directory": 18,
      "cdc manage-service | flags: --service=directory, --validate-sinks": 1,
      "cdc manage-service | flags: --service=proxy": 1,
      "cdc manage-service | flags: --source-table=Actor, --track-": 1,
      "cdc manage-service-schema": 3,
      "cdc manage-service-schema | flags: --list": 1,
      "cdc manage-service-schema | flags: --list, --service=chat": 2,
      "cdc manage-service-schema | flags: --list-custom-tables, --service=calendar": 1,
      "cdc manage-service-schema | flags: --list-services": 2,
      "cdc manage-service-schema | flags: --remove-custom-table=public.my_events, --service=calendar": 1,
      "cdc manage-service-schema | flags: --service=calendar": 1,
      "cdc manage-service-schema | flags: --service=calendar, --show=public.my_events": 1,
      "cdc manage-service-schema | flags: --service=chat": 7,
      "cdc manage-services config": 3,
      "cdc manage-services config directory": 11,
      "cdc manage-services config directory | flags: --, --all": 1,
      "cdc manage-services config directory | flags: --, --all, --inspect-sink": 1,
      "cdc manage-services config directory | flags: --, --inspect": 1,
      "cdc manage-services config directory | flags: --, --sink=sink_asma.proxy": 1,
      "cdc manage-services config | flags: --": 3,
      "cdc manage-services config | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1,
      "cdc manage-services config | flags: --, --add-sink-table=pub.A, --sink=asma": 1,
      "cdc manage-services config | flags: --, --add-sink-table=pub.Actor, --service=dir": 1,
      "cdc manage-services config | flags: --, --add-source-table=Actor": 1,
      "cdc manage-services config | flags: --, --add-source-table=Actor, --inspect": 1,
      "cdc manage-services config | flags: --, --modify-custom-table=tbl": 1,
      "cdc manage-services config | flags: --, --sink-table=pub.Actor, --sink=asma": 1,
      "cdc manage-services config | flags: --, --sink-table=t, --sink=a": 1,
      "cdc manage-services config | flags: --, --sink=sink_asma.directory": 1,
      "cdc manage-services config | flags: --, --sink=sink_asma.proxy": 3,
      "cdc manage-services config | flags: --, --source-table=Actor": 1,
      "cdc manage-services config | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1,
      "cdc manage-services config | flags: --add-sink-table, --fr": 1,
      "cdc manage-services config | flags: --add-sink-table=pub.Actor, --map-": 1,
      "cdc manage-services config | flags: --add-sink-table=pub.Actor, --sink-": 1,
      "cdc manage-services config | flags: --add-sink-table=public.": 1,
      "cdc manage-services config | flags: --add-source-table=dbo.": 3,
      "cdc manage-services config | flags: --add-source-table=dbo., --add-source-table=dbo.Address": 1,
      "cdc manage-services config | flags: --create-service=directory": 1,
      "cdc manage-services config | flags: --inspect, --service=myservice": 1,
      "cdc manage-services config | flags: --source-table=Actor, --track-": 1,
      "cdc manage-services schema custom-tables": 6,
      "cdc manage-services schema custom-tables | flags: --service=n": 1,
      "cdc manage-sink-groups": 2,
      "cdc manage-sink-groups | flags: --add-new-sink-group=analytics": 1,
      "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --for-source-group=foo, --type=postgres": 1,
      "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --type=postgres": 3,
      "cdc manage-sink-groups | flags: --add-server=default, --sink-group=sink_analytics": 2,
      "cdc manage-sink-groups | flags: --add-server=prod, --sink-group=sink_analytics": 1,
      "cdc manage-sink-groups | flags: --add-to-ignore-list=temp_%\",": 1,
      "cdc manage-sink-groups | flags: --add-to-schema-excludes=hdb_catalog\",": 1,
      "cdc manage-sink-groups | flags: --create": 3,
      "cdc manage-sink-groups | flags: --create, --source-group=asma\",": 1,
      "cdc manage-sink-groups | flags: --create, --source-group=foo": 4,
      "cdc manage-sink-groups | flags: --info=sink_analytics": 3,
      "cdc manage-sink-groups | flags: --info=sink_asma\",": 1,
      "cdc manage-sink-groups | flags: --info=sink_foo": 1,
      "cdc manage-sink-groups | flags: --introspect-types, --sink-group=sink_analytics": 1,
      "cdc manage-sink-groups | flags: --list": 3,
      "cdc manage-sink-groups | flags: --remove-server=default, --sink-group=sink_analytics": 2,
      "cdc manage-sink-groups | flags: --remove=sink_analytics": 2,
      "cdc manage-sink-groups | flags: --remove=sink_test\",": 1,
      "cdc manage-sink-groups | flags: --sink-group=sink_analytics, --update": 2,
      "cdc manage-sink-groups | flags: --sink-group=sink_asma": 8,
      "cdc manage-sink-groups | flags: --sink-group=sink_asma, --update": 1,
      "cdc manage-sink-groups | flags: --validate": 3,
      "cdc manage-source-groups": 7,
      "cdc manage-source-groups | args: 2 | flags: --remove-extraction-pattern=prod": 3,
      "cdc manage-source-groups | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1,
      "cdc manage-source-groups | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1,
      "cdc manage-source-groups | flags: --, --add-server=srv1": 1,
      "cdc manage-source-groups | flags: --, --introspect-types": 1,
      "cdc manage-source-groups | flags: --add-extraction-pattern=default": 2,
      "cdc manage-source-groups | flags: --add-extraction-pattern=prod": 4,
      "cdc manage-source-groups | flags: --add-server=analytics, --source-type=postgres": 2,
      "cdc manage-source-groups | flags: --add-to-ignore-list\",": 1,
      "cdc manage-source-groups | flags: --add-to-ignore-list=\"pattern_to_ignore": 1,
      "cdc manage-source-groups | flags: --add-to-ignore-list=\"test_pattern": 1,
      "cdc manage-source-groups | flags: --add-to-schema-excludes\",": 1,
      "cdc manage-source-groups | flags: --add-to-schema-excludes=\"schema_to_exclude": 1,
      "cdc manage-source-groups | flags: --add-to-schema-excludes=\"test_schema": 1,
      "cdc manage-source-groups | flags: --all, --update": 1,
      "cdc manage-source-groups | flags: --create=asma, --pattern=db-shared": 1,
      "cdc manage-source-groups | flags: --create=my-group": 4,
      "cdc manage-source-groups | flags: --create=test-group": 1,
      "cdc manage-source-groups | flags: --info": 3,
      "cdc manage-source-groups | flags: --info\",": 1,
      "cdc manage-source-groups | flags: --list": 1,
      "cdc manage-source-groups | flags: --list-extraction-patterns": 1,
      "cdc manage-source-groups | flags: --list-extraction-patterns=SERVER": 1,
      "cdc manage-source-groups | flags: --list-extraction-patterns=prod": 4,
      "cdc manage-source-groups | flags: --list-ignore-patterns": 1,
      "cdc manage-source-groups | flags: --list-schema-excludes": 1,
      "cdc manage-source-groups | flags: --set-extraction-pattern=default": 2,
      "cdc manage-source-groups | flags: --update": 10,
      "cdc manage-source-groups | flags: --update\",": 1,
      "cdc manage-source-groups | flags: --update=default": 1,
      "cdc manage-source-groups | flags: --update=prod": 1,
      "cdc reload-cdc-autocompletions": 2,
      "cdc scaffold adopus": 2,
      "cdc scaffold asma": 2,
      "cdc scaffold my-group": 3,
      "cdc scaffold myproject": 2,
      "cdc scaffold myproject | flags: --pattern=db-shared, --source-type=postgres": 2,
      "cdc scaffold | flags: --implementation=test, --pattern=db-shared": 1,
      "cdc setup-local": 1,
      "cdc setup-local | flags: --enable-local-sink": 2,
      "cdc setup-local | flags: --enable-local-sink, --enable-local-source": 2,
      "cdc setup-local | flags: --enable-local-source": 2,
      "cdc setup-local | flags: --full": 2,
      "cdc test": 2,
      "cdc test tests/cli/test_scaffold.py": 1,
      "cdc test | flags: --all": 2,
      "cdc test | flags: --cli": 2,
      "cdc test | flags: --fast-pipelines": 2,
      "cdc test | flags: --full-pipelines": 2,
      "cdc test | flags: -k=scaffold": 2,
      "cdc test | flags: -v": 2,
      "cdc validate": 3
    }
  }
}
```

## Human Readable

Generated: 2026-02-22T19:35:01+00:00

### igor.efrem

| command | count |
| --- | ---: |
| cdc manage-service \| flags: --service=directory | 18 |
| cdc generate | 15 |
| cdc manage-server-group \| flags: --update | 11 |
| cdc manage-services config directory | 11 |
| cdc manage-source-groups \| flags: --update | 10 |
| cdc manage-sink-groups \| flags: --sink-group=sink_asma | 8 |
| cdc manage-service-schema \| flags: --service=chat | 7 |
| cdc manage-source-groups | 7 |
| cdc manage-server-group \| flags: --create=my-group | 6 |
| cdc manage-services schema custom-tables | 6 |
| cdc manage-server-group | 5 |
| cdc manage-service \| flags: --add-table=Actor, --primary-key=actno, --service=adopus | 5 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=adopus | 5 |
| cdc manage-column-templates \| flags: --add=tenant_id | 4 |
| cdc manage-sink-groups \| flags: --create, --source-group=foo | 4 |
| cdc manage-source-groups \| flags: --add-extraction-pattern=prod | 4 |
| cdc manage-source-groups \| flags: --create=my-group | 4 |
| cdc manage-source-groups \| flags: --list-extraction-patterns=prod | 4 |
| cdc init | 3 |
| cdc manage-column-templates \| flags: --edit=tenant_id | 3 |
| cdc manage-column-templates \| flags: --list | 3 |
| cdc manage-column-templates \| flags: --remove=tenant_id | 3 |
| cdc manage-column-templates \| flags: --show=tenant_id | 3 |
| cdc manage-server-group \| flags: --list | 3 |
| cdc manage-server-group \| flags: --list-extraction-patterns=prod | 3 |
| cdc manage-service | 3 |
| cdc manage-service \| flags: --, --sink=sink_asma.proxy | 3 |
| cdc manage-service \| flags: --add-source-table=dbo.Users, --service=myservice | 3 |
| cdc manage-service-schema | 3 |
| cdc manage-services config | 3 |
| cdc manage-services config \| flags: -- | 3 |
| cdc manage-services config \| flags: --, --sink=sink_asma.proxy | 3 |
| cdc manage-services config \| flags: --add-source-table=dbo. | 3 |
| cdc manage-sink-groups \| flags: --add-new-sink-group=analytics, --type=postgres | 3 |
| cdc manage-sink-groups \| flags: --create | 3 |
| cdc manage-sink-groups \| flags: --info=sink_analytics | 3 |
| cdc manage-sink-groups \| flags: --list | 3 |
| cdc manage-sink-groups \| flags: --validate | 3 |
| cdc manage-source-groups \| args: 2 \| flags: --remove-extraction-pattern=prod | 3 |
| cdc manage-source-groups \| flags: --info | 3 |
| cdc scaffold my-group | 3 |
| cdc validate | 3 |
| cdc generate \| flags: --environment=dev, --service=my-service | 2 |
| cdc init \| flags: --name=adopus-cdc, --type=adopus | 2 |
| cdc init \| flags: --name=asma-cdc, --type=asma | 2 |
| cdc manage-server-group \| args: 2 \| flags: --remove-extraction-pattern=prod | 2 |
| cdc manage-server-group \| flags: --info | 2 |
| cdc manage-server-group \| flags: --list-ignore-patterns | 2 |
| cdc manage-service \| flags: --add-source-table=public.users, --service=proxy | 2 |
| cdc manage-service \| flags: --add-table=Actor, --service=adopus | 2 |
| cdc manage-service \| flags: --all, --inspect, --service=adopus | 2 |
| cdc manage-service \| flags: --create-service, --service=myservice | 2 |
| cdc manage-service \| flags: --inspect-mssql, --schema=dbo, --service=adopus | 2 |
| cdc manage-service \| flags: --remove-table=Test, --service=adopus | 2 |
| cdc manage-service \| flags: --runtime, --service=directory, --validate-bloblang | 2 |
| cdc manage-service \| flags: --service=adopus | 2 |
| cdc manage-service \| flags: --service=adopus, --validate-config | 2 |
| cdc manage-service-schema \| flags: --list, --service=chat | 2 |
| cdc manage-service-schema \| flags: --list-services | 2 |
| cdc manage-sink-groups | 2 |
| cdc manage-sink-groups \| flags: --add-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --sink-group=sink_analytics, --update | 2 |
| cdc manage-source-groups \| flags: --add-extraction-pattern=default | 2 |
| cdc manage-source-groups \| flags: --add-server=analytics, --source-type=postgres | 2 |
| cdc manage-source-groups \| flags: --set-extraction-pattern=default | 2 |
| cdc reload-cdc-autocompletions | 2 |
| cdc scaffold adopus | 2 |
| cdc scaffold asma | 2 |
| cdc scaffold myproject | 2 |
| cdc scaffold myproject \| flags: --pattern=db-shared, --source-type=postgres | 2 |
| cdc setup-local \| flags: --enable-local-sink | 2 |
| cdc setup-local \| flags: --enable-local-sink, --enable-local-source | 2 |
| cdc setup-local \| flags: --enable-local-source | 2 |
| cdc setup-local \| flags: --full | 2 |
| cdc test | 2 |
| cdc test \| flags: --all | 2 |
| cdc test \| flags: --cli | 2 |
| cdc test \| flags: --fast-pipelines | 2 |
| cdc test \| flags: --full-pipelines | 2 |
| cdc test \| flags: -k=scaffold | 2 |
| cdc test \| flags: -v | 2 |
| cdc generate \| flags: --environment=local, --service=adopus | 1 |
| cdc init \| flags: --git-init, --name=my-project, --type=adopus | 1 |
| cdc init \| flags: --name=PROJECT_NAME | 1 |
| cdc init \| flags: --name=my-project, --target-dir=/path/to/project, --type=adopus | 1 |
| cdc init \| flags: --name=my-project, --type=adopus | 1 |
| cdc manage-column-templates \| flags: --add=sync_timestamp | 1 |
| cdc manage-column-templates \| flags: --remove=tenant_id", | 1 |
| cdc manage-column-templates \| flags: --show=tenant_id", | 1 |
| cdc manage-pipelines stress-test | 1 |
| cdc manage-pipelines verify-sync | 1 |
| cdc manage-server-group \| args: INDEX \| flags: --remove-extraction-pattern=SERVER | 1 |
| cdc manage-server-group \| args: PATTERN \| flags: --add-extraction-pattern=SERVER | 1 |
| cdc manage-server-group \| flags: --add-group=adopus | 1 |
| cdc manage-server-group \| flags: --add-server=analytics, --source-type=postgres | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list", | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list="pattern | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list="pattern_to_ignore | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list="test_pattern | 1 |
| cdc manage-server-group \| flags: --add-to-schema-excludes", | 1 |
| cdc manage-server-group \| flags: --add-to-schema-excludes="schema_to_exclude | 1 |
| cdc manage-server-group \| flags: --add-to-schema-excludes="test_schema | 1 |
| cdc manage-server-group \| flags: --all, --update | 1 |
| cdc manage-server-group \| flags: --create=asma, --pattern=db-shared | 1 |
| cdc manage-server-group \| flags: --create=test-group | 1 |
| cdc manage-server-group \| flags: --info", | 1 |
| cdc manage-server-group \| flags: --list-extraction-patterns | 1 |
| cdc manage-server-group \| flags: --list-extraction-patterns=SERVER | 1 |
| cdc manage-server-group \| flags: --list-schema-excludes | 1 |
| cdc manage-server-group \| flags: --refresh | 1 |
| cdc manage-server-group \| flags: --server-group=adopus, --update | 1 |
| cdc manage-server-group \| flags: --server-group=asma, --update | 1 |
| cdc manage-server-group \| flags: --update", | 1 |
| cdc manage-server-group \| flags: --update=default | 1 |
| cdc manage-server-group \| flags: --update=prod | 1 |
| cdc manage-service directory \| flags: --, --inspect | 1 |
| cdc manage-service directory \| flags: --, --sink=sink_asma.proxy | 1 |
| cdc manage-service \| args: dbo.Fraver \| flags: --add-source-tables=dbo.Actor, --service=adopus | 1 |
| cdc manage-service \| flags: --, --add-column-template=tpl, --sink-table=t, --sink=a | 1 |
| cdc manage-service \| flags: --, --add-sink-table=pub.A, --sink=asma | 1 |
| cdc manage-service \| flags: --, --add-sink-table=pub.Actor | 1 |
| cdc manage-service \| flags: --, --add-sink-table=pub.Actor, --service=dir | 1 |
| cdc manage-service \| flags: --, --add-source-table=Actor | 1 |
| cdc manage-service \| flags: --, --add-source-table=Actor, --inspect | 1 |
| cdc manage-service \| flags: --, --modify-custom-table=tbl | 1 |
| cdc manage-service \| flags: --, --sink-table=pub.Actor, --sink=asma | 1 |
| cdc manage-service \| flags: --, --sink-table=t, --sink=a | 1 |
| cdc manage-service \| flags: --, --source-table=Actor | 1 |
| cdc manage-service \| flags: --add-column-template=tmpl, --sink- | 1 |
| cdc manage-service \| flags: --add-column-template=tmpl, --sink-, --sink=asma | 1 |
| cdc manage-service \| flags: --add-sink-table=pub.Actor, --map- | 1 |
| cdc manage-service \| flags: --add-sink-table=pub.Actor, --sink- | 1 |
| cdc manage-service \| flags: --add-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-service \| flags: --add-sink=sink_asma.chat, --service=directory | 1 |
| cdc manage-service \| flags: --add-source-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-service \| flags: --add-source-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-service \| flags: --add-source-table=dbo.Orders, --service=myservice | 1 |
| cdc manage-service \| flags: --add-table=Fraver, --primary-key=fraverid, --service=adopus | 1 |
| cdc manage-service \| flags: --add-table=MyTable, --primary-key=id, --service=my-service | 1 |
| cdc manage-service \| flags: --add-table=Orders, --primary-key=order_id, --service=my-service | 1 |
| cdc manage-service \| flags: --add-table=Users, --primary-key=id, --service=my-service | 1 |
| cdc manage-service \| flags: --add-validation-database=AdOpusTest",, --create-service=adopus | 1 |
| cdc manage-service \| flags: --all",, --inspect, --service=adopus | 1 |
| cdc manage-service \| flags: --all, --generate-validation, --service=adopus | 1 |
| cdc manage-service \| flags: --all, --inspect, --service=myservice | 1 |
| cdc manage-service \| flags: --all, --inspect-mssql, --service=adopus | 1 |
| cdc manage-service \| flags: --all, --inspect-sink=sink_asma.calendar, --service=directory | 1 |
| cdc manage-service \| flags: --create-service, --server=analytics, --service=analytics_data | 1 |
| cdc manage-service \| flags: --create-service=myservice", | 1 |
| cdc manage-service \| flags: --create=adopus, --server-group=adopus | 1 |
| cdc manage-service \| flags: --create=my-service | 1 |
| cdc manage-service \| flags: --create=my-service, --server-group=my-group | 1 |
| cdc manage-service \| flags: --create=my-service, --server-group=my-server-group | 1 |
| cdc manage-service \| flags: --env=prod",, --inspect, --service=adopus | 1 |
| cdc manage-service \| flags: --inspect, --save, --schema=dbo, --service=myservice | 1 |
| cdc manage-service \| flags: --inspect, --schema=dbo",, --service=adopus | 1 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=my-service | 1 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=myservice | 1 |
| cdc manage-service \| flags: --inspect, --service=myservice | 1 |
| cdc manage-service \| flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory | 1 |
| cdc manage-service \| flags: --list-services", | 1 |
| cdc manage-service \| flags: --list-sinks, --service=directory | 1 |
| cdc manage-service \| flags: --remove-service=myservice | 1 |
| cdc manage-service \| flags: --remove-service=myservice", | 1 |
| cdc manage-service \| flags: --remove-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-service \| flags: --remove-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-service \| flags: --remove-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-service \| flags: --runtime, --service=directory, --validate-config | 1 |
| cdc manage-service \| flags: --service=directory, --validate-sinks | 1 |
| cdc manage-service \| flags: --service=proxy | 1 |
| cdc manage-service \| flags: --source-table=Actor, --track- | 1 |
| cdc manage-service-schema \| flags: --list | 1 |
| cdc manage-service-schema \| flags: --list-custom-tables, --service=calendar | 1 |
| cdc manage-service-schema \| flags: --remove-custom-table=public.my_events, --service=calendar | 1 |
| cdc manage-service-schema \| flags: --service=calendar | 1 |
| cdc manage-service-schema \| flags: --service=calendar, --show=public.my_events | 1 |
| cdc manage-services config directory \| flags: --, --all | 1 |
| cdc manage-services config directory \| flags: --, --all, --inspect-sink | 1 |
| cdc manage-services config directory \| flags: --, --inspect | 1 |
| cdc manage-services config directory \| flags: --, --sink=sink_asma.proxy | 1 |
| cdc manage-services config \| flags: --, --add-column-template=tpl, --sink-table=t, --sink=a | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.A, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.Actor, --service=dir | 1 |
| cdc manage-services config \| flags: --, --add-source-table=Actor | 1 |
| cdc manage-services config \| flags: --, --add-source-table=Actor, --inspect | 1 |
| cdc manage-services config \| flags: --, --modify-custom-table=tbl | 1 |
| cdc manage-services config \| flags: --, --sink-table=pub.Actor, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --sink-table=t, --sink=a | 1 |
| cdc manage-services config \| flags: --, --sink=sink_asma.directory | 1 |
| cdc manage-services config \| flags: --, --source-table=Actor | 1 |
| cdc manage-services config \| flags: --add-column-template=tmpl, --sink-, --sink=asma | 1 |
| cdc manage-services config \| flags: --add-sink-table, --fr | 1 |
| cdc manage-services config \| flags: --add-sink-table=pub.Actor, --map- | 1 |
| cdc manage-services config \| flags: --add-sink-table=pub.Actor, --sink- | 1 |
| cdc manage-services config \| flags: --add-sink-table=public. | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo., --add-source-table=dbo.Address | 1 |
| cdc manage-services config \| flags: --create-service=directory | 1 |
| cdc manage-services config \| flags: --inspect, --service=myservice | 1 |
| cdc manage-services config \| flags: --source-table=Actor, --track- | 1 |
| cdc manage-services schema custom-tables \| flags: --service=n | 1 |
| cdc manage-sink-groups \| flags: --add-new-sink-group=analytics | 1 |
| cdc manage-sink-groups \| flags: --add-new-sink-group=analytics, --for-source-group=foo, --type=postgres | 1 |
| cdc manage-sink-groups \| flags: --add-server=prod, --sink-group=sink_analytics | 1 |
| cdc manage-sink-groups \| flags: --add-to-ignore-list=temp_%", | 1 |
| cdc manage-sink-groups \| flags: --add-to-schema-excludes=hdb_catalog", | 1 |
| cdc manage-sink-groups \| flags: --create, --source-group=asma", | 1 |
| cdc manage-sink-groups \| flags: --info=sink_asma", | 1 |
| cdc manage-sink-groups \| flags: --info=sink_foo | 1 |
| cdc manage-sink-groups \| flags: --introspect-types, --sink-group=sink_analytics | 1 |
| cdc manage-sink-groups \| flags: --remove=sink_test", | 1 |
| cdc manage-sink-groups \| flags: --sink-group=sink_asma, --update | 1 |
| cdc manage-source-groups \| args: INDEX \| flags: --remove-extraction-pattern=SERVER | 1 |
| cdc manage-source-groups \| args: PATTERN \| flags: --add-extraction-pattern=SERVER | 1 |
| cdc manage-source-groups \| flags: --, --add-server=srv1 | 1 |
| cdc manage-source-groups \| flags: --, --introspect-types | 1 |
| cdc manage-source-groups \| flags: --add-to-ignore-list", | 1 |
| cdc manage-source-groups \| flags: --add-to-ignore-list="pattern_to_ignore | 1 |
| cdc manage-source-groups \| flags: --add-to-ignore-list="test_pattern | 1 |
| cdc manage-source-groups \| flags: --add-to-schema-excludes", | 1 |
| cdc manage-source-groups \| flags: --add-to-schema-excludes="schema_to_exclude | 1 |
| cdc manage-source-groups \| flags: --add-to-schema-excludes="test_schema | 1 |
| cdc manage-source-groups \| flags: --all, --update | 1 |
| cdc manage-source-groups \| flags: --create=asma, --pattern=db-shared | 1 |
| cdc manage-source-groups \| flags: --create=test-group | 1 |
| cdc manage-source-groups \| flags: --info", | 1 |
| cdc manage-source-groups \| flags: --list | 1 |
| cdc manage-source-groups \| flags: --list-extraction-patterns | 1 |
| cdc manage-source-groups \| flags: --list-extraction-patterns=SERVER | 1 |
| cdc manage-source-groups \| flags: --list-ignore-patterns | 1 |
| cdc manage-source-groups \| flags: --list-schema-excludes | 1 |
| cdc manage-source-groups \| flags: --update", | 1 |
| cdc manage-source-groups \| flags: --update=default | 1 |
| cdc manage-source-groups \| flags: --update=prod | 1 |
| cdc scaffold \| flags: --implementation=test, --pattern=db-shared | 1 |
| cdc setup-local | 1 |
| cdc test tests/cli/test_scaffold.py | 1 |

### total

| command | count |
| --- | ---: |
| cdc manage-service \| flags: --service=directory | 18 |
| cdc generate | 15 |
| cdc manage-server-group \| flags: --update | 11 |
| cdc manage-services config directory | 11 |
| cdc manage-source-groups \| flags: --update | 10 |
| cdc manage-sink-groups \| flags: --sink-group=sink_asma | 8 |
| cdc manage-service-schema \| flags: --service=chat | 7 |
| cdc manage-source-groups | 7 |
| cdc manage-server-group \| flags: --create=my-group | 6 |
| cdc manage-services schema custom-tables | 6 |
| cdc manage-server-group | 5 |
| cdc manage-service \| flags: --add-table=Actor, --primary-key=actno, --service=adopus | 5 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=adopus | 5 |
| cdc manage-column-templates \| flags: --add=tenant_id | 4 |
| cdc manage-sink-groups \| flags: --create, --source-group=foo | 4 |
| cdc manage-source-groups \| flags: --add-extraction-pattern=prod | 4 |
| cdc manage-source-groups \| flags: --create=my-group | 4 |
| cdc manage-source-groups \| flags: --list-extraction-patterns=prod | 4 |
| cdc init | 3 |
| cdc manage-column-templates \| flags: --edit=tenant_id | 3 |
| cdc manage-column-templates \| flags: --list | 3 |
| cdc manage-column-templates \| flags: --remove=tenant_id | 3 |
| cdc manage-column-templates \| flags: --show=tenant_id | 3 |
| cdc manage-server-group \| flags: --list | 3 |
| cdc manage-server-group \| flags: --list-extraction-patterns=prod | 3 |
| cdc manage-service | 3 |
| cdc manage-service \| flags: --, --sink=sink_asma.proxy | 3 |
| cdc manage-service \| flags: --add-source-table=dbo.Users, --service=myservice | 3 |
| cdc manage-service-schema | 3 |
| cdc manage-services config | 3 |
| cdc manage-services config \| flags: -- | 3 |
| cdc manage-services config \| flags: --, --sink=sink_asma.proxy | 3 |
| cdc manage-services config \| flags: --add-source-table=dbo. | 3 |
| cdc manage-sink-groups \| flags: --add-new-sink-group=analytics, --type=postgres | 3 |
| cdc manage-sink-groups \| flags: --create | 3 |
| cdc manage-sink-groups \| flags: --info=sink_analytics | 3 |
| cdc manage-sink-groups \| flags: --list | 3 |
| cdc manage-sink-groups \| flags: --validate | 3 |
| cdc manage-source-groups \| args: 2 \| flags: --remove-extraction-pattern=prod | 3 |
| cdc manage-source-groups \| flags: --info | 3 |
| cdc scaffold my-group | 3 |
| cdc validate | 3 |
| cdc generate \| flags: --environment=dev, --service=my-service | 2 |
| cdc init \| flags: --name=adopus-cdc, --type=adopus | 2 |
| cdc init \| flags: --name=asma-cdc, --type=asma | 2 |
| cdc manage-server-group \| args: 2 \| flags: --remove-extraction-pattern=prod | 2 |
| cdc manage-server-group \| flags: --info | 2 |
| cdc manage-server-group \| flags: --list-ignore-patterns | 2 |
| cdc manage-service \| flags: --add-source-table=public.users, --service=proxy | 2 |
| cdc manage-service \| flags: --add-table=Actor, --service=adopus | 2 |
| cdc manage-service \| flags: --all, --inspect, --service=adopus | 2 |
| cdc manage-service \| flags: --create-service, --service=myservice | 2 |
| cdc manage-service \| flags: --inspect-mssql, --schema=dbo, --service=adopus | 2 |
| cdc manage-service \| flags: --remove-table=Test, --service=adopus | 2 |
| cdc manage-service \| flags: --runtime, --service=directory, --validate-bloblang | 2 |
| cdc manage-service \| flags: --service=adopus | 2 |
| cdc manage-service \| flags: --service=adopus, --validate-config | 2 |
| cdc manage-service-schema \| flags: --list, --service=chat | 2 |
| cdc manage-service-schema \| flags: --list-services | 2 |
| cdc manage-sink-groups | 2 |
| cdc manage-sink-groups \| flags: --add-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --sink-group=sink_analytics, --update | 2 |
| cdc manage-source-groups \| flags: --add-extraction-pattern=default | 2 |
| cdc manage-source-groups \| flags: --add-server=analytics, --source-type=postgres | 2 |
| cdc manage-source-groups \| flags: --set-extraction-pattern=default | 2 |
| cdc reload-cdc-autocompletions | 2 |
| cdc scaffold adopus | 2 |
| cdc scaffold asma | 2 |
| cdc scaffold myproject | 2 |
| cdc scaffold myproject \| flags: --pattern=db-shared, --source-type=postgres | 2 |
| cdc setup-local \| flags: --enable-local-sink | 2 |
| cdc setup-local \| flags: --enable-local-sink, --enable-local-source | 2 |
| cdc setup-local \| flags: --enable-local-source | 2 |
| cdc setup-local \| flags: --full | 2 |
| cdc test | 2 |
| cdc test \| flags: --all | 2 |
| cdc test \| flags: --cli | 2 |
| cdc test \| flags: --fast-pipelines | 2 |
| cdc test \| flags: --full-pipelines | 2 |
| cdc test \| flags: -k=scaffold | 2 |
| cdc test \| flags: -v | 2 |
| cdc generate \| flags: --environment=local, --service=adopus | 1 |
| cdc init \| flags: --git-init, --name=my-project, --type=adopus | 1 |
| cdc init \| flags: --name=PROJECT_NAME | 1 |
| cdc init \| flags: --name=my-project, --target-dir=/path/to/project, --type=adopus | 1 |
| cdc init \| flags: --name=my-project, --type=adopus | 1 |
| cdc manage-column-templates \| flags: --add=sync_timestamp | 1 |
| cdc manage-column-templates \| flags: --remove=tenant_id", | 1 |
| cdc manage-column-templates \| flags: --show=tenant_id", | 1 |
| cdc manage-pipelines stress-test | 1 |
| cdc manage-pipelines verify-sync | 1 |
| cdc manage-server-group \| args: INDEX \| flags: --remove-extraction-pattern=SERVER | 1 |
| cdc manage-server-group \| args: PATTERN \| flags: --add-extraction-pattern=SERVER | 1 |
| cdc manage-server-group \| flags: --add-group=adopus | 1 |
| cdc manage-server-group \| flags: --add-server=analytics, --source-type=postgres | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list", | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list="pattern | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list="pattern_to_ignore | 1 |
| cdc manage-server-group \| flags: --add-to-ignore-list="test_pattern | 1 |
| cdc manage-server-group \| flags: --add-to-schema-excludes", | 1 |
| cdc manage-server-group \| flags: --add-to-schema-excludes="schema_to_exclude | 1 |
| cdc manage-server-group \| flags: --add-to-schema-excludes="test_schema | 1 |
| cdc manage-server-group \| flags: --all, --update | 1 |
| cdc manage-server-group \| flags: --create=asma, --pattern=db-shared | 1 |
| cdc manage-server-group \| flags: --create=test-group | 1 |
| cdc manage-server-group \| flags: --info", | 1 |
| cdc manage-server-group \| flags: --list-extraction-patterns | 1 |
| cdc manage-server-group \| flags: --list-extraction-patterns=SERVER | 1 |
| cdc manage-server-group \| flags: --list-schema-excludes | 1 |
| cdc manage-server-group \| flags: --refresh | 1 |
| cdc manage-server-group \| flags: --server-group=adopus, --update | 1 |
| cdc manage-server-group \| flags: --server-group=asma, --update | 1 |
| cdc manage-server-group \| flags: --update", | 1 |
| cdc manage-server-group \| flags: --update=default | 1 |
| cdc manage-server-group \| flags: --update=prod | 1 |
| cdc manage-service directory \| flags: --, --inspect | 1 |
| cdc manage-service directory \| flags: --, --sink=sink_asma.proxy | 1 |
| cdc manage-service \| args: dbo.Fraver \| flags: --add-source-tables=dbo.Actor, --service=adopus | 1 |
| cdc manage-service \| flags: --, --add-column-template=tpl, --sink-table=t, --sink=a | 1 |
| cdc manage-service \| flags: --, --add-sink-table=pub.A, --sink=asma | 1 |
| cdc manage-service \| flags: --, --add-sink-table=pub.Actor | 1 |
| cdc manage-service \| flags: --, --add-sink-table=pub.Actor, --service=dir | 1 |
| cdc manage-service \| flags: --, --add-source-table=Actor | 1 |
| cdc manage-service \| flags: --, --add-source-table=Actor, --inspect | 1 |
| cdc manage-service \| flags: --, --modify-custom-table=tbl | 1 |
| cdc manage-service \| flags: --, --sink-table=pub.Actor, --sink=asma | 1 |
| cdc manage-service \| flags: --, --sink-table=t, --sink=a | 1 |
| cdc manage-service \| flags: --, --source-table=Actor | 1 |
| cdc manage-service \| flags: --add-column-template=tmpl, --sink- | 1 |
| cdc manage-service \| flags: --add-column-template=tmpl, --sink-, --sink=asma | 1 |
| cdc manage-service \| flags: --add-sink-table=pub.Actor, --map- | 1 |
| cdc manage-service \| flags: --add-sink-table=pub.Actor, --sink- | 1 |
| cdc manage-service \| flags: --add-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-service \| flags: --add-sink=sink_asma.chat, --service=directory | 1 |
| cdc manage-service \| flags: --add-source-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-service \| flags: --add-source-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-service \| flags: --add-source-table=dbo.Orders, --service=myservice | 1 |
| cdc manage-service \| flags: --add-table=Fraver, --primary-key=fraverid, --service=adopus | 1 |
| cdc manage-service \| flags: --add-table=MyTable, --primary-key=id, --service=my-service | 1 |
| cdc manage-service \| flags: --add-table=Orders, --primary-key=order_id, --service=my-service | 1 |
| cdc manage-service \| flags: --add-table=Users, --primary-key=id, --service=my-service | 1 |
| cdc manage-service \| flags: --add-validation-database=AdOpusTest",, --create-service=adopus | 1 |
| cdc manage-service \| flags: --all",, --inspect, --service=adopus | 1 |
| cdc manage-service \| flags: --all, --generate-validation, --service=adopus | 1 |
| cdc manage-service \| flags: --all, --inspect, --service=myservice | 1 |
| cdc manage-service \| flags: --all, --inspect-mssql, --service=adopus | 1 |
| cdc manage-service \| flags: --all, --inspect-sink=sink_asma.calendar, --service=directory | 1 |
| cdc manage-service \| flags: --create-service, --server=analytics, --service=analytics_data | 1 |
| cdc manage-service \| flags: --create-service=myservice", | 1 |
| cdc manage-service \| flags: --create=adopus, --server-group=adopus | 1 |
| cdc manage-service \| flags: --create=my-service | 1 |
| cdc manage-service \| flags: --create=my-service, --server-group=my-group | 1 |
| cdc manage-service \| flags: --create=my-service, --server-group=my-server-group | 1 |
| cdc manage-service \| flags: --env=prod",, --inspect, --service=adopus | 1 |
| cdc manage-service \| flags: --inspect, --save, --schema=dbo, --service=myservice | 1 |
| cdc manage-service \| flags: --inspect, --schema=dbo",, --service=adopus | 1 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=my-service | 1 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=myservice | 1 |
| cdc manage-service \| flags: --inspect, --service=myservice | 1 |
| cdc manage-service \| flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory | 1 |
| cdc manage-service \| flags: --list-services", | 1 |
| cdc manage-service \| flags: --list-sinks, --service=directory | 1 |
| cdc manage-service \| flags: --remove-service=myservice | 1 |
| cdc manage-service \| flags: --remove-service=myservice", | 1 |
| cdc manage-service \| flags: --remove-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-service \| flags: --remove-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-service \| flags: --remove-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-service \| flags: --runtime, --service=directory, --validate-config | 1 |
| cdc manage-service \| flags: --service=directory, --validate-sinks | 1 |
| cdc manage-service \| flags: --service=proxy | 1 |
| cdc manage-service \| flags: --source-table=Actor, --track- | 1 |
| cdc manage-service-schema \| flags: --list | 1 |
| cdc manage-service-schema \| flags: --list-custom-tables, --service=calendar | 1 |
| cdc manage-service-schema \| flags: --remove-custom-table=public.my_events, --service=calendar | 1 |
| cdc manage-service-schema \| flags: --service=calendar | 1 |
| cdc manage-service-schema \| flags: --service=calendar, --show=public.my_events | 1 |
| cdc manage-services config directory \| flags: --, --all | 1 |
| cdc manage-services config directory \| flags: --, --all, --inspect-sink | 1 |
| cdc manage-services config directory \| flags: --, --inspect | 1 |
| cdc manage-services config directory \| flags: --, --sink=sink_asma.proxy | 1 |
| cdc manage-services config \| flags: --, --add-column-template=tpl, --sink-table=t, --sink=a | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.A, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.Actor, --service=dir | 1 |
| cdc manage-services config \| flags: --, --add-source-table=Actor | 1 |
| cdc manage-services config \| flags: --, --add-source-table=Actor, --inspect | 1 |
| cdc manage-services config \| flags: --, --modify-custom-table=tbl | 1 |
| cdc manage-services config \| flags: --, --sink-table=pub.Actor, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --sink-table=t, --sink=a | 1 |
| cdc manage-services config \| flags: --, --sink=sink_asma.directory | 1 |
| cdc manage-services config \| flags: --, --source-table=Actor | 1 |
| cdc manage-services config \| flags: --add-column-template=tmpl, --sink-, --sink=asma | 1 |
| cdc manage-services config \| flags: --add-sink-table, --fr | 1 |
| cdc manage-services config \| flags: --add-sink-table=pub.Actor, --map- | 1 |
| cdc manage-services config \| flags: --add-sink-table=pub.Actor, --sink- | 1 |
| cdc manage-services config \| flags: --add-sink-table=public. | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo., --add-source-table=dbo.Address | 1 |
| cdc manage-services config \| flags: --create-service=directory | 1 |
| cdc manage-services config \| flags: --inspect, --service=myservice | 1 |
| cdc manage-services config \| flags: --source-table=Actor, --track- | 1 |
| cdc manage-services schema custom-tables \| flags: --service=n | 1 |
| cdc manage-sink-groups \| flags: --add-new-sink-group=analytics | 1 |
| cdc manage-sink-groups \| flags: --add-new-sink-group=analytics, --for-source-group=foo, --type=postgres | 1 |
| cdc manage-sink-groups \| flags: --add-server=prod, --sink-group=sink_analytics | 1 |
| cdc manage-sink-groups \| flags: --add-to-ignore-list=temp_%", | 1 |
| cdc manage-sink-groups \| flags: --add-to-schema-excludes=hdb_catalog", | 1 |
| cdc manage-sink-groups \| flags: --create, --source-group=asma", | 1 |
| cdc manage-sink-groups \| flags: --info=sink_asma", | 1 |
| cdc manage-sink-groups \| flags: --info=sink_foo | 1 |
| cdc manage-sink-groups \| flags: --introspect-types, --sink-group=sink_analytics | 1 |
| cdc manage-sink-groups \| flags: --remove=sink_test", | 1 |
| cdc manage-sink-groups \| flags: --sink-group=sink_asma, --update | 1 |
| cdc manage-source-groups \| args: INDEX \| flags: --remove-extraction-pattern=SERVER | 1 |
| cdc manage-source-groups \| args: PATTERN \| flags: --add-extraction-pattern=SERVER | 1 |
| cdc manage-source-groups \| flags: --, --add-server=srv1 | 1 |
| cdc manage-source-groups \| flags: --, --introspect-types | 1 |
| cdc manage-source-groups \| flags: --add-to-ignore-list", | 1 |
| cdc manage-source-groups \| flags: --add-to-ignore-list="pattern_to_ignore | 1 |
| cdc manage-source-groups \| flags: --add-to-ignore-list="test_pattern | 1 |
| cdc manage-source-groups \| flags: --add-to-schema-excludes", | 1 |
| cdc manage-source-groups \| flags: --add-to-schema-excludes="schema_to_exclude | 1 |
| cdc manage-source-groups \| flags: --add-to-schema-excludes="test_schema | 1 |
| cdc manage-source-groups \| flags: --all, --update | 1 |
| cdc manage-source-groups \| flags: --create=asma, --pattern=db-shared | 1 |
| cdc manage-source-groups \| flags: --create=test-group | 1 |
| cdc manage-source-groups \| flags: --info", | 1 |
| cdc manage-source-groups \| flags: --list | 1 |
| cdc manage-source-groups \| flags: --list-extraction-patterns | 1 |
| cdc manage-source-groups \| flags: --list-extraction-patterns=SERVER | 1 |
| cdc manage-source-groups \| flags: --list-ignore-patterns | 1 |
| cdc manage-source-groups \| flags: --list-schema-excludes | 1 |
| cdc manage-source-groups \| flags: --update", | 1 |
| cdc manage-source-groups \| flags: --update=default | 1 |
| cdc manage-source-groups \| flags: --update=prod | 1 |
| cdc scaffold \| flags: --implementation=test, --pattern=db-shared | 1 |
| cdc setup-local | 1 |
| cdc test tests/cli/test_scaffold.py | 1 |
