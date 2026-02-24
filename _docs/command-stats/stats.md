# CDC Command Usage Stats

This report counts normalized `cdc ...` command occurrences by git user.
Normalization keeps command path and ignores option/argument ordering.

## Machine Readable

```toon
generated_at_utc: "2026-02-23T00:43:52+00:00"
users:
  "igor.efrem":
    "cdc manage-services config directory 15": 1
    "cdc manage-services config directory | flags: | flags: --al=15": 1
    "cdc msc | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=20": 1
    "cdc manage-services config directory 14": 1
    "cdc manage-services config directory | flags: | flags: --al=14": 1
    "cdc msc | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --sink-schema=2": 1
    "cdc msc | flags: | flags: --create-service=2": 1
    "cdc manage-services config | flags: | flags: --,, --add-column-template,, --add-sink-table,, --sink=1": 1
    "cdc manage-services config | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=1": 1
    "cdc msc | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink,, --sink-schema=1": 1
    "cdc msc | flags: | flags: --help=1": 1
    "cdc msc | flags: --add-sink-table, --from=dbo.Actor": 1
    "cdc manage-services config | flags: --, --add-sink-table=pub.Actor, --sink=asma": 1
    "cdc manage-services config | flags: --, --add-column-template=tpl, --add-sink-table=pub.Actor, --sink=asma": 1
    "cdc manage-services config directory 4": 1
    "cdc manage-services config directory | flags: | flags: --al=4": 1
    "cdc msr custom-tables | flags: | flags: --add-custom-table=customer_id,, --service=directoryt1n": 1
    "cdc msr custom-tables | flags: | flags: --add-custom-tablet2n, --service,": 1
    "cdc msr custom-tables | flags: | flags: --add-custom-table,, --service\":=3,": 1
    "cdc msr custom-tables | flags: | flags: --add-custom-table=customer_id,, --service=directoryt2n": 1
    "cdc msr custom-tables | flags: | flags: --add-custom-tablet1n, --service,": 1
    "cdc ms config | flags: --list-services": 1
    "cdc ms config | flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save": 1
    "cdc manage-services config directory 1": 1
    "cdc manage-services config directory | flags: | flags: --al=1": 1
    "cdc manage-services config directory\": 1,": 2
    "cdc manage-services config directory | flags: | flags: --al\":=1": 2
    "cdc generate | flags: | args: 2, | flags: --environment=dev,, --service=my-service\":": 2
    "cdc generate | flags: | args: 1, | flags: --environment=local,, --service=adopus\":": 2
    "cdc init | flags: | args: 1, | flags: --git-init,, --name=my-project,, --type=adopus\":": 2
    "cdc init | flags: | args: 1, | flags: --name=PROJECT_NAME\":": 2
    "cdc init | flags: | args: 2, | flags: --name=adopus-cdc,, --type=adopus\":": 2
    "cdc init | flags: | args: 2, | flags: --name=asma-cdc,, --type=asma\":": 2
    "cdc init | flags: | args: 1, | flags: --name=my-project,, --target-dir=/path/to/project,, --type=adopus\":": 2
    "cdc init | flags: | args: 1, | flags: --name=my-project,, --type=adopus\":": 2
    "cdc manage-column-templates | flags: | args: 1, | flags: --add=sync_timestamp\":": 2
    "cdc manage-column-templates | flags: | args: 4, | flags: --add=tenant_id\":": 2
    "cdc manage-column-templates | flags: | args: 3, | flags: --edit=tenant_id\":": 2
    "cdc manage-column-templates | flags: | flags: --list\":=3,": 2
    "cdc manage-column-templates | flags: | args: 3, | flags: --remove=tenant_id\":": 2
    "cdc manage-column-templates | flags: | args: 1, | flags: --remove=tenant_id\\\",\":": 2
    "cdc manage-column-templates | flags: | args: 3, | flags: --show=tenant_id\":": 2
    "cdc manage-column-templates | flags: | args: 1, | flags: --show=tenant_id\\\",\":": 2
    "cdc manage-pipelines stress-test\": 1,": 2
    "cdc manage-pipelines verify-sync\": 1,": 2
    "cdc manage-server-group | args: 2 | flags: | args: 2, | flags: --remove-extraction-pattern=prod\":": 2
    "cdc manage-server-group | args: INDEX | flags: | args: 1, | flags: --remove-extraction-pattern=SERVER\":": 2
    "cdc manage-server-group | args: PATTERN | flags: | args: 1, | flags: --add-extraction-pattern=SERVER\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-group=adopus\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-server=analytics,, --source-type=postgres\":": 2
    "cdc manage-server-group | flags: | flags: --add-to-ignore-list\\\",\":=1,": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-to-ignore-list=\\\"pattern\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-to-ignore-list=\\\"pattern_to_ignore\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-to-ignore-list=\\\"test_pattern\":": 2
    "cdc manage-server-group | flags: | flags: --add-to-schema-excludes\\\",\":=1,": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"schema_to_exclude\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"test_schema\":": 2
    "cdc manage-server-group | flags: | flags: --all,, --update\":=1,": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --create=asma,, --pattern=db-shared\":": 2
    "cdc manage-server-group | flags: | args: 6, | flags: --create=my-group\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --create=test-group\":": 2
    "cdc manage-server-group | flags: | flags: --info\":=2,": 2
    "cdc manage-server-group | flags: | flags: --info\\\",\":=1,": 2
    "cdc manage-server-group | flags: | flags: --list\":=3,": 2
    "cdc manage-server-group | flags: | flags: --list-extraction-patterns\":=1,": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --list-extraction-patterns=SERVER\":": 2
    "cdc manage-server-group | flags: | args: 3, | flags: --list-extraction-patterns=prod\":": 2
    "cdc manage-server-group | flags: | flags: --list-ignore-patterns\":=2,": 2
    "cdc manage-server-group | flags: | flags: --list-schema-excludes\":=1,": 2
    "cdc manage-server-group | flags: | flags: --refresh\":=1,": 2
    "cdc manage-server-group | flags: | flags: --server-group=adopus,, --update\":=1,": 2
    "cdc manage-server-group | flags: | flags: --server-group=asma,, --update\":=1,": 2
    "cdc manage-server-group | flags: | flags: --update\":=11,": 2
    "cdc manage-server-group | flags: | flags: --update\\\",\":=1,": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --update=default\":": 2
    "cdc manage-server-group | flags: | args: 1, | flags: --update=prod\":": 2
    "cdc manage-service directory | flags: | flags: --,, --inspect\":=1,": 2
    "cdc manage-service directory | flags: | args: 1, | flags: --,, --sink=sink_asma.proxy\":": 2
    "cdc manage-service | args: dbo.Fraver | flags: | args: 1, | flags: --add-source-tables=dbo.Actor,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --add-sink-table=pub.A,, --sink=asma\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --add-sink-table=pub.Actor\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --add-sink-table=pub.Actor,, --service=dir\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --add-source-table=Actor\":": 2
    "cdc manage-service | flags: | flags: --,, --add-source-table=Actor,, --inspect\":=1,": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --modify-custom-table=tbl\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --sink-table=pub.Actor,, --sink=asma\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --sink-table=t,, --sink=a\":": 2
    "cdc manage-service | flags: | args: 3, | flags: --,, --sink=sink_asma.proxy\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --,, --source-table=Actor\":": 2
    "cdc manage-service | flags: | flags: --add-column-template=tmpl,, --sink-\":=1,": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-column-template=tmpl,, --sink-,, --sink=asma\":": 2
    "cdc manage-service | flags: | flags: --add-sink-table=pub.Actor,, --map-\":=1,": 2
    "cdc manage-service | flags: | flags: --add-sink-table=pub.Actor,, --sink-\":=1,": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-sink=sink_asma.chat\\\",,, --service=directory\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-sink=sink_asma.chat,, --service=directory\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-source-table=dbo.Actor\\\",,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-source-table=dbo.Actor,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-source-table=dbo.Orders,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 3, | flags: --add-source-table=dbo.Users,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 2, | flags: --add-source-table=public.users,, --service=proxy\":": 2
    "cdc manage-service | flags: | args: 5, | flags: --add-table=Actor,, --primary-key=actno,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 2, | flags: --add-table=Actor,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-table=Fraver,, --primary-key=fraverid,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-table=MyTable,, --primary-key=id,, --service=my-service\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-table=Orders,, --primary-key=order_id,, --service=my-service\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-table=Users,, --primary-key=id,, --service=my-service\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --add-validation-database=AdOpusTest\\\",,, --create-service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --all\\\",,, --inspect,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --all,, --generate-validation,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 2, | flags: --all,, --inspect,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --all,, --inspect,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --all,, --inspect-mssql,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --all,, --inspect-sink=sink_asma.calendar,, --service=directory\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --create-service,, --server=analytics,, --service=analytics_data\":": 2
    "cdc manage-service | flags: | args: 2, | flags: --create-service,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --create-service=myservice\\\",\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --create=adopus,, --server-group=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --create=my-service\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --create=my-service,, --server-group=my-group\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --create=my-service,, --server-group=my-server-group\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --env=prod\\\",,, --inspect,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --inspect,, --save,, --schema=dbo,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --inspect,, --schema=dbo\\\",,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 5, | flags: --inspect,, --schema=dbo,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --inspect,, --schema=dbo,, --service=my-service\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --inspect,, --schema=dbo,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --inspect,, --service=myservice\":": 2
    "cdc manage-service | flags: | args: 2, | flags: --inspect-mssql,, --schema=dbo,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --inspect-sink=sink_asma.calendar,, --schema=public,, --service=directory\":": 2
    "cdc manage-service | flags: | flags: --list-services\\\",\":=1,": 2
    "cdc manage-service | flags: | args: 1, | flags: --list-sinks,, --service=directory\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --remove-service=myservice\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --remove-service=myservice\\\",\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --remove-sink=sink_asma.chat\\\",,, --service=directory\":": 2
    "cdc manage-service | flags: | args: 2, | flags: --remove-table=Test,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --remove-table=dbo.Actor\\\",,, --service=adopus\":": 2
    "cdc manage-service | flags: | args: 1, | flags: --remove-table=dbo.Actor,, --service=adopus\":": 2
    "cdc manage-service | flags: | flags: --runtime,, --service=directory,, --validate-bloblang\":=2,": 2
    "cdc manage-service | flags: | flags: --runtime,, --service=directory,, --validate-config\":=1,": 2
    "cdc manage-service | flags: | args: 2, | flags: --service=adopus\":": 2
    "cdc manage-service | flags: | flags: --service=adopus,, --validate-config\":=2,": 2
    "cdc manage-service | flags: | args: 18, | flags: --service=directory\":": 2
    "cdc manage-service | flags: | flags: --service=directory,, --validate-sinks\":=1,": 2
    "cdc manage-service | flags: | args: 1, | flags: --service=proxy\":": 2
    "cdc manage-service | flags: | flags: --source-table=Actor,, --track-\":=1,": 2
    "cdc manage-service-schema | flags: | flags: --list\":=1,": 2
    "cdc manage-service-schema | flags: | args: 2, | flags: --list,, --service=chat\":": 2
    "cdc manage-service-schema | flags: | args: 1, | flags: --list-custom-tables,, --service=calendar\":": 2
    "cdc manage-service-schema | flags: | flags: --list-services\":=2,": 2
    "cdc manage-service-schema | flags: | args: 1, | flags: --remove-custom-table=public.my_events,, --service=calendar\":": 2
    "cdc manage-service-schema | flags: | args: 1, | flags: --service=calendar\":": 2
    "cdc manage-service-schema | flags: | args: 1, | flags: --service=calendar,, --show=public.my_events\":": 2
    "cdc manage-service-schema | flags: | args: 7, | flags: --service=chat\":": 2
    "cdc manage-services config\": 3,": 2
    "cdc manage-services config directory\": 11,": 2
    "cdc manage-services config directory | flags: | flags: --,, --all\":=1,": 2
    "cdc manage-services config directory | flags: | flags: --,, --all,, --inspect-sink\":=1,": 2
    "cdc manage-services config directory | flags: | flags: --,, --inspect\":=1,": 2
    "cdc manage-services config directory | flags: | args: 1, | flags: --,, --sink=sink_asma.proxy\":": 2
    "cdc manage-services config | flags: | flags: --\":=3,": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --add-sink-table=pub.A,, --sink=asma\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --add-sink-table=pub.Actor,, --service=dir\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --add-source-table=Actor\":": 2
    "cdc manage-services config | flags: | flags: --,, --add-source-table=Actor,, --inspect\":=1,": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --modify-custom-table=tbl\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --sink-table=pub.Actor,, --sink=asma\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --sink-table=t,, --sink=a\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --sink=sink_asma.directory\":": 2
    "cdc manage-services config | flags: | args: 3, | flags: --,, --sink=sink_asma.proxy\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --,, --source-table=Actor\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --add-column-template=tmpl,, --sink-,, --sink=asma\":": 2
    "cdc manage-services config | flags: | flags: --add-sink-table,, --fr\":=1,": 2
    "cdc manage-services config | flags: | flags: --add-sink-table=pub.Actor,, --map-\":=1,": 2
    "cdc manage-services config | flags: | flags: --add-sink-table=pub.Actor,, --sink-\":=1,": 2
    "cdc manage-services config | flags: | args: 1, | flags: --add-sink-table=public.\":": 2
    "cdc manage-services config | flags: | args: 3, | flags: --add-source-table=dbo.\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --add-source-table=dbo.,, --add-source-table=dbo.Address\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --create-service=directory\":": 2
    "cdc manage-services config | flags: | args: 1, | flags: --inspect,, --service=myservice\":": 2
    "cdc manage-services config | flags: | flags: --source-table=Actor,, --track-\":=1,": 2
    "cdc manage-services schema custom-tables\": 6,": 2
    "cdc manage-services schema custom-tables | flags: | args: 1, | flags: --service=n\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --add-new-sink-group=analytics\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --add-new-sink-group=analytics,, --for-source-group=foo,, --type=postgres\":": 2
    "cdc manage-sink-groups | flags: | args: 3, | flags: --add-new-sink-group=analytics,, --type=postgres\":": 2
    "cdc manage-sink-groups | flags: | args: 2, | flags: --add-server=default,, --sink-group=sink_analytics\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --add-server=prod,, --sink-group=sink_analytics\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --add-to-ignore-list=temp_%\\\",\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --add-to-schema-excludes=hdb_catalog\\\",\":": 2
    "cdc manage-sink-groups | flags: | flags: --create\":=3,": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --create,, --source-group=asma\\\",\":": 2
    "cdc manage-sink-groups | flags: | args: 4, | flags: --create,, --source-group=foo\":": 2
    "cdc manage-sink-groups | flags: | args: 3, | flags: --info=sink_analytics\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --info=sink_asma\\\",\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --info=sink_foo\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --introspect-types,, --sink-group=sink_analytics\":": 2
    "cdc manage-sink-groups | flags: | flags: --list\":=3,": 2
    "cdc manage-sink-groups | flags: | args: 2, | flags: --remove-server=default,, --sink-group=sink_analytics\":": 2
    "cdc manage-sink-groups | flags: | args: 2, | flags: --remove=sink_analytics\":": 2
    "cdc manage-sink-groups | flags: | args: 1, | flags: --remove=sink_test\\\",\":": 2
    "cdc manage-sink-groups | flags: | flags: --sink-group=sink_analytics,, --update\":=2,": 2
    "cdc manage-sink-groups | flags: | args: 8, | flags: --sink-group=sink_asma\":": 2
    "cdc manage-sink-groups | flags: | flags: --sink-group=sink_asma,, --update\":=1,": 2
    "cdc manage-sink-groups | flags: | flags: --validate\":=3,": 2
    "cdc manage-source-groups | args: 2 | flags: | args: 3, | flags: --remove-extraction-pattern=prod\":": 2
    "cdc manage-source-groups | args: INDEX | flags: | args: 1, | flags: --remove-extraction-pattern=SERVER\":": 2
    "cdc manage-source-groups | args: PATTERN | flags: | args: 1, | flags: --add-extraction-pattern=SERVER\":": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --,, --add-server=srv1\":": 2
    "cdc manage-source-groups | flags: | flags: --,, --introspect-types\":=1,": 2
    "cdc manage-source-groups | flags: | args: 2, | flags: --add-extraction-pattern=default\":": 2
    "cdc manage-source-groups | flags: | args: 4, | flags: --add-extraction-pattern=prod\":": 2
    "cdc manage-source-groups | flags: | args: 2, | flags: --add-server=analytics,, --source-type=postgres\":": 2
    "cdc manage-source-groups | flags: | flags: --add-to-ignore-list\\\",\":=1,": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-ignore-list=\\\"pattern_to_ignore\":": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-ignore-list=\\\"test_pattern\":": 2
    "cdc manage-source-groups | flags: | flags: --add-to-schema-excludes\\\",\":=1,": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"schema_to_exclude\":": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"test_schema\":": 2
    "cdc manage-source-groups | flags: | flags: --all,, --update\":=1,": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --create=asma,, --pattern=db-shared\":": 2
    "cdc manage-source-groups | flags: | args: 4, | flags: --create=my-group\":": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --create=test-group\":": 2
    "cdc manage-source-groups | flags: | flags: --info\":=3,": 2
    "cdc manage-source-groups | flags: | flags: --info\\\",\":=1,": 2
    "cdc manage-source-groups | flags: | flags: --list\":=1,": 2
    "cdc manage-source-groups | flags: | flags: --list-extraction-patterns\":=1,": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --list-extraction-patterns=SERVER\":": 2
    "cdc manage-source-groups | flags: | args: 4, | flags: --list-extraction-patterns=prod\":": 2
    "cdc manage-source-groups | flags: | flags: --list-ignore-patterns\":=1,": 2
    "cdc manage-source-groups | flags: | flags: --list-schema-excludes\":=1,": 2
    "cdc manage-source-groups | flags: | args: 2, | flags: --set-extraction-pattern=default\":": 2
    "cdc manage-source-groups | flags: | flags: --update\":=10,": 2
    "cdc manage-source-groups | flags: | flags: --update\\\",\":=1,": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --update=default\":": 2
    "cdc manage-source-groups | flags: | args: 1, | flags: --update=prod\":": 2
    "cdc scaffold adopus\": 2,": 2
    "cdc scaffold asma\": 2,": 2
    "cdc scaffold my-group\": 3,": 2
    "cdc scaffold myproject\": 2,": 2
    "cdc scaffold myproject | flags: | args: 2, | flags: --pattern=db-shared,, --source-type=postgres\":": 2
    "cdc scaffold | flags: | args: 1, | flags: --implementation=test,, --pattern=db-shared\":": 2
    "cdc setup-local | flags: | flags: --enable-local-sink\":=2,": 2
    "cdc setup-local | flags: | flags: --enable-local-sink,, --enable-local-source\":=2,": 2
    "cdc setup-local | flags: | flags: --enable-local-source\":=2,": 2
    "cdc setup-local | flags: | flags: --full\":=2,": 2
    "cdc test tests/cli/test_scaffold.py\": 1,": 2
    "cdc test | flags: | flags: --all\":=2,": 2
    "cdc test | flags: | flags: --cli\":=2,": 2
    "cdc test | flags: | flags: --fast-pipelines\":=2,": 2
    "cdc test | flags: | flags: --full-pipelines\":=2,": 2
    "cdc test | flags: | args: 2, | flags: -k=scaffold\":": 2
    "cdc test | flags: | flags: -v\":=2,": 2
    "cdc manage-services config": 4
    "cdc manage-services config | flags: --all, --inspect, --service=adopus": 2
    "cdc manage-services config | flags: --add-table=Actor, --service=adopus": 1
    "cdc manage-services config | flags: --remove-table=Test, --service=adopus": 1
    "cdc manage-services config | flags: --list-services\",": 1
    "cdc manage-services config | flags: --all\",, --inspect, --service=adopus": 1
    "cdc manage-services config | flags: --create-service=myservice\",": 1
    "cdc manage-services config | flags: --add-validation-database=AdOpusTest\",, --create-service=adopus": 1
    "cdc manage-services config | flags: --remove-service=myservice\",": 1
    "cdc manage-services config | flags: --add-source-table=dbo.Actor\",, --service=adopus": 1
    "cdc manage-services config | flags: --remove-table=dbo.Actor\",, --service=adopus": 1
    "cdc manage-services config | flags: --service=proxy": 1
    "cdc manage-services config | flags: --service=directory": 17
    "cdc manage-services config | flags: --inspect, --schema=dbo\",, --service=adopus": 1
    "cdc manage-services config | flags: --add-sink=sink_asma.chat\",, --service=directory": 1
    "cdc manage-services config | flags: --remove-sink=sink_asma.chat\",, --service=directory": 1
    "cdc manage-services config | flags: --env=prod\",, --inspect, --service=adopus": 1
    "cdc manage-services config | flags: --service=directory, --sink-all\",, --sink-inspect=sink_asma.calendar": 1
    "cdc manage-services config | flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save\",": 1
    "cdc manage-services config | flags: --service=adopus": 2
    "cdc manage-services config | flags: --create-service, --service=myservice": 1
    "cdc manage-services config | flags: --remove-service=myservice": 1
    "cdc manage-services config | flags: --inspect, --schema=dbo, --service=adopus": 1
    "cdc manage-services config | flags: --add-source-table=dbo.Actor, --service=adopus": 1
    "cdc manage-services config | flags: --remove-table=dbo.Actor, --service=adopus": 1
    "cdc manage-services config | flags: --service=adopus, --validate-config": 1
    "cdc manage-services config | flags: --all, --inspect-sink=sink_asma.calendar, --service=directory": 1
    "cdc manage-services config | flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory": 1
    "cdc manage-services config | flags: --add-sink=sink_asma.chat, --service=directory": 1
    "cdc manage-services config | flags: --list-sinks, --service=directory": 1
    "cdc manage-services config | flags: --service=directory, --validate-sinks": 1
    "cdc manage-services schema custom-tables | flags: --list, --service=chat": 2
    "cdc manage-services schema custom-tables | flags: --service=chat": 7
    "cdc manage-services schema custom-tables | flags: --list-services": 2
    "cdc manage-services schema custom-tables": 9
    "cdc manage-service | flags: --list-services\",": 1
    "cdc manage-services config | flags: --": 3
    "cdc manage-services config | flags: --, --sink=sink_asma.directory": 1
    "cdc manage-services config | flags: --add-sink-table, --fr": 1
    "cdc manage-services config | flags: --add-source-table=dbo.": 3
    "cdc manage-services config | flags: --add-source-table=dbo., --add-source-table=dbo.Address": 1
    "cdc manage-services config | flags: --add-sink-table=public.": 1
    "cdc manage-source-groups": 7
    "cdc manage-source-groups | flags: --update": 10
    "cdc manage-sink-groups": 2
    "cdc manage-sink-groups | flags: --sink-group=sink_analytics, --update": 2
    "cdc manage-service | flags: --add-validation-database=AdOpusTest\",, --create-service=adopus": 1
    "cdc manage-sink-groups | flags: --sink-group=sink_asma": 8
    "cdc manage-services config | flags: --create-service=directory": 1
    "cdc manage-sink-groups | flags: --add-to-ignore-list=temp_%\",": 1
    "cdc manage-sink-groups | flags: --add-to-schema-excludes=hdb_catalog\",": 1
    "cdc manage-sink-groups | flags: --sink-group=sink_asma, --update": 1
    "cdc manage-services config directory": 11
    "cdc manage-services config directory | flags: --, --all": 1
    "cdc manage-services config | flags: --inspect, --service=myservice": 1
    "cdc manage-services config | flags: --add-sink-table=pub.Actor, --map-": 1
    "cdc manage-services config | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1
    "cdc manage-services config | flags: --add-sink-table=pub.Actor, --sink-": 1
    "cdc manage-services schema custom-tables | flags: --service=n": 1
    "cdc manage-services config | flags: --source-table=Actor, --track-": 1
    "cdc manage-services config | flags: --, --sink=sink_asma.proxy": 3
    "cdc manage-services config directory | flags: --, --sink=sink_asma.proxy": 1
    "cdc manage-services config | flags: --, --sink-table=pub.Actor, --sink=asma": 1
    "cdc manage-services config | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1
    "cdc manage-services config | flags: --, --sink-table=t, --sink=a": 1
    "cdc manage-services config directory | flags: --, --all, --inspect-sink": 1
    "cdc manage-services config | flags: --, --add-source-table=Actor": 1
    "cdc manage-services config | flags: --, --add-sink-table=pub.Actor, --service=dir": 1
    "cdc manage-services config | flags: --, --source-table=Actor": 1
    "cdc manage-services config | flags: --, --modify-custom-table=tbl": 1
    "cdc manage-services config | flags: --, --add-source-table=Actor, --inspect": 1
    "cdc manage-services config directory | flags: --, --inspect": 1
    "cdc manage-services config | flags: --, --add-sink-table=pub.A, --sink=asma": 1
    "cdc test | flags: --fast-pipelines": 2
    "cdc test | flags: --full-pipelines": 2
    "cdc manage-pipelines verify-sync": 1
    "cdc manage-pipelines stress-test": 1
    "cdc scaffold myproject | flags: --pattern=db-shared, --source-type=postgres": 2
    "cdc manage-service | flags: --remove-service=myservice\",": 1
    "cdc manage-service | flags: --remove-service=myservice": 1
    "cdc manage-service | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1
    "cdc manage-service | flags: --, --sink=sink_asma.proxy": 3
    "cdc manage-service directory | flags: --, --sink=sink_asma.proxy": 1
    "cdc manage-service | flags: --, --sink-table=pub.Actor, --sink=asma": 1
    "cdc manage-service | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1
    "cdc manage-service | flags: --, --sink-table=t, --sink=a": 1
    "cdc manage-service | flags: --, --add-sink-table=pub.Actor, --service=dir": 1
    "cdc manage-service | flags: --, --add-sink-table=pub.A, --sink=asma": 1
    "cdc manage-service | flags: --add-sink-table=pub.Actor, --map-": 1
    "cdc manage-service | flags: --add-column-template=tmpl, --sink-": 1
    "cdc manage-service | flags: --add-sink-table=pub.Actor, --sink-": 1
    "cdc manage-service | flags: --source-table=Actor, --track-": 1
    "cdc manage-service | flags: --, --add-source-table=Actor": 1
    "cdc manage-service | flags: --, --add-sink-table=pub.Actor": 1
    "cdc manage-service | flags: --, --source-table=Actor": 1
    "cdc manage-service | flags: --, --modify-custom-table=tbl": 1
    "cdc manage-service | flags: --, --add-source-table=Actor, --inspect": 1
    "cdc manage-service directory | flags: --, --inspect": 1
    "cdc manage-source-groups | flags: --, --introspect-types": 1
    "cdc manage-source-groups | flags: --, --add-server=srv1": 1
    "cdc manage-source-groups | flags: --info": 3
    "cdc scaffold | flags: --implementation=test, --pattern=db-shared": 1
    "cdc manage-service | flags: --inspect, --service=myservice": 1
    "cdc manage-column-templates | flags: --list": 3
    "cdc manage-column-templates | flags: --show=tenant_id": 3
    "cdc manage-column-templates | flags: --add=tenant_id": 4
    "cdc manage-column-templates | flags: --edit=tenant_id": 3
    "cdc manage-column-templates | flags: --remove=tenant_id": 3
    "cdc manage-column-templates | flags: --remove=tenant_id\",": 1
    "cdc manage-column-templates | flags: --show=tenant_id\",": 1
    "cdc manage-column-templates | flags: --add=sync_timestamp": 1
    "cdc manage-service | flags: --service=proxy": 1
    "cdc manage-service | flags: --runtime, --service=directory, --validate-bloblang": 2
    "cdc manage-service | flags: --runtime, --service=directory, --validate-config": 1
    "cdc manage-service-schema | flags: --service=calendar": 1
    "cdc manage-service-schema | flags: --list-custom-tables, --service=calendar": 1
    "cdc manage-service-schema | flags: --service=calendar, --show=public.my_events": 1
    "cdc manage-service-schema | flags: --remove-custom-table=public.my_events, --service=calendar": 1
    "cdc manage-service-schema | flags: --list": 1
    "cdc manage-service | flags: --service=directory": 18
    "cdc manage-service-schema | flags: --list, --service=chat": 2
    "cdc manage-service-schema | flags: --service=chat": 7
    "cdc manage-service-schema | flags: --list-services": 2
    "cdc manage-service-schema": 3
    "cdc test": 2
    "cdc test | flags: --cli": 2
    "cdc test | flags: --all": 2
    "cdc test | flags: -v": 2
    "cdc test | flags: -k=scaffold": 2
    "cdc test tests/cli/test_scaffold.py": 1
    "cdc manage-service | flags: --all\",, --inspect, --service=adopus": 1
    "cdc manage-service | flags: --create-service=myservice\",": 1
    "cdc manage-service | flags: --add-source-table=dbo.Actor\",, --service=adopus": 1
    "cdc manage-service | flags: --remove-table=dbo.Actor\",, --service=adopus": 1
    "cdc manage-service | flags: --inspect, --schema=dbo\",, --service=adopus": 1
    "cdc manage-service | flags: --add-sink=sink_asma.chat\",, --service=directory": 1
    "cdc manage-service | flags: --remove-sink=sink_asma.chat\",, --service=directory": 1
    "cdc manage-service | flags: --env=prod\",, --inspect, --service=adopus": 1
    "cdc manage-service | flags: --service=adopus": 2
    "cdc manage-sink-groups | flags: --introspect-types, --sink-group=sink_analytics": 1
    "cdc manage-source-groups | flags: --set-extraction-pattern=default": 2
    "cdc manage-source-groups | flags: --add-extraction-pattern=prod": 4
    "cdc manage-source-groups | flags: --add-extraction-pattern=default": 2
    "cdc manage-source-groups | flags: --add-server=analytics, --source-type=postgres": 2
    "cdc manage-source-groups | flags: --list-extraction-patterns=prod": 4
    "cdc manage-source-groups | args: 2 | flags: --remove-extraction-pattern=prod": 3
    "cdc manage-service": 3
    "cdc manage-service | flags: --all, --inspect, --service=adopus": 2
    "cdc manage-service | flags: --add-table=Actor, --service=adopus": 2
    "cdc manage-service | flags: --remove-table=Test, --service=adopus": 2
    "cdc manage-service | flags: --all, --inspect-sink=sink_asma.calendar, --service=directory": 1
    "cdc manage-service | flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory": 1
    "cdc manage-service | flags: --add-sink=sink_asma.chat, --service=directory": 1
    "cdc manage-service | flags: --list-sinks, --service=directory": 1
    "cdc manage-service | flags: --service=directory, --validate-sinks": 1
    "cdc manage-sink-groups | flags: --add-new-sink-group=analytics": 1
    "cdc manage-sink-groups | flags: --create, --source-group=asma\",": 1
    "cdc manage-sink-groups | flags: --info=sink_asma\",": 1
    "cdc manage-sink-groups | flags: --remove=sink_test\",": 1
    "cdc manage-source-groups | flags: --create=test-group": 1
    "cdc manage-source-groups | flags: --add-to-ignore-list=\"test_pattern": 1
    "cdc manage-source-groups | flags: --add-to-schema-excludes=\"test_schema": 1
    "cdc manage-source-groups | flags: --list": 1
    "cdc manage-source-groups | flags: --create=my-group": 4
    "cdc manage-source-groups | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1
    "cdc manage-source-groups | flags: --list-extraction-patterns": 1
    "cdc manage-source-groups | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1
    "cdc manage-source-groups | flags: --list-extraction-patterns=SERVER": 1
    "cdc manage-source-groups | flags: --create=asma, --pattern=db-shared": 1
    "cdc manage-sink-groups | flags: --create, --source-group=foo": 4
    "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --for-source-group=foo, --type=postgres": 1
    "cdc manage-sink-groups | flags: --list": 3
    "cdc manage-sink-groups | flags: --info=sink_foo": 1
    "cdc manage-sink-groups | flags: --info=sink_analytics": 3
    "cdc manage-sink-groups | flags: --validate": 3
    "cdc manage-source-groups | flags: --update=default": 1
    "cdc manage-source-groups | flags: --update=prod": 1
    "cdc manage-source-groups | flags: --all, --update": 1
    "cdc manage-source-groups | flags: --list-ignore-patterns": 1
    "cdc manage-source-groups | flags: --add-to-ignore-list=\"pattern_to_ignore": 1
    "cdc manage-source-groups | flags: --list-schema-excludes": 1
    "cdc manage-source-groups | flags: --add-to-schema-excludes=\"schema_to_exclude": 1
    "cdc manage-sink-groups | flags: --create": 3
    "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --type=postgres": 3
    "cdc manage-sink-groups | flags: --add-server=default, --sink-group=sink_analytics": 2
    "cdc manage-sink-groups | flags: --remove-server=default, --sink-group=sink_analytics": 2
    "cdc manage-sink-groups | flags: --remove=sink_analytics": 2
    "cdc manage-sink-groups | flags: --add-server=prod, --sink-group=sink_analytics": 1
    "cdc manage-source-groups | flags: --update\",": 1
    "cdc manage-source-groups | flags: --info\",": 1
    "cdc manage-source-groups | flags: --add-to-ignore-list\",": 1
    "cdc manage-source-groups | flags: --add-to-schema-excludes\",": 1
    "cdc manage-service | flags: --add-table=Actor, --primary-key=actno, --service=adopus": 5
    "cdc generate": 15
    "cdc manage-service | flags: --inspect, --schema=dbo, --service=adopus": 5
    "cdc reload-cdc-autocompletions": 2
    "cdc manage-server-group | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1
    "cdc manage-server-group | flags: --list-extraction-patterns": 1
    "cdc manage-server-group | flags: --list-extraction-patterns=prod": 3
    "cdc manage-server-group | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1
    "cdc manage-server-group | args: 2 | flags: --remove-extraction-pattern=prod": 2
    "cdc manage-server-group | flags: --list-extraction-patterns=SERVER": 1
    "cdc manage-server-group | flags: --update": 11
    "cdc manage-server-group | flags: --update=default": 1
    "cdc manage-server-group | flags: --update=prod": 1
    "cdc manage-server-group | flags: --all, --update": 1
    "cdc manage-service | flags: --create-service, --server=analytics, --service=analytics_data": 1
    "cdc manage-server-group | flags: --add-server=analytics, --source-type=postgres": 1
    "cdc manage-server-group | flags: --update\",": 1
    "cdc manage-server-group | flags: --info\",": 1
    "cdc manage-server-group | flags: --add-to-ignore-list\",": 1
    "cdc manage-server-group | flags: --add-to-schema-excludes\",": 1
    "cdc scaffold my-group": 3
    "cdc manage-service | flags: --create=my-service": 1
    "cdc init": 3
    "cdc scaffold adopus": 2
    "cdc scaffold asma": 2
    "cdc scaffold myproject": 2
    "cdc setup-local | flags: --enable-local-sink": 2
    "cdc setup-local | flags: --enable-local-source": 2
    "cdc setup-local | flags: --enable-local-sink, --enable-local-source": 2
    "cdc setup-local | flags: --full": 2
    "cdc manage-server-group | flags: --create=test-group": 1
    "cdc manage-server-group | flags: --add-to-ignore-list=\"test_pattern": 1
    "cdc manage-server-group | flags: --add-to-schema-excludes=\"test_schema": 1
    "cdc manage-server-group | flags: --create=my-group": 6
    "cdc manage-service | flags: --create=my-service, --server-group=my-group": 1
    "cdc manage-service | flags: --add-table=Users, --primary-key=id, --service=my-service": 1
    "cdc manage-service | flags: --add-table=Orders, --primary-key=order_id, --service=my-service": 1
    "cdc manage-service | flags: --inspect, --schema=dbo, --service=my-service": 1
    "cdc generate | flags: --environment=dev, --service=my-service": 2
    "cdc manage-server-group | flags: --info": 2
    "cdc manage-server-group | flags: --list": 3
    "cdc validate": 3
    "cdc manage-service | flags: --create=my-service, --server-group=my-server-group": 1
    "cdc manage-service | flags: --add-table=MyTable, --primary-key=id, --service=my-service": 1
    "cdc manage-service | flags: --create=adopus, --server-group=adopus": 1
    "cdc manage-service | flags: --add-table=Fraver, --primary-key=fraverid, --service=adopus": 1
    "cdc manage-server-group | flags: --add-group=adopus": 1
    "cdc generate | flags: --environment=local, --service=adopus": 1
    "cdc manage-server-group | flags: --refresh": 1
    "cdc manage-server-group": 5
    "cdc manage-server-group | flags: --create=asma, --pattern=db-shared": 1
    "cdc manage-server-group | flags: --list-ignore-patterns": 2
    "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern_to_ignore": 1
    "cdc manage-server-group | flags: --list-schema-excludes": 1
    "cdc manage-server-group | flags: --add-to-schema-excludes=\"schema_to_exclude": 1
    "cdc init | flags: --name=my-project, --type=adopus": 1
    "cdc init | flags: --name=adopus-cdc, --type=adopus": 2
    "cdc init | flags: --name=asma-cdc, --type=asma": 2
    "cdc init | flags: --name=my-project, --target-dir=/path/to/project, --type=adopus": 1
    "cdc manage-service | flags: --create-service, --service=myservice": 2
    "cdc manage-service | flags: --add-source-table=dbo.Actor, --service=adopus": 1
    "cdc manage-service | args: dbo.Fraver | flags: --add-source-tables=dbo.Actor, --service=adopus": 1
    "cdc manage-service | flags: --remove-table=dbo.Actor, --service=adopus": 1
    "cdc manage-service | flags: --service=adopus, --validate-config": 2
    "cdc init | flags: --git-init, --name=my-project, --type=adopus": 1
    "cdc init | flags: --name=PROJECT_NAME": 1
    "cdc manage-service | flags: --add-source-table=public.users, --service=proxy": 2
    "cdc manage-service | flags: --add-source-table=dbo.Users, --service=myservice": 3
    "cdc manage-service | flags: --add-source-table=dbo.Orders, --service=myservice": 1
    "cdc manage-service | flags: --all, --inspect, --service=myservice": 1
    "cdc manage-service | flags: --inspect, --schema=dbo, --service=myservice": 1
    "cdc manage-service | flags: --inspect, --save, --schema=dbo, --service=myservice": 1
    "cdc manage-service | flags: --inspect-mssql, --schema=dbo, --service=adopus": 2
    "cdc setup-local": 1
    "cdc manage-server-group | flags: --server-group=adopus, --update": 1
    "cdc manage-server-group | flags: --server-group=asma, --update": 1
    "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern": 1
    "cdc manage-service | flags: --all, --inspect-mssql, --service=adopus": 1
    "cdc manage-service | flags: --all, --generate-validation, --service=adopus": 1
total:
  "cdc manage-service | flags: --service=directory": 18
  "cdc manage-services config | flags: --service=directory": 17
  "cdc generate": 15
  "cdc manage-server-group | flags: --update": 11
  "cdc manage-services config directory": 11
  "cdc manage-source-groups | flags: --update": 10
  "cdc manage-services schema custom-tables": 9
  "cdc manage-sink-groups | flags: --sink-group=sink_asma": 8
  "cdc manage-service-schema | flags: --service=chat": 7
  "cdc manage-services schema custom-tables | flags: --service=chat": 7
  "cdc manage-source-groups": 7
  "cdc manage-server-group | flags: --create=my-group": 6
  "cdc manage-server-group": 5
  "cdc manage-service | flags: --add-table=Actor, --primary-key=actno, --service=adopus": 5
  "cdc manage-service | flags: --inspect, --schema=dbo, --service=adopus": 5
  "cdc manage-column-templates | flags: --add=tenant_id": 4
  "cdc manage-services config": 4
  "cdc manage-sink-groups | flags: --create, --source-group=foo": 4
  "cdc manage-source-groups | flags: --add-extraction-pattern=prod": 4
  "cdc manage-source-groups | flags: --create=my-group": 4
  "cdc manage-source-groups | flags: --list-extraction-patterns=prod": 4
  "cdc init": 3
  "cdc manage-column-templates | flags: --edit=tenant_id": 3
  "cdc manage-column-templates | flags: --list": 3
  "cdc manage-column-templates | flags: --remove=tenant_id": 3
  "cdc manage-column-templates | flags: --show=tenant_id": 3
  "cdc manage-server-group | flags: --list": 3
  "cdc manage-server-group | flags: --list-extraction-patterns=prod": 3
  "cdc manage-service": 3
  "cdc manage-service | flags: --, --sink=sink_asma.proxy": 3
  "cdc manage-service | flags: --add-source-table=dbo.Users, --service=myservice": 3
  "cdc manage-service-schema": 3
  "cdc manage-services config | flags: --": 3
  "cdc manage-services config | flags: --, --sink=sink_asma.proxy": 3
  "cdc manage-services config | flags: --add-source-table=dbo.": 3
  "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --type=postgres": 3
  "cdc manage-sink-groups | flags: --create": 3
  "cdc manage-sink-groups | flags: --info=sink_analytics": 3
  "cdc manage-sink-groups | flags: --list": 3
  "cdc manage-sink-groups | flags: --validate": 3
  "cdc manage-source-groups | args: 2 | flags: --remove-extraction-pattern=prod": 3
  "cdc manage-source-groups | flags: --info": 3
  "cdc scaffold my-group": 3
  "cdc validate": 3
  "cdc generate | flags: --environment=dev, --service=my-service": 2
  "cdc generate | flags: | args: 1, | flags: --environment=local,, --service=adopus\":": 2
  "cdc generate | flags: | args: 2, | flags: --environment=dev,, --service=my-service\":": 2
  "cdc init | flags: --name=adopus-cdc, --type=adopus": 2
  "cdc init | flags: --name=asma-cdc, --type=asma": 2
  "cdc init | flags: | args: 1, | flags: --git-init,, --name=my-project,, --type=adopus\":": 2
  "cdc init | flags: | args: 1, | flags: --name=PROJECT_NAME\":": 2
  "cdc init | flags: | args: 1, | flags: --name=my-project,, --target-dir=/path/to/project,, --type=adopus\":": 2
  "cdc init | flags: | args: 1, | flags: --name=my-project,, --type=adopus\":": 2
  "cdc init | flags: | args: 2, | flags: --name=adopus-cdc,, --type=adopus\":": 2
  "cdc init | flags: | args: 2, | flags: --name=asma-cdc,, --type=asma\":": 2
  "cdc manage-column-templates | flags: | args: 1, | flags: --add=sync_timestamp\":": 2
  "cdc manage-column-templates | flags: | args: 1, | flags: --remove=tenant_id\\\",\":": 2
  "cdc manage-column-templates | flags: | args: 1, | flags: --show=tenant_id\\\",\":": 2
  "cdc manage-column-templates | flags: | args: 3, | flags: --edit=tenant_id\":": 2
  "cdc manage-column-templates | flags: | args: 3, | flags: --remove=tenant_id\":": 2
  "cdc manage-column-templates | flags: | args: 3, | flags: --show=tenant_id\":": 2
  "cdc manage-column-templates | flags: | args: 4, | flags: --add=tenant_id\":": 2
  "cdc manage-column-templates | flags: | flags: --list\":=3,": 2
  "cdc manage-pipelines stress-test\": 1,": 2
  "cdc manage-pipelines verify-sync\": 1,": 2
  "cdc manage-server-group | args: 2 | flags: --remove-extraction-pattern=prod": 2
  "cdc manage-server-group | args: 2 | flags: | args: 2, | flags: --remove-extraction-pattern=prod\":": 2
  "cdc manage-server-group | args: INDEX | flags: | args: 1, | flags: --remove-extraction-pattern=SERVER\":": 2
  "cdc manage-server-group | args: PATTERN | flags: | args: 1, | flags: --add-extraction-pattern=SERVER\":": 2
  "cdc manage-server-group | flags: --info": 2
  "cdc manage-server-group | flags: --list-ignore-patterns": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-group=adopus\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-server=analytics,, --source-type=postgres\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-to-ignore-list=\\\"pattern\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-to-ignore-list=\\\"pattern_to_ignore\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-to-ignore-list=\\\"test_pattern\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"schema_to_exclude\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"test_schema\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --create=asma,, --pattern=db-shared\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --create=test-group\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --list-extraction-patterns=SERVER\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --update=default\":": 2
  "cdc manage-server-group | flags: | args: 1, | flags: --update=prod\":": 2
  "cdc manage-server-group | flags: | args: 3, | flags: --list-extraction-patterns=prod\":": 2
  "cdc manage-server-group | flags: | args: 6, | flags: --create=my-group\":": 2
  "cdc manage-server-group | flags: | flags: --add-to-ignore-list\\\",\":=1,": 2
  "cdc manage-server-group | flags: | flags: --add-to-schema-excludes\\\",\":=1,": 2
  "cdc manage-server-group | flags: | flags: --all,, --update\":=1,": 2
  "cdc manage-server-group | flags: | flags: --info\":=2,": 2
  "cdc manage-server-group | flags: | flags: --info\\\",\":=1,": 2
  "cdc manage-server-group | flags: | flags: --list\":=3,": 2
  "cdc manage-server-group | flags: | flags: --list-extraction-patterns\":=1,": 2
  "cdc manage-server-group | flags: | flags: --list-ignore-patterns\":=2,": 2
  "cdc manage-server-group | flags: | flags: --list-schema-excludes\":=1,": 2
  "cdc manage-server-group | flags: | flags: --refresh\":=1,": 2
  "cdc manage-server-group | flags: | flags: --server-group=adopus,, --update\":=1,": 2
  "cdc manage-server-group | flags: | flags: --server-group=asma,, --update\":=1,": 2
  "cdc manage-server-group | flags: | flags: --update\":=11,": 2
  "cdc manage-server-group | flags: | flags: --update\\\",\":=1,": 2
  "cdc manage-service directory | flags: | args: 1, | flags: --,, --sink=sink_asma.proxy\":": 2
  "cdc manage-service directory | flags: | flags: --,, --inspect\":=1,": 2
  "cdc manage-service | args: dbo.Fraver | flags: | args: 1, | flags: --add-source-tables=dbo.Actor,, --service=adopus\":": 2
  "cdc manage-service | flags: --add-source-table=public.users, --service=proxy": 2
  "cdc manage-service | flags: --add-table=Actor, --service=adopus": 2
  "cdc manage-service | flags: --all, --inspect, --service=adopus": 2
  "cdc manage-service | flags: --create-service, --service=myservice": 2
  "cdc manage-service | flags: --inspect-mssql, --schema=dbo, --service=adopus": 2
  "cdc manage-service | flags: --remove-table=Test, --service=adopus": 2
  "cdc manage-service | flags: --runtime, --service=directory, --validate-bloblang": 2
  "cdc manage-service | flags: --service=adopus": 2
  "cdc manage-service | flags: --service=adopus, --validate-config": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --add-sink-table=pub.A,, --sink=asma\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --add-sink-table=pub.Actor\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --add-sink-table=pub.Actor,, --service=dir\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --add-source-table=Actor\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --modify-custom-table=tbl\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --sink-table=pub.Actor,, --sink=asma\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --sink-table=t,, --sink=a\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --,, --source-table=Actor\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-column-template=tmpl,, --sink-,, --sink=asma\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-sink=sink_asma.chat,, --service=directory\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-sink=sink_asma.chat\\\",,, --service=directory\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-source-table=dbo.Actor,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-source-table=dbo.Actor\\\",,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-source-table=dbo.Orders,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-table=Fraver,, --primary-key=fraverid,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-table=MyTable,, --primary-key=id,, --service=my-service\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-table=Orders,, --primary-key=order_id,, --service=my-service\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-table=Users,, --primary-key=id,, --service=my-service\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --add-validation-database=AdOpusTest\\\",,, --create-service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --all,, --generate-validation,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --all,, --inspect,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --all,, --inspect-mssql,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --all,, --inspect-sink=sink_asma.calendar,, --service=directory\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --all\\\",,, --inspect,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --create-service,, --server=analytics,, --service=analytics_data\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --create-service=myservice\\\",\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --create=adopus,, --server-group=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --create=my-service\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --create=my-service,, --server-group=my-group\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --create=my-service,, --server-group=my-server-group\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --env=prod\\\",,, --inspect,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --inspect,, --save,, --schema=dbo,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --inspect,, --schema=dbo,, --service=my-service\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --inspect,, --schema=dbo,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --inspect,, --schema=dbo\\\",,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --inspect,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --inspect-sink=sink_asma.calendar,, --schema=public,, --service=directory\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --list-sinks,, --service=directory\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --remove-service=myservice\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --remove-service=myservice\\\",\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --remove-sink=sink_asma.chat\\\",,, --service=directory\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --remove-table=dbo.Actor,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --remove-table=dbo.Actor\\\",,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 1, | flags: --service=proxy\":": 2
  "cdc manage-service | flags: | args: 18, | flags: --service=directory\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --add-source-table=public.users,, --service=proxy\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --add-table=Actor,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --all,, --inspect,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --create-service,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --inspect-mssql,, --schema=dbo,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --remove-table=Test,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 2, | flags: --service=adopus\":": 2
  "cdc manage-service | flags: | args: 3, | flags: --,, --sink=sink_asma.proxy\":": 2
  "cdc manage-service | flags: | args: 3, | flags: --add-source-table=dbo.Users,, --service=myservice\":": 2
  "cdc manage-service | flags: | args: 5, | flags: --add-table=Actor,, --primary-key=actno,, --service=adopus\":": 2
  "cdc manage-service | flags: | args: 5, | flags: --inspect,, --schema=dbo,, --service=adopus\":": 2
  "cdc manage-service | flags: | flags: --,, --add-source-table=Actor,, --inspect\":=1,": 2
  "cdc manage-service | flags: | flags: --add-column-template=tmpl,, --sink-\":=1,": 2
  "cdc manage-service | flags: | flags: --add-sink-table=pub.Actor,, --map-\":=1,": 2
  "cdc manage-service | flags: | flags: --add-sink-table=pub.Actor,, --sink-\":=1,": 2
  "cdc manage-service | flags: | flags: --list-services\\\",\":=1,": 2
  "cdc manage-service | flags: | flags: --runtime,, --service=directory,, --validate-bloblang\":=2,": 2
  "cdc manage-service | flags: | flags: --runtime,, --service=directory,, --validate-config\":=1,": 2
  "cdc manage-service | flags: | flags: --service=adopus,, --validate-config\":=2,": 2
  "cdc manage-service | flags: | flags: --service=directory,, --validate-sinks\":=1,": 2
  "cdc manage-service | flags: | flags: --source-table=Actor,, --track-\":=1,": 2
  "cdc manage-service-schema | flags: --list, --service=chat": 2
  "cdc manage-service-schema | flags: --list-services": 2
  "cdc manage-service-schema | flags: | args: 1, | flags: --list-custom-tables,, --service=calendar\":": 2
  "cdc manage-service-schema | flags: | args: 1, | flags: --remove-custom-table=public.my_events,, --service=calendar\":": 2
  "cdc manage-service-schema | flags: | args: 1, | flags: --service=calendar\":": 2
  "cdc manage-service-schema | flags: | args: 1, | flags: --service=calendar,, --show=public.my_events\":": 2
  "cdc manage-service-schema | flags: | args: 2, | flags: --list,, --service=chat\":": 2
  "cdc manage-service-schema | flags: | args: 7, | flags: --service=chat\":": 2
  "cdc manage-service-schema | flags: | flags: --list\":=1,": 2
  "cdc manage-service-schema | flags: | flags: --list-services\":=2,": 2
  "cdc manage-services config directory | flags: | args: 1, | flags: --,, --sink=sink_asma.proxy\":": 2
  "cdc manage-services config directory | flags: | flags: --,, --all\":=1,": 2
  "cdc manage-services config directory | flags: | flags: --,, --all,, --inspect-sink\":=1,": 2
  "cdc manage-services config directory | flags: | flags: --,, --inspect\":=1,": 2
  "cdc manage-services config directory | flags: | flags: --al\":=1": 2
  "cdc manage-services config directory\": 1,": 2
  "cdc manage-services config directory\": 11,": 2
  "cdc manage-services config | flags: --all, --inspect, --service=adopus": 2
  "cdc manage-services config | flags: --service=adopus": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --add-sink-table=pub.A,, --sink=asma\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --add-sink-table=pub.Actor,, --service=dir\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --add-source-table=Actor\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --modify-custom-table=tbl\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --sink-table=pub.Actor,, --sink=asma\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --sink-table=t,, --sink=a\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --sink=sink_asma.directory\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --,, --source-table=Actor\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --add-column-template=tmpl,, --sink-,, --sink=asma\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --add-sink-table=public.\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --add-source-table=dbo.,, --add-source-table=dbo.Address\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --create-service=directory\":": 2
  "cdc manage-services config | flags: | args: 1, | flags: --inspect,, --service=myservice\":": 2
  "cdc manage-services config | flags: | args: 3, | flags: --,, --sink=sink_asma.proxy\":": 2
  "cdc manage-services config | flags: | args: 3, | flags: --add-source-table=dbo.\":": 2
  "cdc manage-services config | flags: | flags: --\":=3,": 2
  "cdc manage-services config | flags: | flags: --,, --add-source-table=Actor,, --inspect\":=1,": 2
  "cdc manage-services config | flags: | flags: --add-sink-table,, --fr\":=1,": 2
  "cdc manage-services config | flags: | flags: --add-sink-table=pub.Actor,, --map-\":=1,": 2
  "cdc manage-services config | flags: | flags: --add-sink-table=pub.Actor,, --sink-\":=1,": 2
  "cdc manage-services config | flags: | flags: --source-table=Actor,, --track-\":=1,": 2
  "cdc manage-services config\": 3,": 2
  "cdc manage-services schema custom-tables | flags: --list, --service=chat": 2
  "cdc manage-services schema custom-tables | flags: --list-services": 2
  "cdc manage-services schema custom-tables | flags: | args: 1, | flags: --service=n\":": 2
  "cdc manage-services schema custom-tables\": 6,": 2
  "cdc manage-sink-groups": 2
  "cdc manage-sink-groups | flags: --add-server=default, --sink-group=sink_analytics": 2
  "cdc manage-sink-groups | flags: --remove-server=default, --sink-group=sink_analytics": 2
  "cdc manage-sink-groups | flags: --remove=sink_analytics": 2
  "cdc manage-sink-groups | flags: --sink-group=sink_analytics, --update": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --add-new-sink-group=analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --add-new-sink-group=analytics,, --for-source-group=foo,, --type=postgres\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --add-server=prod,, --sink-group=sink_analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --add-to-ignore-list=temp_%\\\",\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --add-to-schema-excludes=hdb_catalog\\\",\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --create,, --source-group=asma\\\",\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --info=sink_asma\\\",\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --info=sink_foo\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --introspect-types,, --sink-group=sink_analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 1, | flags: --remove=sink_test\\\",\":": 2
  "cdc manage-sink-groups | flags: | args: 2, | flags: --add-server=default,, --sink-group=sink_analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 2, | flags: --remove-server=default,, --sink-group=sink_analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 2, | flags: --remove=sink_analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 3, | flags: --add-new-sink-group=analytics,, --type=postgres\":": 2
  "cdc manage-sink-groups | flags: | args: 3, | flags: --info=sink_analytics\":": 2
  "cdc manage-sink-groups | flags: | args: 4, | flags: --create,, --source-group=foo\":": 2
  "cdc manage-sink-groups | flags: | args: 8, | flags: --sink-group=sink_asma\":": 2
  "cdc manage-sink-groups | flags: | flags: --create\":=3,": 2
  "cdc manage-sink-groups | flags: | flags: --list\":=3,": 2
  "cdc manage-sink-groups | flags: | flags: --sink-group=sink_analytics,, --update\":=2,": 2
  "cdc manage-sink-groups | flags: | flags: --sink-group=sink_asma,, --update\":=1,": 2
  "cdc manage-sink-groups | flags: | flags: --validate\":=3,": 2
  "cdc manage-source-groups | args: 2 | flags: | args: 3, | flags: --remove-extraction-pattern=prod\":": 2
  "cdc manage-source-groups | args: INDEX | flags: | args: 1, | flags: --remove-extraction-pattern=SERVER\":": 2
  "cdc manage-source-groups | args: PATTERN | flags: | args: 1, | flags: --add-extraction-pattern=SERVER\":": 2
  "cdc manage-source-groups | flags: --add-extraction-pattern=default": 2
  "cdc manage-source-groups | flags: --add-server=analytics, --source-type=postgres": 2
  "cdc manage-source-groups | flags: --set-extraction-pattern=default": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --,, --add-server=srv1\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-ignore-list=\\\"pattern_to_ignore\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-ignore-list=\\\"test_pattern\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"schema_to_exclude\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --add-to-schema-excludes=\\\"test_schema\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --create=asma,, --pattern=db-shared\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --create=test-group\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --list-extraction-patterns=SERVER\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --update=default\":": 2
  "cdc manage-source-groups | flags: | args: 1, | flags: --update=prod\":": 2
  "cdc manage-source-groups | flags: | args: 2, | flags: --add-extraction-pattern=default\":": 2
  "cdc manage-source-groups | flags: | args: 2, | flags: --add-server=analytics,, --source-type=postgres\":": 2
  "cdc manage-source-groups | flags: | args: 2, | flags: --set-extraction-pattern=default\":": 2
  "cdc manage-source-groups | flags: | args: 4, | flags: --add-extraction-pattern=prod\":": 2
  "cdc manage-source-groups | flags: | args: 4, | flags: --create=my-group\":": 2
  "cdc manage-source-groups | flags: | args: 4, | flags: --list-extraction-patterns=prod\":": 2
  "cdc manage-source-groups | flags: | flags: --,, --introspect-types\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --add-to-ignore-list\\\",\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --add-to-schema-excludes\\\",\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --all,, --update\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --info\":=3,": 2
  "cdc manage-source-groups | flags: | flags: --info\\\",\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --list\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --list-extraction-patterns\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --list-ignore-patterns\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --list-schema-excludes\":=1,": 2
  "cdc manage-source-groups | flags: | flags: --update\":=10,": 2
  "cdc manage-source-groups | flags: | flags: --update\\\",\":=1,": 2
  "cdc reload-cdc-autocompletions": 2
  "cdc scaffold adopus": 2
  "cdc scaffold adopus\": 2,": 2
  "cdc scaffold asma": 2
  "cdc scaffold asma\": 2,": 2
  "cdc scaffold my-group\": 3,": 2
  "cdc scaffold myproject": 2
  "cdc scaffold myproject | flags: --pattern=db-shared, --source-type=postgres": 2
  "cdc scaffold myproject | flags: | args: 2, | flags: --pattern=db-shared,, --source-type=postgres\":": 2
  "cdc scaffold myproject\": 2,": 2
  "cdc scaffold | flags: | args: 1, | flags: --implementation=test,, --pattern=db-shared\":": 2
  "cdc setup-local | flags: --enable-local-sink": 2
  "cdc setup-local | flags: --enable-local-sink, --enable-local-source": 2
  "cdc setup-local | flags: --enable-local-source": 2
  "cdc setup-local | flags: --full": 2
  "cdc setup-local | flags: | flags: --enable-local-sink\":=2,": 2
  "cdc setup-local | flags: | flags: --enable-local-sink,, --enable-local-source\":=2,": 2
  "cdc setup-local | flags: | flags: --enable-local-source\":=2,": 2
  "cdc setup-local | flags: | flags: --full\":=2,": 2
  "cdc test": 2
  "cdc test tests/cli/test_scaffold.py\": 1,": 2
  "cdc test | flags: --all": 2
  "cdc test | flags: --cli": 2
  "cdc test | flags: --fast-pipelines": 2
  "cdc test | flags: --full-pipelines": 2
  "cdc test | flags: -k=scaffold": 2
  "cdc test | flags: -v": 2
  "cdc test | flags: | args: 2, | flags: -k=scaffold\":": 2
  "cdc test | flags: | flags: --all\":=2,": 2
  "cdc test | flags: | flags: --cli\":=2,": 2
  "cdc test | flags: | flags: --fast-pipelines\":=2,": 2
  "cdc test | flags: | flags: --full-pipelines\":=2,": 2
  "cdc test | flags: | flags: -v\":=2,": 2
  "cdc generate | flags: --environment=local, --service=adopus": 1
  "cdc init | flags: --git-init, --name=my-project, --type=adopus": 1
  "cdc init | flags: --name=PROJECT_NAME": 1
  "cdc init | flags: --name=my-project, --target-dir=/path/to/project, --type=adopus": 1
  "cdc init | flags: --name=my-project, --type=adopus": 1
  "cdc manage-column-templates | flags: --add=sync_timestamp": 1
  "cdc manage-column-templates | flags: --remove=tenant_id\",": 1
  "cdc manage-column-templates | flags: --show=tenant_id\",": 1
  "cdc manage-pipelines stress-test": 1
  "cdc manage-pipelines verify-sync": 1
  "cdc manage-server-group | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1
  "cdc manage-server-group | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1
  "cdc manage-server-group | flags: --add-group=adopus": 1
  "cdc manage-server-group | flags: --add-server=analytics, --source-type=postgres": 1
  "cdc manage-server-group | flags: --add-to-ignore-list\",": 1
  "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern": 1
  "cdc manage-server-group | flags: --add-to-ignore-list=\"pattern_to_ignore": 1
  "cdc manage-server-group | flags: --add-to-ignore-list=\"test_pattern": 1
  "cdc manage-server-group | flags: --add-to-schema-excludes\",": 1
  "cdc manage-server-group | flags: --add-to-schema-excludes=\"schema_to_exclude": 1
  "cdc manage-server-group | flags: --add-to-schema-excludes=\"test_schema": 1
  "cdc manage-server-group | flags: --all, --update": 1
  "cdc manage-server-group | flags: --create=asma, --pattern=db-shared": 1
  "cdc manage-server-group | flags: --create=test-group": 1
  "cdc manage-server-group | flags: --info\",": 1
  "cdc manage-server-group | flags: --list-extraction-patterns": 1
  "cdc manage-server-group | flags: --list-extraction-patterns=SERVER": 1
  "cdc manage-server-group | flags: --list-schema-excludes": 1
  "cdc manage-server-group | flags: --refresh": 1
  "cdc manage-server-group | flags: --server-group=adopus, --update": 1
  "cdc manage-server-group | flags: --server-group=asma, --update": 1
  "cdc manage-server-group | flags: --update\",": 1
  "cdc manage-server-group | flags: --update=default": 1
  "cdc manage-server-group | flags: --update=prod": 1
  "cdc manage-service directory | flags: --, --inspect": 1
  "cdc manage-service directory | flags: --, --sink=sink_asma.proxy": 1
  "cdc manage-service | args: dbo.Fraver | flags: --add-source-tables=dbo.Actor, --service=adopus": 1
  "cdc manage-service | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1
  "cdc manage-service | flags: --, --add-sink-table=pub.A, --sink=asma": 1
  "cdc manage-service | flags: --, --add-sink-table=pub.Actor": 1
  "cdc manage-service | flags: --, --add-sink-table=pub.Actor, --service=dir": 1
  "cdc manage-service | flags: --, --add-source-table=Actor": 1
  "cdc manage-service | flags: --, --add-source-table=Actor, --inspect": 1
  "cdc manage-service | flags: --, --modify-custom-table=tbl": 1
  "cdc manage-service | flags: --, --sink-table=pub.Actor, --sink=asma": 1
  "cdc manage-service | flags: --, --sink-table=t, --sink=a": 1
  "cdc manage-service | flags: --, --source-table=Actor": 1
  "cdc manage-service | flags: --add-column-template=tmpl, --sink-": 1
  "cdc manage-service | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1
  "cdc manage-service | flags: --add-sink-table=pub.Actor, --map-": 1
  "cdc manage-service | flags: --add-sink-table=pub.Actor, --sink-": 1
  "cdc manage-service | flags: --add-sink=sink_asma.chat\",, --service=directory": 1
  "cdc manage-service | flags: --add-sink=sink_asma.chat, --service=directory": 1
  "cdc manage-service | flags: --add-source-table=dbo.Actor\",, --service=adopus": 1
  "cdc manage-service | flags: --add-source-table=dbo.Actor, --service=adopus": 1
  "cdc manage-service | flags: --add-source-table=dbo.Orders, --service=myservice": 1
  "cdc manage-service | flags: --add-table=Fraver, --primary-key=fraverid, --service=adopus": 1
  "cdc manage-service | flags: --add-table=MyTable, --primary-key=id, --service=my-service": 1
  "cdc manage-service | flags: --add-table=Orders, --primary-key=order_id, --service=my-service": 1
  "cdc manage-service | flags: --add-table=Users, --primary-key=id, --service=my-service": 1
  "cdc manage-service | flags: --add-validation-database=AdOpusTest\",, --create-service=adopus": 1
  "cdc manage-service | flags: --all\",, --inspect, --service=adopus": 1
  "cdc manage-service | flags: --all, --generate-validation, --service=adopus": 1
  "cdc manage-service | flags: --all, --inspect, --service=myservice": 1
  "cdc manage-service | flags: --all, --inspect-mssql, --service=adopus": 1
  "cdc manage-service | flags: --all, --inspect-sink=sink_asma.calendar, --service=directory": 1
  "cdc manage-service | flags: --create-service, --server=analytics, --service=analytics_data": 1
  "cdc manage-service | flags: --create-service=myservice\",": 1
  "cdc manage-service | flags: --create=adopus, --server-group=adopus": 1
  "cdc manage-service | flags: --create=my-service": 1
  "cdc manage-service | flags: --create=my-service, --server-group=my-group": 1
  "cdc manage-service | flags: --create=my-service, --server-group=my-server-group": 1
  "cdc manage-service | flags: --env=prod\",, --inspect, --service=adopus": 1
  "cdc manage-service | flags: --inspect, --save, --schema=dbo, --service=myservice": 1
  "cdc manage-service | flags: --inspect, --schema=dbo\",, --service=adopus": 1
  "cdc manage-service | flags: --inspect, --schema=dbo, --service=my-service": 1
  "cdc manage-service | flags: --inspect, --schema=dbo, --service=myservice": 1
  "cdc manage-service | flags: --inspect, --service=myservice": 1
  "cdc manage-service | flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory": 1
  "cdc manage-service | flags: --list-services\",": 1
  "cdc manage-service | flags: --list-sinks, --service=directory": 1
  "cdc manage-service | flags: --remove-service=myservice": 1
  "cdc manage-service | flags: --remove-service=myservice\",": 1
  "cdc manage-service | flags: --remove-sink=sink_asma.chat\",, --service=directory": 1
  "cdc manage-service | flags: --remove-table=dbo.Actor\",, --service=adopus": 1
  "cdc manage-service | flags: --remove-table=dbo.Actor, --service=adopus": 1
  "cdc manage-service | flags: --runtime, --service=directory, --validate-config": 1
  "cdc manage-service | flags: --service=directory, --validate-sinks": 1
  "cdc manage-service | flags: --service=proxy": 1
  "cdc manage-service | flags: --source-table=Actor, --track-": 1
  "cdc manage-service-schema | flags: --list": 1
  "cdc manage-service-schema | flags: --list-custom-tables, --service=calendar": 1
  "cdc manage-service-schema | flags: --remove-custom-table=public.my_events, --service=calendar": 1
  "cdc manage-service-schema | flags: --service=calendar": 1
  "cdc manage-service-schema | flags: --service=calendar, --show=public.my_events": 1
  "cdc manage-services config directory 1": 1
  "cdc manage-services config directory 14": 1
  "cdc manage-services config directory 15": 1
  "cdc manage-services config directory 4": 1
  "cdc manage-services config directory | flags: --, --all": 1
  "cdc manage-services config directory | flags: --, --all, --inspect-sink": 1
  "cdc manage-services config directory | flags: --, --inspect": 1
  "cdc manage-services config directory | flags: --, --sink=sink_asma.proxy": 1
  "cdc manage-services config directory | flags: | flags: --al=1": 1
  "cdc manage-services config directory | flags: | flags: --al=14": 1
  "cdc manage-services config directory | flags: | flags: --al=15": 1
  "cdc manage-services config directory | flags: | flags: --al=4": 1
  "cdc manage-services config | flags: --, --add-column-template=tpl, --add-sink-table=pub.Actor, --sink=asma": 1
  "cdc manage-services config | flags: --, --add-column-template=tpl, --sink-table=t, --sink=a": 1
  "cdc manage-services config | flags: --, --add-sink-table=pub.A, --sink=asma": 1
  "cdc manage-services config | flags: --, --add-sink-table=pub.Actor, --service=dir": 1
  "cdc manage-services config | flags: --, --add-sink-table=pub.Actor, --sink=asma": 1
  "cdc manage-services config | flags: --, --add-source-table=Actor": 1
  "cdc manage-services config | flags: --, --add-source-table=Actor, --inspect": 1
  "cdc manage-services config | flags: --, --modify-custom-table=tbl": 1
  "cdc manage-services config | flags: --, --sink-table=pub.Actor, --sink=asma": 1
  "cdc manage-services config | flags: --, --sink-table=t, --sink=a": 1
  "cdc manage-services config | flags: --, --sink=sink_asma.directory": 1
  "cdc manage-services config | flags: --, --source-table=Actor": 1
  "cdc manage-services config | flags: --add-column-template=tmpl, --sink-, --sink=asma": 1
  "cdc manage-services config | flags: --add-sink-table, --fr": 1
  "cdc manage-services config | flags: --add-sink-table=pub.Actor, --map-": 1
  "cdc manage-services config | flags: --add-sink-table=pub.Actor, --sink-": 1
  "cdc manage-services config | flags: --add-sink-table=public.": 1
  "cdc manage-services config | flags: --add-sink=sink_asma.chat\",, --service=directory": 1
  "cdc manage-services config | flags: --add-sink=sink_asma.chat, --service=directory": 1
  "cdc manage-services config | flags: --add-source-table=dbo., --add-source-table=dbo.Address": 1
  "cdc manage-services config | flags: --add-source-table=dbo.Actor\",, --service=adopus": 1
  "cdc manage-services config | flags: --add-source-table=dbo.Actor, --service=adopus": 1
  "cdc manage-services config | flags: --add-table=Actor, --service=adopus": 1
  "cdc manage-services config | flags: --add-validation-database=AdOpusTest\",, --create-service=adopus": 1
  "cdc manage-services config | flags: --all\",, --inspect, --service=adopus": 1
  "cdc manage-services config | flags: --all, --inspect-sink=sink_asma.calendar, --service=directory": 1
  "cdc manage-services config | flags: --create-service, --service=myservice": 1
  "cdc manage-services config | flags: --create-service=directory": 1
  "cdc manage-services config | flags: --create-service=myservice\",": 1
  "cdc manage-services config | flags: --env=prod\",, --inspect, --service=adopus": 1
  "cdc manage-services config | flags: --inspect, --schema=dbo\",, --service=adopus": 1
  "cdc manage-services config | flags: --inspect, --schema=dbo, --service=adopus": 1
  "cdc manage-services config | flags: --inspect, --service=myservice": 1
  "cdc manage-services config | flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory": 1
  "cdc manage-services config | flags: --list-services\",": 1
  "cdc manage-services config | flags: --list-sinks, --service=directory": 1
  "cdc manage-services config | flags: --remove-service=myservice": 1
  "cdc manage-services config | flags: --remove-service=myservice\",": 1
  "cdc manage-services config | flags: --remove-sink=sink_asma.chat\",, --service=directory": 1
  "cdc manage-services config | flags: --remove-table=Test, --service=adopus": 1
  "cdc manage-services config | flags: --remove-table=dbo.Actor\",, --service=adopus": 1
  "cdc manage-services config | flags: --remove-table=dbo.Actor, --service=adopus": 1
  "cdc manage-services config | flags: --service=adopus, --validate-config": 1
  "cdc manage-services config | flags: --service=directory, --sink-all\",, --sink-inspect=sink_asma.calendar": 1
  "cdc manage-services config | flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save\",": 1
  "cdc manage-services config | flags: --service=directory, --validate-sinks": 1
  "cdc manage-services config | flags: --service=proxy": 1
  "cdc manage-services config | flags: --source-table=Actor, --track-": 1
  "cdc manage-services config | flags: | flags: --,, --add-column-template,, --add-sink-table,, --sink=1": 1
  "cdc manage-services config | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=1": 1
  "cdc manage-services schema custom-tables | flags: --service=n": 1
  "cdc manage-sink-groups | flags: --add-new-sink-group=analytics": 1
  "cdc manage-sink-groups | flags: --add-new-sink-group=analytics, --for-source-group=foo, --type=postgres": 1
  "cdc manage-sink-groups | flags: --add-server=prod, --sink-group=sink_analytics": 1
  "cdc manage-sink-groups | flags: --add-to-ignore-list=temp_%\",": 1
  "cdc manage-sink-groups | flags: --add-to-schema-excludes=hdb_catalog\",": 1
  "cdc manage-sink-groups | flags: --create, --source-group=asma\",": 1
  "cdc manage-sink-groups | flags: --info=sink_asma\",": 1
  "cdc manage-sink-groups | flags: --info=sink_foo": 1
  "cdc manage-sink-groups | flags: --introspect-types, --sink-group=sink_analytics": 1
  "cdc manage-sink-groups | flags: --remove=sink_test\",": 1
  "cdc manage-sink-groups | flags: --sink-group=sink_asma, --update": 1
  "cdc manage-source-groups | args: INDEX | flags: --remove-extraction-pattern=SERVER": 1
  "cdc manage-source-groups | args: PATTERN | flags: --add-extraction-pattern=SERVER": 1
  "cdc manage-source-groups | flags: --, --add-server=srv1": 1
  "cdc manage-source-groups | flags: --, --introspect-types": 1
  "cdc manage-source-groups | flags: --add-to-ignore-list\",": 1
  "cdc manage-source-groups | flags: --add-to-ignore-list=\"pattern_to_ignore": 1
  "cdc manage-source-groups | flags: --add-to-ignore-list=\"test_pattern": 1
  "cdc manage-source-groups | flags: --add-to-schema-excludes\",": 1
  "cdc manage-source-groups | flags: --add-to-schema-excludes=\"schema_to_exclude": 1
  "cdc manage-source-groups | flags: --add-to-schema-excludes=\"test_schema": 1
  "cdc manage-source-groups | flags: --all, --update": 1
  "cdc manage-source-groups | flags: --create=asma, --pattern=db-shared": 1
  "cdc manage-source-groups | flags: --create=test-group": 1
  "cdc manage-source-groups | flags: --info\",": 1
  "cdc manage-source-groups | flags: --list": 1
  "cdc manage-source-groups | flags: --list-extraction-patterns": 1
  "cdc manage-source-groups | flags: --list-extraction-patterns=SERVER": 1
  "cdc manage-source-groups | flags: --list-ignore-patterns": 1
  "cdc manage-source-groups | flags: --list-schema-excludes": 1
  "cdc manage-source-groups | flags: --update\",": 1
  "cdc manage-source-groups | flags: --update=default": 1
  "cdc manage-source-groups | flags: --update=prod": 1
  "cdc ms config | flags: --list-services": 1
  "cdc ms config | flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save": 1
  "cdc msc | flags: --add-sink-table, --from=dbo.Actor": 1
  "cdc msc | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink,, --sink-schema=1": 1
  "cdc msc | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=20": 1
  "cdc msc | flags: | flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --sink-schema=2": 1
  "cdc msc | flags: | flags: --create-service=2": 1
  "cdc msc | flags: | flags: --help=1": 1
  "cdc msr custom-tables | flags: | flags: --add-custom-table,, --service\":=3,": 1
  "cdc msr custom-tables | flags: | flags: --add-custom-table=customer_id,, --service=directoryt1n": 1
  "cdc msr custom-tables | flags: | flags: --add-custom-table=customer_id,, --service=directoryt2n": 1
  "cdc msr custom-tables | flags: | flags: --add-custom-tablet1n, --service,": 1
  "cdc msr custom-tables | flags: | flags: --add-custom-tablet2n, --service,": 1
  "cdc scaffold | flags: --implementation=test, --pattern=db-shared": 1
  "cdc setup-local": 1
  "cdc test tests/cli/test_scaffold.py": 1
```

## Human Readable

Generated: 2026-02-23T00:43:52+00:00

### igor.efrem

| command | count |
| --- | ---: |
| cdc manage-service \| flags: --service=directory | 18 |
| cdc manage-services config \| flags: --service=directory | 17 |
| cdc generate | 15 |
| cdc manage-server-group \| flags: --update | 11 |
| cdc manage-services config directory | 11 |
| cdc manage-source-groups \| flags: --update | 10 |
| cdc manage-services schema custom-tables | 9 |
| cdc manage-sink-groups \| flags: --sink-group=sink_asma | 8 |
| cdc manage-service-schema \| flags: --service=chat | 7 |
| cdc manage-services schema custom-tables \| flags: --service=chat | 7 |
| cdc manage-source-groups | 7 |
| cdc manage-server-group \| flags: --create=my-group | 6 |
| cdc manage-server-group | 5 |
| cdc manage-service \| flags: --add-table=Actor, --primary-key=actno, --service=adopus | 5 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=adopus | 5 |
| cdc manage-column-templates \| flags: --add=tenant_id | 4 |
| cdc manage-services config | 4 |
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
| cdc generate \| flags: \| args: 1, \| flags: --environment=local,, --service=adopus": | 2 |
| cdc generate \| flags: \| args: 2, \| flags: --environment=dev,, --service=my-service": | 2 |
| cdc init \| flags: --name=adopus-cdc, --type=adopus | 2 |
| cdc init \| flags: --name=asma-cdc, --type=asma | 2 |
| cdc init \| flags: \| args: 1, \| flags: --git-init,, --name=my-project,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 1, \| flags: --name=PROJECT_NAME": | 2 |
| cdc init \| flags: \| args: 1, \| flags: --name=my-project,, --target-dir=/path/to/project,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 1, \| flags: --name=my-project,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 2, \| flags: --name=adopus-cdc,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 2, \| flags: --name=asma-cdc,, --type=asma": | 2 |
| cdc manage-column-templates \| flags: \| args: 1, \| flags: --add=sync_timestamp": | 2 |
| cdc manage-column-templates \| flags: \| args: 1, \| flags: --remove=tenant_id\",": | 2 |
| cdc manage-column-templates \| flags: \| args: 1, \| flags: --show=tenant_id\",": | 2 |
| cdc manage-column-templates \| flags: \| args: 3, \| flags: --edit=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| args: 3, \| flags: --remove=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| args: 3, \| flags: --show=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| args: 4, \| flags: --add=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| flags: --list":=3, | 2 |
| cdc manage-pipelines stress-test": 1, | 2 |
| cdc manage-pipelines verify-sync": 1, | 2 |
| cdc manage-server-group \| args: 2 \| flags: --remove-extraction-pattern=prod | 2 |
| cdc manage-server-group \| args: 2 \| flags: \| args: 2, \| flags: --remove-extraction-pattern=prod": | 2 |
| cdc manage-server-group \| args: INDEX \| flags: \| args: 1, \| flags: --remove-extraction-pattern=SERVER": | 2 |
| cdc manage-server-group \| args: PATTERN \| flags: \| args: 1, \| flags: --add-extraction-pattern=SERVER": | 2 |
| cdc manage-server-group \| flags: --info | 2 |
| cdc manage-server-group \| flags: --list-ignore-patterns | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-group=adopus": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-server=analytics,, --source-type=postgres": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"pattern": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"pattern_to_ignore": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"test_pattern": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"schema_to_exclude": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"test_schema": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --create=asma,, --pattern=db-shared": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --create=test-group": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --list-extraction-patterns=SERVER": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --update=default": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --update=prod": | 2 |
| cdc manage-server-group \| flags: \| args: 3, \| flags: --list-extraction-patterns=prod": | 2 |
| cdc manage-server-group \| flags: \| args: 6, \| flags: --create=my-group": | 2 |
| cdc manage-server-group \| flags: \| flags: --add-to-ignore-list\",":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --add-to-schema-excludes\",":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --all,, --update":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --info":=2, | 2 |
| cdc manage-server-group \| flags: \| flags: --info\",":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --list":=3, | 2 |
| cdc manage-server-group \| flags: \| flags: --list-extraction-patterns":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --list-ignore-patterns":=2, | 2 |
| cdc manage-server-group \| flags: \| flags: --list-schema-excludes":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --refresh":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --server-group=adopus,, --update":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --server-group=asma,, --update":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --update":=11, | 2 |
| cdc manage-server-group \| flags: \| flags: --update\",":=1, | 2 |
| cdc manage-service directory \| flags: \| args: 1, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-service directory \| flags: \| flags: --,, --inspect":=1, | 2 |
| cdc manage-service \| args: dbo.Fraver \| flags: \| args: 1, \| flags: --add-source-tables=dbo.Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: --add-source-table=public.users, --service=proxy | 2 |
| cdc manage-service \| flags: --add-table=Actor, --service=adopus | 2 |
| cdc manage-service \| flags: --all, --inspect, --service=adopus | 2 |
| cdc manage-service \| flags: --create-service, --service=myservice | 2 |
| cdc manage-service \| flags: --inspect-mssql, --schema=dbo, --service=adopus | 2 |
| cdc manage-service \| flags: --remove-table=Test, --service=adopus | 2 |
| cdc manage-service \| flags: --runtime, --service=directory, --validate-bloblang | 2 |
| cdc manage-service \| flags: --service=adopus | 2 |
| cdc manage-service \| flags: --service=adopus, --validate-config | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.A,, --sink=asma": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.Actor": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.Actor,, --service=dir": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-source-table=Actor": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --modify-custom-table=tbl": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --sink-table=pub.Actor,, --sink=asma": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --source-table=Actor": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-column-template=tmpl,, --sink-,, --sink=asma": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-sink=sink_asma.chat,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-sink=sink_asma.chat\",,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-source-table=dbo.Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-source-table=dbo.Actor\",,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-source-table=dbo.Orders,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=Fraver,, --primary-key=fraverid,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=MyTable,, --primary-key=id,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=Orders,, --primary-key=order_id,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=Users,, --primary-key=id,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-validation-database=AdOpusTest\",,, --create-service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --generate-validation,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --inspect,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --inspect-mssql,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --inspect-sink=sink_asma.calendar,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all\",,, --inspect,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create-service,, --server=analytics,, --service=analytics_data": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create-service=myservice\",": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=adopus,, --server-group=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=my-service,, --server-group=my-group": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=my-service,, --server-group=my-server-group": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --env=prod\",,, --inspect,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --save,, --schema=dbo,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --schema=dbo,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --schema=dbo,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --schema=dbo\",,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect-sink=sink_asma.calendar,, --schema=public,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --list-sinks,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-service=myservice\",": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-sink=sink_asma.chat\",,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-table=dbo.Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-table=dbo.Actor\",,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --service=proxy": | 2 |
| cdc manage-service \| flags: \| args: 18, \| flags: --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --add-source-table=public.users,, --service=proxy": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --add-table=Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --all,, --inspect,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --create-service,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --inspect-mssql,, --schema=dbo,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --remove-table=Test,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 3, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-service \| flags: \| args: 3, \| flags: --add-source-table=dbo.Users,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 5, \| flags: --add-table=Actor,, --primary-key=actno,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 5, \| flags: --inspect,, --schema=dbo,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| flags: --,, --add-source-table=Actor,, --inspect":=1, | 2 |
| cdc manage-service \| flags: \| flags: --add-column-template=tmpl,, --sink-":=1, | 2 |
| cdc manage-service \| flags: \| flags: --add-sink-table=pub.Actor,, --map-":=1, | 2 |
| cdc manage-service \| flags: \| flags: --add-sink-table=pub.Actor,, --sink-":=1, | 2 |
| cdc manage-service \| flags: \| flags: --list-services\",":=1, | 2 |
| cdc manage-service \| flags: \| flags: --runtime,, --service=directory,, --validate-bloblang":=2, | 2 |
| cdc manage-service \| flags: \| flags: --runtime,, --service=directory,, --validate-config":=1, | 2 |
| cdc manage-service \| flags: \| flags: --service=adopus,, --validate-config":=2, | 2 |
| cdc manage-service \| flags: \| flags: --service=directory,, --validate-sinks":=1, | 2 |
| cdc manage-service \| flags: \| flags: --source-table=Actor,, --track-":=1, | 2 |
| cdc manage-service-schema \| flags: --list, --service=chat | 2 |
| cdc manage-service-schema \| flags: --list-services | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --list-custom-tables,, --service=calendar": | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --remove-custom-table=public.my_events,, --service=calendar": | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --service=calendar": | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --service=calendar,, --show=public.my_events": | 2 |
| cdc manage-service-schema \| flags: \| args: 2, \| flags: --list,, --service=chat": | 2 |
| cdc manage-service-schema \| flags: \| args: 7, \| flags: --service=chat": | 2 |
| cdc manage-service-schema \| flags: \| flags: --list":=1, | 2 |
| cdc manage-service-schema \| flags: \| flags: --list-services":=2, | 2 |
| cdc manage-services config directory \| flags: \| args: 1, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-services config directory \| flags: \| flags: --,, --all":=1, | 2 |
| cdc manage-services config directory \| flags: \| flags: --,, --all,, --inspect-sink":=1, | 2 |
| cdc manage-services config directory \| flags: \| flags: --,, --inspect":=1, | 2 |
| cdc manage-services config directory \| flags: \| flags: --al":=1 | 2 |
| cdc manage-services config directory": 1, | 2 |
| cdc manage-services config directory": 11, | 2 |
| cdc manage-services config \| flags: --all, --inspect, --service=adopus | 2 |
| cdc manage-services config \| flags: --service=adopus | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.A,, --sink=asma": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.Actor,, --service=dir": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-source-table=Actor": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --modify-custom-table=tbl": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --sink-table=pub.Actor,, --sink=asma": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --sink=sink_asma.directory": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --source-table=Actor": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --add-column-template=tmpl,, --sink-,, --sink=asma": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --add-sink-table=public.": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --add-source-table=dbo.,, --add-source-table=dbo.Address": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --create-service=directory": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --inspect,, --service=myservice": | 2 |
| cdc manage-services config \| flags: \| args: 3, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-services config \| flags: \| args: 3, \| flags: --add-source-table=dbo.": | 2 |
| cdc manage-services config \| flags: \| flags: --":=3, | 2 |
| cdc manage-services config \| flags: \| flags: --,, --add-source-table=Actor,, --inspect":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --add-sink-table,, --fr":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --add-sink-table=pub.Actor,, --map-":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --add-sink-table=pub.Actor,, --sink-":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --source-table=Actor,, --track-":=1, | 2 |
| cdc manage-services config": 3, | 2 |
| cdc manage-services schema custom-tables \| flags: --list, --service=chat | 2 |
| cdc manage-services schema custom-tables \| flags: --list-services | 2 |
| cdc manage-services schema custom-tables \| flags: \| args: 1, \| flags: --service=n": | 2 |
| cdc manage-services schema custom-tables": 6, | 2 |
| cdc manage-sink-groups | 2 |
| cdc manage-sink-groups \| flags: --add-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --sink-group=sink_analytics, --update | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-new-sink-group=analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-new-sink-group=analytics,, --for-source-group=foo,, --type=postgres": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-server=prod,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-to-ignore-list=temp_%\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-to-schema-excludes=hdb_catalog\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --create,, --source-group=asma\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --info=sink_asma\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --info=sink_foo": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --introspect-types,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --remove=sink_test\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 2, \| flags: --add-server=default,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 2, \| flags: --remove-server=default,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 2, \| flags: --remove=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 3, \| flags: --add-new-sink-group=analytics,, --type=postgres": | 2 |
| cdc manage-sink-groups \| flags: \| args: 3, \| flags: --info=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 4, \| flags: --create,, --source-group=foo": | 2 |
| cdc manage-sink-groups \| flags: \| args: 8, \| flags: --sink-group=sink_asma": | 2 |
| cdc manage-sink-groups \| flags: \| flags: --create":=3, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --list":=3, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --sink-group=sink_analytics,, --update":=2, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --sink-group=sink_asma,, --update":=1, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --validate":=3, | 2 |
| cdc manage-source-groups \| args: 2 \| flags: \| args: 3, \| flags: --remove-extraction-pattern=prod": | 2 |
| cdc manage-source-groups \| args: INDEX \| flags: \| args: 1, \| flags: --remove-extraction-pattern=SERVER": | 2 |
| cdc manage-source-groups \| args: PATTERN \| flags: \| args: 1, \| flags: --add-extraction-pattern=SERVER": | 2 |
| cdc manage-source-groups \| flags: --add-extraction-pattern=default | 2 |
| cdc manage-source-groups \| flags: --add-server=analytics, --source-type=postgres | 2 |
| cdc manage-source-groups \| flags: --set-extraction-pattern=default | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --,, --add-server=srv1": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"pattern_to_ignore": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"test_pattern": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"schema_to_exclude": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"test_schema": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --create=asma,, --pattern=db-shared": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --create=test-group": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --list-extraction-patterns=SERVER": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --update=default": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --update=prod": | 2 |
| cdc manage-source-groups \| flags: \| args: 2, \| flags: --add-extraction-pattern=default": | 2 |
| cdc manage-source-groups \| flags: \| args: 2, \| flags: --add-server=analytics,, --source-type=postgres": | 2 |
| cdc manage-source-groups \| flags: \| args: 2, \| flags: --set-extraction-pattern=default": | 2 |
| cdc manage-source-groups \| flags: \| args: 4, \| flags: --add-extraction-pattern=prod": | 2 |
| cdc manage-source-groups \| flags: \| args: 4, \| flags: --create=my-group": | 2 |
| cdc manage-source-groups \| flags: \| args: 4, \| flags: --list-extraction-patterns=prod": | 2 |
| cdc manage-source-groups \| flags: \| flags: --,, --introspect-types":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --add-to-ignore-list\",":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --add-to-schema-excludes\",":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --all,, --update":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --info":=3, | 2 |
| cdc manage-source-groups \| flags: \| flags: --info\",":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list-extraction-patterns":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list-ignore-patterns":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list-schema-excludes":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --update":=10, | 2 |
| cdc manage-source-groups \| flags: \| flags: --update\",":=1, | 2 |
| cdc reload-cdc-autocompletions | 2 |
| cdc scaffold adopus | 2 |
| cdc scaffold adopus": 2, | 2 |
| cdc scaffold asma | 2 |
| cdc scaffold asma": 2, | 2 |
| cdc scaffold my-group": 3, | 2 |
| cdc scaffold myproject | 2 |
| cdc scaffold myproject \| flags: --pattern=db-shared, --source-type=postgres | 2 |
| cdc scaffold myproject \| flags: \| args: 2, \| flags: --pattern=db-shared,, --source-type=postgres": | 2 |
| cdc scaffold myproject": 2, | 2 |
| cdc scaffold \| flags: \| args: 1, \| flags: --implementation=test,, --pattern=db-shared": | 2 |
| cdc setup-local \| flags: --enable-local-sink | 2 |
| cdc setup-local \| flags: --enable-local-sink, --enable-local-source | 2 |
| cdc setup-local \| flags: --enable-local-source | 2 |
| cdc setup-local \| flags: --full | 2 |
| cdc setup-local \| flags: \| flags: --enable-local-sink":=2, | 2 |
| cdc setup-local \| flags: \| flags: --enable-local-sink,, --enable-local-source":=2, | 2 |
| cdc setup-local \| flags: \| flags: --enable-local-source":=2, | 2 |
| cdc setup-local \| flags: \| flags: --full":=2, | 2 |
| cdc test | 2 |
| cdc test tests/cli/test_scaffold.py": 1, | 2 |
| cdc test \| flags: --all | 2 |
| cdc test \| flags: --cli | 2 |
| cdc test \| flags: --fast-pipelines | 2 |
| cdc test \| flags: --full-pipelines | 2 |
| cdc test \| flags: -k=scaffold | 2 |
| cdc test \| flags: -v | 2 |
| cdc test \| flags: \| args: 2, \| flags: -k=scaffold": | 2 |
| cdc test \| flags: \| flags: --all":=2, | 2 |
| cdc test \| flags: \| flags: --cli":=2, | 2 |
| cdc test \| flags: \| flags: --fast-pipelines":=2, | 2 |
| cdc test \| flags: \| flags: --full-pipelines":=2, | 2 |
| cdc test \| flags: \| flags: -v":=2, | 2 |
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
| cdc manage-services config directory 1 | 1 |
| cdc manage-services config directory 14 | 1 |
| cdc manage-services config directory 15 | 1 |
| cdc manage-services config directory 4 | 1 |
| cdc manage-services config directory \| flags: --, --all | 1 |
| cdc manage-services config directory \| flags: --, --all, --inspect-sink | 1 |
| cdc manage-services config directory \| flags: --, --inspect | 1 |
| cdc manage-services config directory \| flags: --, --sink=sink_asma.proxy | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=1 | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=14 | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=15 | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=4 | 1 |
| cdc manage-services config \| flags: --, --add-column-template=tpl, --add-sink-table=pub.Actor, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --add-column-template=tpl, --sink-table=t, --sink=a | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.A, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.Actor, --service=dir | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.Actor, --sink=asma | 1 |
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
| cdc manage-services config \| flags: --add-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-services config \| flags: --add-sink=sink_asma.chat, --service=directory | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo., --add-source-table=dbo.Address | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-services config \| flags: --add-table=Actor, --service=adopus | 1 |
| cdc manage-services config \| flags: --add-validation-database=AdOpusTest",, --create-service=adopus | 1 |
| cdc manage-services config \| flags: --all",, --inspect, --service=adopus | 1 |
| cdc manage-services config \| flags: --all, --inspect-sink=sink_asma.calendar, --service=directory | 1 |
| cdc manage-services config \| flags: --create-service, --service=myservice | 1 |
| cdc manage-services config \| flags: --create-service=directory | 1 |
| cdc manage-services config \| flags: --create-service=myservice", | 1 |
| cdc manage-services config \| flags: --env=prod",, --inspect, --service=adopus | 1 |
| cdc manage-services config \| flags: --inspect, --schema=dbo",, --service=adopus | 1 |
| cdc manage-services config \| flags: --inspect, --schema=dbo, --service=adopus | 1 |
| cdc manage-services config \| flags: --inspect, --service=myservice | 1 |
| cdc manage-services config \| flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory | 1 |
| cdc manage-services config \| flags: --list-services", | 1 |
| cdc manage-services config \| flags: --list-sinks, --service=directory | 1 |
| cdc manage-services config \| flags: --remove-service=myservice | 1 |
| cdc manage-services config \| flags: --remove-service=myservice", | 1 |
| cdc manage-services config \| flags: --remove-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-services config \| flags: --remove-table=Test, --service=adopus | 1 |
| cdc manage-services config \| flags: --remove-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-services config \| flags: --remove-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-services config \| flags: --service=adopus, --validate-config | 1 |
| cdc manage-services config \| flags: --service=directory, --sink-all",, --sink-inspect=sink_asma.calendar | 1 |
| cdc manage-services config \| flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save", | 1 |
| cdc manage-services config \| flags: --service=directory, --validate-sinks | 1 |
| cdc manage-services config \| flags: --service=proxy | 1 |
| cdc manage-services config \| flags: --source-table=Actor, --track- | 1 |
| cdc manage-services config \| flags: \| flags: --,, --add-column-template,, --add-sink-table,, --sink=1 | 1 |
| cdc manage-services config \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=1 | 1 |
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
| cdc ms config \| flags: --list-services | 1 |
| cdc ms config \| flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save | 1 |
| cdc msc \| flags: --add-sink-table, --from=dbo.Actor | 1 |
| cdc msc \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink,, --sink-schema=1 | 1 |
| cdc msc \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=20 | 1 |
| cdc msc \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --sink-schema=2 | 1 |
| cdc msc \| flags: \| flags: --create-service=2 | 1 |
| cdc msc \| flags: \| flags: --help=1 | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-table,, --service":=3, | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-table=customer_id,, --service=directoryt1n | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-table=customer_id,, --service=directoryt2n | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-tablet1n, --service, | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-tablet2n, --service, | 1 |
| cdc scaffold \| flags: --implementation=test, --pattern=db-shared | 1 |
| cdc setup-local | 1 |
| cdc test tests/cli/test_scaffold.py | 1 |

### total

| command | count |
| --- | ---: |
| cdc manage-service \| flags: --service=directory | 18 |
| cdc manage-services config \| flags: --service=directory | 17 |
| cdc generate | 15 |
| cdc manage-server-group \| flags: --update | 11 |
| cdc manage-services config directory | 11 |
| cdc manage-source-groups \| flags: --update | 10 |
| cdc manage-services schema custom-tables | 9 |
| cdc manage-sink-groups \| flags: --sink-group=sink_asma | 8 |
| cdc manage-service-schema \| flags: --service=chat | 7 |
| cdc manage-services schema custom-tables \| flags: --service=chat | 7 |
| cdc manage-source-groups | 7 |
| cdc manage-server-group \| flags: --create=my-group | 6 |
| cdc manage-server-group | 5 |
| cdc manage-service \| flags: --add-table=Actor, --primary-key=actno, --service=adopus | 5 |
| cdc manage-service \| flags: --inspect, --schema=dbo, --service=adopus | 5 |
| cdc manage-column-templates \| flags: --add=tenant_id | 4 |
| cdc manage-services config | 4 |
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
| cdc generate \| flags: \| args: 1, \| flags: --environment=local,, --service=adopus": | 2 |
| cdc generate \| flags: \| args: 2, \| flags: --environment=dev,, --service=my-service": | 2 |
| cdc init \| flags: --name=adopus-cdc, --type=adopus | 2 |
| cdc init \| flags: --name=asma-cdc, --type=asma | 2 |
| cdc init \| flags: \| args: 1, \| flags: --git-init,, --name=my-project,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 1, \| flags: --name=PROJECT_NAME": | 2 |
| cdc init \| flags: \| args: 1, \| flags: --name=my-project,, --target-dir=/path/to/project,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 1, \| flags: --name=my-project,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 2, \| flags: --name=adopus-cdc,, --type=adopus": | 2 |
| cdc init \| flags: \| args: 2, \| flags: --name=asma-cdc,, --type=asma": | 2 |
| cdc manage-column-templates \| flags: \| args: 1, \| flags: --add=sync_timestamp": | 2 |
| cdc manage-column-templates \| flags: \| args: 1, \| flags: --remove=tenant_id\",": | 2 |
| cdc manage-column-templates \| flags: \| args: 1, \| flags: --show=tenant_id\",": | 2 |
| cdc manage-column-templates \| flags: \| args: 3, \| flags: --edit=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| args: 3, \| flags: --remove=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| args: 3, \| flags: --show=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| args: 4, \| flags: --add=tenant_id": | 2 |
| cdc manage-column-templates \| flags: \| flags: --list":=3, | 2 |
| cdc manage-pipelines stress-test": 1, | 2 |
| cdc manage-pipelines verify-sync": 1, | 2 |
| cdc manage-server-group \| args: 2 \| flags: --remove-extraction-pattern=prod | 2 |
| cdc manage-server-group \| args: 2 \| flags: \| args: 2, \| flags: --remove-extraction-pattern=prod": | 2 |
| cdc manage-server-group \| args: INDEX \| flags: \| args: 1, \| flags: --remove-extraction-pattern=SERVER": | 2 |
| cdc manage-server-group \| args: PATTERN \| flags: \| args: 1, \| flags: --add-extraction-pattern=SERVER": | 2 |
| cdc manage-server-group \| flags: --info | 2 |
| cdc manage-server-group \| flags: --list-ignore-patterns | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-group=adopus": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-server=analytics,, --source-type=postgres": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"pattern": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"pattern_to_ignore": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"test_pattern": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"schema_to_exclude": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"test_schema": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --create=asma,, --pattern=db-shared": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --create=test-group": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --list-extraction-patterns=SERVER": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --update=default": | 2 |
| cdc manage-server-group \| flags: \| args: 1, \| flags: --update=prod": | 2 |
| cdc manage-server-group \| flags: \| args: 3, \| flags: --list-extraction-patterns=prod": | 2 |
| cdc manage-server-group \| flags: \| args: 6, \| flags: --create=my-group": | 2 |
| cdc manage-server-group \| flags: \| flags: --add-to-ignore-list\",":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --add-to-schema-excludes\",":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --all,, --update":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --info":=2, | 2 |
| cdc manage-server-group \| flags: \| flags: --info\",":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --list":=3, | 2 |
| cdc manage-server-group \| flags: \| flags: --list-extraction-patterns":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --list-ignore-patterns":=2, | 2 |
| cdc manage-server-group \| flags: \| flags: --list-schema-excludes":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --refresh":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --server-group=adopus,, --update":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --server-group=asma,, --update":=1, | 2 |
| cdc manage-server-group \| flags: \| flags: --update":=11, | 2 |
| cdc manage-server-group \| flags: \| flags: --update\",":=1, | 2 |
| cdc manage-service directory \| flags: \| args: 1, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-service directory \| flags: \| flags: --,, --inspect":=1, | 2 |
| cdc manage-service \| args: dbo.Fraver \| flags: \| args: 1, \| flags: --add-source-tables=dbo.Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: --add-source-table=public.users, --service=proxy | 2 |
| cdc manage-service \| flags: --add-table=Actor, --service=adopus | 2 |
| cdc manage-service \| flags: --all, --inspect, --service=adopus | 2 |
| cdc manage-service \| flags: --create-service, --service=myservice | 2 |
| cdc manage-service \| flags: --inspect-mssql, --schema=dbo, --service=adopus | 2 |
| cdc manage-service \| flags: --remove-table=Test, --service=adopus | 2 |
| cdc manage-service \| flags: --runtime, --service=directory, --validate-bloblang | 2 |
| cdc manage-service \| flags: --service=adopus | 2 |
| cdc manage-service \| flags: --service=adopus, --validate-config | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.A,, --sink=asma": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.Actor": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.Actor,, --service=dir": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --add-source-table=Actor": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --modify-custom-table=tbl": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --sink-table=pub.Actor,, --sink=asma": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --,, --source-table=Actor": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-column-template=tmpl,, --sink-,, --sink=asma": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-sink=sink_asma.chat,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-sink=sink_asma.chat\",,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-source-table=dbo.Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-source-table=dbo.Actor\",,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-source-table=dbo.Orders,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=Fraver,, --primary-key=fraverid,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=MyTable,, --primary-key=id,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=Orders,, --primary-key=order_id,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-table=Users,, --primary-key=id,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --add-validation-database=AdOpusTest\",,, --create-service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --generate-validation,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --inspect,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --inspect-mssql,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all,, --inspect-sink=sink_asma.calendar,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --all\",,, --inspect,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create-service,, --server=analytics,, --service=analytics_data": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create-service=myservice\",": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=adopus,, --server-group=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=my-service,, --server-group=my-group": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --create=my-service,, --server-group=my-server-group": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --env=prod\",,, --inspect,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --save,, --schema=dbo,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --schema=dbo,, --service=my-service": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --schema=dbo,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --schema=dbo\",,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --inspect-sink=sink_asma.calendar,, --schema=public,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --list-sinks,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-service=myservice\",": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-sink=sink_asma.chat\",,, --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-table=dbo.Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --remove-table=dbo.Actor\",,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 1, \| flags: --service=proxy": | 2 |
| cdc manage-service \| flags: \| args: 18, \| flags: --service=directory": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --add-source-table=public.users,, --service=proxy": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --add-table=Actor,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --all,, --inspect,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --create-service,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --inspect-mssql,, --schema=dbo,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --remove-table=Test,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 2, \| flags: --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 3, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-service \| flags: \| args: 3, \| flags: --add-source-table=dbo.Users,, --service=myservice": | 2 |
| cdc manage-service \| flags: \| args: 5, \| flags: --add-table=Actor,, --primary-key=actno,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| args: 5, \| flags: --inspect,, --schema=dbo,, --service=adopus": | 2 |
| cdc manage-service \| flags: \| flags: --,, --add-source-table=Actor,, --inspect":=1, | 2 |
| cdc manage-service \| flags: \| flags: --add-column-template=tmpl,, --sink-":=1, | 2 |
| cdc manage-service \| flags: \| flags: --add-sink-table=pub.Actor,, --map-":=1, | 2 |
| cdc manage-service \| flags: \| flags: --add-sink-table=pub.Actor,, --sink-":=1, | 2 |
| cdc manage-service \| flags: \| flags: --list-services\",":=1, | 2 |
| cdc manage-service \| flags: \| flags: --runtime,, --service=directory,, --validate-bloblang":=2, | 2 |
| cdc manage-service \| flags: \| flags: --runtime,, --service=directory,, --validate-config":=1, | 2 |
| cdc manage-service \| flags: \| flags: --service=adopus,, --validate-config":=2, | 2 |
| cdc manage-service \| flags: \| flags: --service=directory,, --validate-sinks":=1, | 2 |
| cdc manage-service \| flags: \| flags: --source-table=Actor,, --track-":=1, | 2 |
| cdc manage-service-schema \| flags: --list, --service=chat | 2 |
| cdc manage-service-schema \| flags: --list-services | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --list-custom-tables,, --service=calendar": | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --remove-custom-table=public.my_events,, --service=calendar": | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --service=calendar": | 2 |
| cdc manage-service-schema \| flags: \| args: 1, \| flags: --service=calendar,, --show=public.my_events": | 2 |
| cdc manage-service-schema \| flags: \| args: 2, \| flags: --list,, --service=chat": | 2 |
| cdc manage-service-schema \| flags: \| args: 7, \| flags: --service=chat": | 2 |
| cdc manage-service-schema \| flags: \| flags: --list":=1, | 2 |
| cdc manage-service-schema \| flags: \| flags: --list-services":=2, | 2 |
| cdc manage-services config directory \| flags: \| args: 1, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-services config directory \| flags: \| flags: --,, --all":=1, | 2 |
| cdc manage-services config directory \| flags: \| flags: --,, --all,, --inspect-sink":=1, | 2 |
| cdc manage-services config directory \| flags: \| flags: --,, --inspect":=1, | 2 |
| cdc manage-services config directory \| flags: \| flags: --al":=1 | 2 |
| cdc manage-services config directory": 1, | 2 |
| cdc manage-services config directory": 11, | 2 |
| cdc manage-services config \| flags: --all, --inspect, --service=adopus | 2 |
| cdc manage-services config \| flags: --service=adopus | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-column-template=tpl,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.A,, --sink=asma": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-sink-table=pub.Actor,, --service=dir": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --add-source-table=Actor": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --modify-custom-table=tbl": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --sink-table=pub.Actor,, --sink=asma": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --sink-table=t,, --sink=a": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --sink=sink_asma.directory": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --,, --source-table=Actor": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --add-column-template=tmpl,, --sink-,, --sink=asma": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --add-sink-table=public.": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --add-source-table=dbo.,, --add-source-table=dbo.Address": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --create-service=directory": | 2 |
| cdc manage-services config \| flags: \| args: 1, \| flags: --inspect,, --service=myservice": | 2 |
| cdc manage-services config \| flags: \| args: 3, \| flags: --,, --sink=sink_asma.proxy": | 2 |
| cdc manage-services config \| flags: \| args: 3, \| flags: --add-source-table=dbo.": | 2 |
| cdc manage-services config \| flags: \| flags: --":=3, | 2 |
| cdc manage-services config \| flags: \| flags: --,, --add-source-table=Actor,, --inspect":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --add-sink-table,, --fr":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --add-sink-table=pub.Actor,, --map-":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --add-sink-table=pub.Actor,, --sink-":=1, | 2 |
| cdc manage-services config \| flags: \| flags: --source-table=Actor,, --track-":=1, | 2 |
| cdc manage-services config": 3, | 2 |
| cdc manage-services schema custom-tables \| flags: --list, --service=chat | 2 |
| cdc manage-services schema custom-tables \| flags: --list-services | 2 |
| cdc manage-services schema custom-tables \| flags: \| args: 1, \| flags: --service=n": | 2 |
| cdc manage-services schema custom-tables": 6, | 2 |
| cdc manage-sink-groups | 2 |
| cdc manage-sink-groups \| flags: --add-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove-server=default, --sink-group=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --remove=sink_analytics | 2 |
| cdc manage-sink-groups \| flags: --sink-group=sink_analytics, --update | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-new-sink-group=analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-new-sink-group=analytics,, --for-source-group=foo,, --type=postgres": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-server=prod,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-to-ignore-list=temp_%\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --add-to-schema-excludes=hdb_catalog\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --create,, --source-group=asma\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --info=sink_asma\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --info=sink_foo": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --introspect-types,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 1, \| flags: --remove=sink_test\",": | 2 |
| cdc manage-sink-groups \| flags: \| args: 2, \| flags: --add-server=default,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 2, \| flags: --remove-server=default,, --sink-group=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 2, \| flags: --remove=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 3, \| flags: --add-new-sink-group=analytics,, --type=postgres": | 2 |
| cdc manage-sink-groups \| flags: \| args: 3, \| flags: --info=sink_analytics": | 2 |
| cdc manage-sink-groups \| flags: \| args: 4, \| flags: --create,, --source-group=foo": | 2 |
| cdc manage-sink-groups \| flags: \| args: 8, \| flags: --sink-group=sink_asma": | 2 |
| cdc manage-sink-groups \| flags: \| flags: --create":=3, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --list":=3, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --sink-group=sink_analytics,, --update":=2, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --sink-group=sink_asma,, --update":=1, | 2 |
| cdc manage-sink-groups \| flags: \| flags: --validate":=3, | 2 |
| cdc manage-source-groups \| args: 2 \| flags: \| args: 3, \| flags: --remove-extraction-pattern=prod": | 2 |
| cdc manage-source-groups \| args: INDEX \| flags: \| args: 1, \| flags: --remove-extraction-pattern=SERVER": | 2 |
| cdc manage-source-groups \| args: PATTERN \| flags: \| args: 1, \| flags: --add-extraction-pattern=SERVER": | 2 |
| cdc manage-source-groups \| flags: --add-extraction-pattern=default | 2 |
| cdc manage-source-groups \| flags: --add-server=analytics, --source-type=postgres | 2 |
| cdc manage-source-groups \| flags: --set-extraction-pattern=default | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --,, --add-server=srv1": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"pattern_to_ignore": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-ignore-list=\"test_pattern": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"schema_to_exclude": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --add-to-schema-excludes=\"test_schema": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --create=asma,, --pattern=db-shared": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --create=test-group": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --list-extraction-patterns=SERVER": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --update=default": | 2 |
| cdc manage-source-groups \| flags: \| args: 1, \| flags: --update=prod": | 2 |
| cdc manage-source-groups \| flags: \| args: 2, \| flags: --add-extraction-pattern=default": | 2 |
| cdc manage-source-groups \| flags: \| args: 2, \| flags: --add-server=analytics,, --source-type=postgres": | 2 |
| cdc manage-source-groups \| flags: \| args: 2, \| flags: --set-extraction-pattern=default": | 2 |
| cdc manage-source-groups \| flags: \| args: 4, \| flags: --add-extraction-pattern=prod": | 2 |
| cdc manage-source-groups \| flags: \| args: 4, \| flags: --create=my-group": | 2 |
| cdc manage-source-groups \| flags: \| args: 4, \| flags: --list-extraction-patterns=prod": | 2 |
| cdc manage-source-groups \| flags: \| flags: --,, --introspect-types":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --add-to-ignore-list\",":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --add-to-schema-excludes\",":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --all,, --update":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --info":=3, | 2 |
| cdc manage-source-groups \| flags: \| flags: --info\",":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list-extraction-patterns":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list-ignore-patterns":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --list-schema-excludes":=1, | 2 |
| cdc manage-source-groups \| flags: \| flags: --update":=10, | 2 |
| cdc manage-source-groups \| flags: \| flags: --update\",":=1, | 2 |
| cdc reload-cdc-autocompletions | 2 |
| cdc scaffold adopus | 2 |
| cdc scaffold adopus": 2, | 2 |
| cdc scaffold asma | 2 |
| cdc scaffold asma": 2, | 2 |
| cdc scaffold my-group": 3, | 2 |
| cdc scaffold myproject | 2 |
| cdc scaffold myproject \| flags: --pattern=db-shared, --source-type=postgres | 2 |
| cdc scaffold myproject \| flags: \| args: 2, \| flags: --pattern=db-shared,, --source-type=postgres": | 2 |
| cdc scaffold myproject": 2, | 2 |
| cdc scaffold \| flags: \| args: 1, \| flags: --implementation=test,, --pattern=db-shared": | 2 |
| cdc setup-local \| flags: --enable-local-sink | 2 |
| cdc setup-local \| flags: --enable-local-sink, --enable-local-source | 2 |
| cdc setup-local \| flags: --enable-local-source | 2 |
| cdc setup-local \| flags: --full | 2 |
| cdc setup-local \| flags: \| flags: --enable-local-sink":=2, | 2 |
| cdc setup-local \| flags: \| flags: --enable-local-sink,, --enable-local-source":=2, | 2 |
| cdc setup-local \| flags: \| flags: --enable-local-source":=2, | 2 |
| cdc setup-local \| flags: \| flags: --full":=2, | 2 |
| cdc test | 2 |
| cdc test tests/cli/test_scaffold.py": 1, | 2 |
| cdc test \| flags: --all | 2 |
| cdc test \| flags: --cli | 2 |
| cdc test \| flags: --fast-pipelines | 2 |
| cdc test \| flags: --full-pipelines | 2 |
| cdc test \| flags: -k=scaffold | 2 |
| cdc test \| flags: -v | 2 |
| cdc test \| flags: \| args: 2, \| flags: -k=scaffold": | 2 |
| cdc test \| flags: \| flags: --all":=2, | 2 |
| cdc test \| flags: \| flags: --cli":=2, | 2 |
| cdc test \| flags: \| flags: --fast-pipelines":=2, | 2 |
| cdc test \| flags: \| flags: --full-pipelines":=2, | 2 |
| cdc test \| flags: \| flags: -v":=2, | 2 |
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
| cdc manage-services config directory 1 | 1 |
| cdc manage-services config directory 14 | 1 |
| cdc manage-services config directory 15 | 1 |
| cdc manage-services config directory 4 | 1 |
| cdc manage-services config directory \| flags: --, --all | 1 |
| cdc manage-services config directory \| flags: --, --all, --inspect-sink | 1 |
| cdc manage-services config directory \| flags: --, --inspect | 1 |
| cdc manage-services config directory \| flags: --, --sink=sink_asma.proxy | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=1 | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=14 | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=15 | 1 |
| cdc manage-services config directory \| flags: \| flags: --al=4 | 1 |
| cdc manage-services config \| flags: --, --add-column-template=tpl, --add-sink-table=pub.Actor, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --add-column-template=tpl, --sink-table=t, --sink=a | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.A, --sink=asma | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.Actor, --service=dir | 1 |
| cdc manage-services config \| flags: --, --add-sink-table=pub.Actor, --sink=asma | 1 |
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
| cdc manage-services config \| flags: --add-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-services config \| flags: --add-sink=sink_asma.chat, --service=directory | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo., --add-source-table=dbo.Address | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-services config \| flags: --add-source-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-services config \| flags: --add-table=Actor, --service=adopus | 1 |
| cdc manage-services config \| flags: --add-validation-database=AdOpusTest",, --create-service=adopus | 1 |
| cdc manage-services config \| flags: --all",, --inspect, --service=adopus | 1 |
| cdc manage-services config \| flags: --all, --inspect-sink=sink_asma.calendar, --service=directory | 1 |
| cdc manage-services config \| flags: --create-service, --service=myservice | 1 |
| cdc manage-services config \| flags: --create-service=directory | 1 |
| cdc manage-services config \| flags: --create-service=myservice", | 1 |
| cdc manage-services config \| flags: --env=prod",, --inspect, --service=adopus | 1 |
| cdc manage-services config \| flags: --inspect, --schema=dbo",, --service=adopus | 1 |
| cdc manage-services config \| flags: --inspect, --schema=dbo, --service=adopus | 1 |
| cdc manage-services config \| flags: --inspect, --service=myservice | 1 |
| cdc manage-services config \| flags: --inspect-sink=sink_asma.calendar, --schema=public, --service=directory | 1 |
| cdc manage-services config \| flags: --list-services", | 1 |
| cdc manage-services config \| flags: --list-sinks, --service=directory | 1 |
| cdc manage-services config \| flags: --remove-service=myservice | 1 |
| cdc manage-services config \| flags: --remove-service=myservice", | 1 |
| cdc manage-services config \| flags: --remove-sink=sink_asma.chat",, --service=directory | 1 |
| cdc manage-services config \| flags: --remove-table=Test, --service=adopus | 1 |
| cdc manage-services config \| flags: --remove-table=dbo.Actor",, --service=adopus | 1 |
| cdc manage-services config \| flags: --remove-table=dbo.Actor, --service=adopus | 1 |
| cdc manage-services config \| flags: --service=adopus, --validate-config | 1 |
| cdc manage-services config \| flags: --service=directory, --sink-all",, --sink-inspect=sink_asma.calendar | 1 |
| cdc manage-services config \| flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save", | 1 |
| cdc manage-services config \| flags: --service=directory, --validate-sinks | 1 |
| cdc manage-services config \| flags: --service=proxy | 1 |
| cdc manage-services config \| flags: --source-table=Actor, --track- | 1 |
| cdc manage-services config \| flags: \| flags: --,, --add-column-template,, --add-sink-table,, --sink=1 | 1 |
| cdc manage-services config \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=1 | 1 |
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
| cdc ms config \| flags: --list-services | 1 |
| cdc ms config \| flags: --service=directory, --sink-all, --sink-inspect=sink_asma.calendar, --sink-save | 1 |
| cdc msc \| flags: --add-sink-table, --from=dbo.Actor | 1 |
| cdc msc \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink,, --sink-schema=1 | 1 |
| cdc msc \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --service,, --sink-schema=20 | 1 |
| cdc msc \| flags: \| flags: --add-column-template,, --add-sink-table,, --from,, --replicate-structure,, --sink-schema=2 | 1 |
| cdc msc \| flags: \| flags: --create-service=2 | 1 |
| cdc msc \| flags: \| flags: --help=1 | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-table,, --service":=3, | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-table=customer_id,, --service=directoryt1n | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-table=customer_id,, --service=directoryt2n | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-tablet1n, --service, | 1 |
| cdc msr custom-tables \| flags: \| flags: --add-custom-tablet2n, --service, | 1 |
| cdc scaffold \| flags: --implementation=test, --pattern=db-shared | 1 |
| cdc setup-local | 1 |
| cdc test tests/cli/test_scaffold.py | 1 |
