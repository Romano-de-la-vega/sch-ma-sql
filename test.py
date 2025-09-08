import schema_loader

schema = schema_loader.load_schema(r"tentative decomposer html\output.json")
tables = schema_loader.pick_tables_for_question(schema, "10 projets")
print(schema_loader.compress_to_handles(schema, tables))
