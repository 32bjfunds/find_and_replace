# ETL-Template

## Deliverables

Note: Please list the final products(tables) here. These files should describe the driver process that generates each table.

1. [schema.table_name](outputs/schema.table_name.md)
2.

---

## Folder Structure

- `drivers`:
  - contains main driver scripts leveraging files in `stored-procedures`. These scripts transforms contents from `inputs` to `outputs`
- `inputs`:
  - contains raw files, their location(s), dictionaries and change logs.
- `outputs`:
  - contains documentation for each driver script + diagrams etc.
- `stored-procedures`:
  - scripts that are general enough to be leveraged by driver scripts
