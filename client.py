# This is the only way to test DeleteFromFamily, there is no cbt command that would do this

# pip3 install google-cloud-bigtable
# But first, run in a virtual environment:
# (once) python -m venv venv
# source venv/bin/activate

# this script also needs
# export BIGTABLE_EMULATOR_HOST=localhost:8888
from google.cloud import bigtable

client = bigtable.Client(project="p", admin=True)
instance = client.instance("i")
table = instance.table("cars")
row = table.row("car-1")

# Deletes all cells in the 'history' column family for this row
ret = row.delete_cells(column_family_id="iddup", columns=row.ALL_COLUMNS)
# ret = row.delete()
print(ret)
ret = row.commit()
print(ret)
