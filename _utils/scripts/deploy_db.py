try:
    import os
    import sys
    from pathlib import Path
    v_root = Path(__file__).parents[2]
    sys.path.append(
        os.path.abspath(v_root)
    )
except Exception as e:
    print("erro: ", e)
    raise

from igem.ge import db

v_load = v_root / "load" / "to_start"

# # Delete all data
# db.truncate_table(table='term_group')
# db.truncate_table(table='term_category')
# db.truncate_table(table='term')
# db.truncate_table(table='wordterm')
# db.truncate_table(table='termmap')

# # LOAD DATA
db.load_data(
    table='datasource',
    path=str(v_load / '01_datasource.csv')
)
db.load_data(
    table='connector',
    path=str(v_load / '02_connector.csv')
)
db.load_data(
    table='prefix',
    path=str(v_load / '03_prefix.csv')
)
db.load_data(
    table='ds_column',
    path=str(v_load / '04_ds_column.csv')
)
db.load_data(
    table='term_group',
    path=str(v_load / '05_term_group.csv')
)
db.load_data(
    table='term_category',
    path=str(v_load / '06_term_category.csv')
)


db.load_data(
    table='term',
    path=str(v_load / '07_terms' / 'term-genes.csv')
)

db.load_data(
    table='wordterm',
    path=str(v_load / '08_word_terms' / 'word_terms-gene.csv')
)
