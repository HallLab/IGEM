try:
    import os
    import sys
    from pathlib import Path

    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem.ge import db

path_data = os.path.dirname(__file__) + "/data"

# # Update files to deploy in data folder
db.get_data(table="datasource", path=path_data)
db.get_data(
    table="connector",
    path=path_data,
    columns=[
        "connector",
        "datasource_id__datasource",
        "update_ds",
        "source_path",
        "source_web",
        "source_compact",
        "source_file_name",
        "source_file_format",
        "source_file_sep",
        "source_file_skiprow",
        "target_file_name",
        "target_file_format",
        "description",
    ],
    columns_out=[
        "connector",
        "datasource",
        "update_ds",
        "source_path",
        "source_web",
        "source_compact",
        "source_file_name",
        "source_file_format",
        "source_file_sep",
        "source_file_skiprow",
        "target_file_name",
        "target_file_format",
        "description",
    ],
)
db.get_data(table="prefix", path=path_data)
db.get_data(
    table="ds_column",
    path=path_data,
    columns=[
        "connector_id__connector",
        "status",
        "column_number",
        "column_name",
        "pre_value",
        "single_word",
    ],
    columns_out=[
        "connector",
        "status",
        "column_number",
        "column_name",
        "pre_value",
        "single_word",
    ],
)
db.get_data(table="term_group", path=path_data)
db.get_data(table="term_category", path=path_data)
db.get_data(
    table="term",
    path=path_data,
    columns=[
        "term",
        "term_group_id__term_group",
        "term_category_id__term_category",
        "description",
    ],
    columns_out=["term", "term_group", "term_category", "description"],
)
db.get_data(
    table="wordterm",
    path=path_data,
    columns=["term_id__term", "word", "status", "commute"],
    columns_out=["term", "word", "status", "commute"],
)
print("All data files created")
