Map
---

The map will process each line of the file and combine all found words. The result will be recorded in the WordMap table::
    # python manage.py map --run {all or dataset}

It will start the data term switching phase for all DATASETs or just one specified. Essential to have the file in PSA. Otherwise, the system will display a warning::
    $ python manage.py commute --reset {all or dataset}

The reset option will reset the control for all or a specific DATASET to the switching phase and the next phase
