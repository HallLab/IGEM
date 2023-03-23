Prepare
-------
This second phase of the process aims to transform the original data, thus reducing the need for computational resources in the subsequent steps. Based on the briefly configured DATASET parameters, in this phase, we will have:
    * Deleting header lines
    * Deleting unnecessary columns
    * Transforming ID Columns with Suffix Identifiers
    * Replacement the terms
    * Deletion of the original file

The output will be a new temporary file for consumption in the next phase::
    # python manage.py prepare --run {all or dataset}

It will start the data preparation phase for all DATASETs or just one specified. Essential to have the file in PSA. Otherwise, the system will display a warning::
    $ python manage.py prepare --reset {all or dataset}

The reset option will reset the control for all or a specific DATASET in the preparation phase and the two later ones.
