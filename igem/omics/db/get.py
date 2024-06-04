import os
import sys

import pandas as pd
from django.conf import settings

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from omics.models import snpgene
except Exception as e:
    print(e)
    raise


def get_data(table, **kwargs):
    """
    The get_data() function allows extracting data from the OMICS database
    and loading this data into a Pandas DataFrame structure or CSV File.

    It has an intelligent filter mechanism that allow you to perform data
    selections simply through a conversion layer of function arguments and SQL
    syntax. This allows the same input arguments regardless of implemented
    database management system.

    Parameters
    ----------
    Only the table parameter will be mandatory, the others being optional, and
    will model the data output. In the case of only informing the table, the
    function will return a DataFrame with all the columns and values of the
    table.

    - table: str
        snp
    - path: str
        With this parameter, the function will save the selected data
        in a file in the directory informed as the parameter argument. In this
        scenario, data will not be returned in the form of a Dataframe; only a
        Boolean value will be returned, informing whether the file was
        generated or not
    - columns: list[“str”]
        Columns that will be selected for output. They must be informed with
        the same name as the database. It is possible to load other data from
        other tables as long as it correlate. For example, suppose the table
        only has the term field and not the category field. In that case, you
        can inform as an argument: "term_id__term_category_id__category", the
        system selected the ID of the term, consulted the ID of the category
        in the Term table, and went to the Category table to choose the
        category
    - columns_out: list[“str”]
        If you want to rename the header of the output fields to more familiar
        names, you can use this parameter, passing the desired names in the
        same sequential sequence in the parameter columns
    - snp: Dict{“str”:list[”str”]}
        Filter argument. It is used to filter SNP ID, with the dictionary
        key being the selection argument and the dictionary value being the
        datasources selected as the filter. Without this parameter, the
        function will return all SNPs
    - gene: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the snp, but applied
        to the GENE ID field
    - chrom: Dict{“str”:list[”str”]}
        Filter argument. It uses the same logic as the snp, but applied
        to the Chrom field
    - columns: list[“str”]
        Columns that will be selected for output. They must be informed with
        the same name as the database. It is possible to load other data from
        other tables as long as it correlate. For example, suppose the table
        only has the term field and not the category field. In that case, you
        can inform as an argument: "term_id__term_category_id__category", the
        system selected the ID of the term, consulted the ID of the category
        in the Term table, and went to the Category table to choose the
        category
    - columns_out: list[“str”]
        If you want to rename the header of the output fields to more familiar
        names, you can use this parameter, passing the desired names in the
        same sequential sequence in the parameter columns

    Return
    ------
    Pandas Dataframe or Boolean (If the parameter path is informed, the
    function will generate the file; if successful, it will return the
    TRUE. Otherwise, it will return FALSE)

    Examples
    --------
    >>> from igem.omics import db
    >>> db.get_data(
            table=”snp”,
            snp={“rsid__in”: ["3901","3903"]},
            gene={“geneid__in”: ["105377223"]},
            )

    >>> db.get_data(
            table=”snp”,
            chrom="y",
            path=”{your_path}/snp_result.csv”
            )
    """

    try:
        v_table = table.lower()
        v_path = kwargs.get("path", "")
        v_snp = kwargs.get("snp", {})
        v_gene = kwargs.get("gene", {})
        v_chrom = kwargs.get("chrom", {})
        v_columns = kwargs.get("columns", [])
        v_columns_out = kwargs.get("columns_out", [])

        if v_table == "snp":
            if not v_columns:
                v_columns = [
                    "rsid",
                    "observed",
                    "genomicassembly",
                    "chrom",
                    "start",
                    "end",
                    "loctype",
                    "rsorienttochrom",
                    "contigallele",
                    "contig",
                    "geneid",
                    "genesymbol",
                ]  # noqa E501
                v_columns_out = v_columns
            v_where_cs = {**v_snp, **v_gene, **v_chrom}
            qs = (
                snpgene.objects.filter(**v_where_cs)
                .values_list(*v_columns)
                .order_by("rsid")
            )

            df = pd.DataFrame(list(qs), columns=v_columns_out)

        else:
            return False

        if v_path:
            if df.empty:
                return False
            if not os.path.isdir(v_path):
                return False
            v_file = v_path + "/" + v_table + ".csv"
            df.to_csv(v_file, index=False)
            return True
        else:
            return df
    except Exception as e:
        return e
