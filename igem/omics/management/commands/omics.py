"""
Process to maintain the content of the Igem Database

    $ python manage.py omics --get_data {parameters}

    $ python manage.py omics --get_data 'table="snp"' # noqa E051

    $ python manage.py omics --statistic "table='snp', chrom=['22','2']"
"""

import sys

from django.conf import settings
from django.core.management.base import BaseCommand

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from omics import db  # noqa F401
except:  # noqa E722
    raise


class Command(BaseCommand):
    help = "Process to maintain the content of the OMICS.db"

    def add_arguments(self, parser):
        parser.add_argument(
            "--get_data",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Get data from OMICS.db",
        )

        parser.add_argument(
            "--load_data",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Load data from OMICS.db",
        )

        parser.add_argument(
            "--statistic",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Load data from OMICS.db",
        )

    def handle(self, *args, **options):
        # GET DATA
        if options["get_data"]:
            parameters = str(options["get_data"]).lower()
            self.stdout.write(self.style.SUCCESS("Get data from OMICS.db"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = db.get_data(" + parameters + ")", globals(), df)  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # LOAD DATA
        if options["load_data"]:
            parameters = str(options["load_data"]).lower()
            self.stdout.write(self.style.SUCCESS("Load data from OMICS.db"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec(
                    "df = db.load_data(" + parameters + ")", globals(), df
                )  # noqa E501
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # STATISTIC DATA
        if options["statistic"]:
            parameters = str(options["statistic"]).lower()
            self.stdout.write(
                self.style.SUCCESS("Get statistic data from OMICS.db")
            )  # noqa E501
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec(
                    "df = db.statistic(" + parameters + ")", globals(), df
                )  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))
