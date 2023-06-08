"""
Process to maintain the content of the Igem Database

    $ python manage.py db --get_data {parameters}
    $ python manage.py db --load_data {parameters}
    $ python manage.py db --delete_data {parameters}
    $ python manage.py db --truncate_table {parameters}
    $ python manage.py db --backup {parameters}
    $ python manage.py db --restore {parameters}

    $ python manage.py db --get_data 'table="datasource"' # noqa E051
"""

import sys

from django.conf import settings
from django.core.management.base import BaseCommand

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from server import sql  # noqa F401
except:  # noqa E722
    raise


class Command(BaseCommand):
    help = "Process to maintain the content of the GE.db"

    def add_arguments(self, parser):
        parser.add_argument(
            "--get_data",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Get data from GE.db",
        )

        parser.add_argument(
            "--load_data",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Load data from GE.db",
        )

        parser.add_argument(
            "--delete_data",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Delete data in GE.db",
        )

        parser.add_argument(
            "--truncate_table",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Trncate table in GE.db",
        )

        parser.add_argument(
            "--backup",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Trncate table in GE.db",
        )

        parser.add_argument(
            "--restore",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Trncate table in GE.db",
        )

    def handle(self, *args, **options):
        # GET DATA
        if options["get_data"]:
            parameters = str(options["get_data"]).lower()
            self.stdout.write(self.style.SUCCESS("Get data from GE.db"))
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
            self.stdout.write(self.style.SUCCESS("Load data from GE.db"))
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
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # DELETE DATA
        if options["delete_data"]:
            parameters = str(options["delete_data"]).lower()
            self.stdout.write(self.style.SUCCESS("Delete data from GE.db"))
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
                    "df = db.delete_data(" + parameters + ")", globals(), df
                )  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # TRUNCATE TABLE
        if options["truncate_table"]:
            parameters = str(options["truncate_table"]).lower()
            self.stdout.write(self.style.SUCCESS("Truncate table in GE.db"))
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
                    "df = db.truncate_table(" + parameters + ")", globals(), df
                )  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # BACKUP TABLE
        if options["backup"]:
            parameters = str(options["backup"]).lower()
            self.stdout.write(self.style.SUCCESS("backup table in GE.db"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = db.backup(" + parameters + ")", globals(), df)  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # RESTORE TABLE
        if options["restore"]:
            parameters = str(options["restore"]).lower()
            self.stdout.write(self.style.SUCCESS("restore table in GE.db"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = db.restore(" + parameters + ")", globals(), df)  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))
