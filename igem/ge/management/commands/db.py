"""
Process to maintain the content of the Igem Database

    $ python manage.py db --get_data {parameters}
    $ python manage.py db --sync_db {parameters}

    $ python manage.py db --get_data 'table="datasource"' # noqa E051
    $ python manage.py db --sync_db 'table="all", path="/root/back"' # noqa E051

"""

import sys

from django.conf import settings
from django.core.management.base import BaseCommand

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from ge import db  # noqa F401
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
            "--sync_db",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="Sync IGEM DB",
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

        # SYNC IGEM DB in Client Version
        if options["sync_db"]:
            parameters = str(options["sync_db"]).lower()
            self.stdout.write(self.style.SUCCESS("Start of IGEM db synchronization for client version"))  # noqa E501
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = db.sync_db(" + parameters + ")", globals(), df)  # noqa E501
                if (df["df"].__class__.__name__) == "DataFrame":
                    print(df["df"])
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))
