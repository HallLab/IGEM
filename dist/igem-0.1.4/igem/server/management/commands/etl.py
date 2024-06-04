"""
Process to maintain the content of the Igem Database

    $ python manage.py etl --collect {parameters}
    $ python manage.py etl --prepare {parameters}
    $ python manage.py etl --map {parameters}
    $ python manage.py etl --reduce {parameters}

    $ python manage.py etl --collect 'connector="ctdcgint"'
    $ python manage.py etl --prepare 'connector="ctdcgint"'
    $ python manage.py etl --map 'connector="ctdcgint"'
    $ python manage.py etl --reduce 'connector="ctdcgint"'
"""

import sys

from django.conf import settings
from django.core.management.base import BaseCommand

try:
    x = str(settings.BASE_DIR)
    sys.path.append(x)
    from server import etl  # noqa F401
except:  # noqa E722
    raise


class Command(BaseCommand):
    help = "Process to maintain the content of the GE.db"

    def add_arguments(self, parser):
        parser.add_argument(
            "--collect",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="collect datasource data",
        )

        parser.add_argument(
            "--prepare",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="prepare data to merge",
        )

        parser.add_argument(
            "--map",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="map links terms",
        )

        parser.add_argument(
            "--reduce",
            type=str,
            metavar="parameters",
            action="store",
            default=None,
            help="reduce terms links mapped",
        )

    def handle(self, *args, **options):
        # ETL COLLECT
        if options["collect"]:
            parameters = str(options["collect"]).lower()
            self.stdout.write(self.style.SUCCESS("collect connector data"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = etl.collect(" + parameters + ")", globals(), df)  # noqa E501
                if df["df"]:
                    self.stdout.write(
                        self.style.HTTP_REDIRECT("Collect Finish")  # noqa E501
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR_OUTPUT("Error on Collect")
                    )  # noqa E501
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # ETL PREPARE
        if options["prepare"]:
            parameters = str(options["prepare"]).lower()
            self.stdout.write(self.style.SUCCESS("prepare datasource data"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = etl.prepare(" + parameters + ")", globals(), df)  # noqa E501
                if df["df"]:
                    self.stdout.write(
                        self.style.HTTP_REDIRECT("Prepare Finish")  # noqa E501
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR_OUTPUT("Error on Prepare")
                    )  # noqa E501
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # ETL MAP
        if options["map"]:
            parameters = str(options["map"]).lower()
            self.stdout.write(self.style.SUCCESS("Map datasource data"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = etl.map(" + parameters + ")", globals(), df)  # noqa E501
                if df["df"]:
                    self.stdout.write(
                        self.style.HTTP_REDIRECT("Map Finish")  # noqa E501
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR_OUTPUT("Error on Map")
                    )  # noqa E501
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))

        # ETL REDUCE
        if options["reduce"]:
            parameters = str(options["reduce"]).lower()
            self.stdout.write(self.style.SUCCESS("Reduce datasource data"))
            self.stdout.write(
                self.style.HTTP_REDIRECT(
                    f"  Informed parameters: {parameters}"
                )  # noqa E501
            )
            print()  # give a space
            # Run function
            try:
                df = {}
                exec("df = etl.reduce(" + parameters + ")", globals(), df)  # noqa E501
                if df["df"]:
                    self.stdout.write(
                        self.style.HTTP_REDIRECT("Reduce Finish")  # noqa E501
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR_OUTPUT("Error on Reduce")
                    )  # noqa E501
            except Exception as e:
                self.stdout.write(self.style.ERROR_OUTPUT(f"  {e}"))
