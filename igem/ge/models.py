from django.db import connection, models

# Data Master to collecting data
v_version = "0.1.4"


class Datasource(models.Model):
    datasource = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)
    category = models.CharField(max_length=20)
    website = models.CharField(max_length=200)

    def __str__(self):
        return self.datasource

    class Meta:
        verbose_name_plural = "Datasource"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class Connector(models.Model):
    connector = models.CharField(max_length=20, unique=True)
    datasource = models.ForeignKey(Datasource, on_delete=models.CASCADE)
    description = models.CharField(max_length=200, default="")
    update_ds = models.BooleanField(default=True, verbose_name="Activate")
    source_path = models.CharField(max_length=300, default="")
    source_web = models.BooleanField(
        default=True, verbose_name="Source path from Internet"
    )  # noqa E501
    source_compact = models.BooleanField(default=False)
    source_file_name = models.CharField(max_length=200)
    source_file_format = models.CharField(max_length=200)
    source_file_sep = models.CharField(max_length=3, default=",")
    source_file_skiprow = models.IntegerField(default=0)
    target_file_name = models.CharField(max_length=200)
    target_file_format = models.CharField(max_length=200)
    target_file_keep = models.BooleanField(
        default=False, verbose_name="Keep file"
    )  # noqa E501

    def __str__(self):
        return self.connector

    class Meta:
        verbose_name_plural = "Connector"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class PrefixOpc(models.Model):
    pre_value = models.CharField(
        max_length=5, primary_key=True, verbose_name="Value Prefix"
    )  # noqa E501

    def __str__(self):
        return self.pre_value

    class Meta:
        verbose_name_plural = "Term - Prefix"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class DSTColumn(models.Model):
    connector = models.ForeignKey(Connector, on_delete=models.CASCADE)
    status = models.BooleanField(default=False, verbose_name="Active?")
    column_number = models.IntegerField(
        default=0, verbose_name="Column Sequence"
    )  # noqa E501
    column_name = models.CharField(
        max_length=40, blank=True, verbose_name="Column Name"
    )  # noqa E501
    # pre_choice = models.BooleanField(default=False, verbose_name='Prefix?')
    # pre_value = models.CharField(max_length=5, blank=True, verbose_name='Value Prefix')  # noqa E501
    pre_value = models.ForeignKey(
        PrefixOpc,
        on_delete=models.CASCADE,
        default="None",
        verbose_name="Prefix",  # noqa E501
    )  # noqa E501
    single_word = models.BooleanField(
        default=False, verbose_name="Single Word"
    )  # noqa E501

    class Meta:
        verbose_name_plural = "Connector - Fields"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class WFControl(models.Model):
    STATUS_CODE = (
        ("c", "Current"),
        ("o", "Overwrite"),
        ("w", "WorkProcess"),
    )
    connector = models.ForeignKey(Connector, on_delete=models.CASCADE)
    last_update = models.DateTimeField(verbose_name="Last Update Connector")
    source_file_version = models.CharField(
        max_length=500, null=True, blank=True, default=""
    )
    source_file_size = models.BigIntegerField(default=0)
    target_file_size = models.BigIntegerField(default=0)
    chk_collect = models.BooleanField(
        default=False, verbose_name="Collect Processed"
    )  # noqa E501
    chk_prepare = models.BooleanField(
        default=False, verbose_name="Prepare Processed"
    )  # noqa E501
    chk_map = models.BooleanField(default=False, verbose_name="Map Processed")
    chk_reduce = models.BooleanField(
        default=False, verbose_name="Reduce Processed"
    )  # noqa E501
    igem_version = models.CharField(max_length=15, default=v_version)
    status = models.CharField(max_length=1, choices=STATUS_CODE, default="w")
    time_collect = models.IntegerField(default=0)
    time_prepare = models.IntegerField(default=0)
    time_map = models.IntegerField(default=0)
    time_reduce = models.IntegerField(default=0)
    row_collect = models.IntegerField(default=0)
    row_prepare = models.IntegerField(default=0)
    row_map = models.IntegerField(default=0)
    row_reduce = models.IntegerField(default=0)

    class Meta:
        verbose_name_plural = "Workflow"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class Logs(models.Model):
    STATUS_CODE = (
        ("e", "Error"),
        ("w", "Warning"),
        ("s", "Success"),
    )
    process = models.CharField(max_length=65)
    igem_version = models.CharField(max_length=15, default=v_version)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=1, choices=STATUS_CODE, default="s")
    description = models.TextField(null=True, blank=True, default=None)


class TermGroup(models.Model):
    term_group = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)

    def __str__(self):
        return self.term_group

    class Meta:
        verbose_name_plural = "Term - Group"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class TermCategory(models.Model):
    term_category = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)

    def __str__(self):
        return self.term_category

    class Meta:
        verbose_name_plural = "Term - Category"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class Term(models.Model):
    term = models.CharField(max_length=40, unique=True)
    description = models.CharField(max_length=400)
    term_group = models.ForeignKey(TermGroup, on_delete=models.CASCADE)
    term_category = models.ForeignKey(TermCategory, on_delete=models.CASCADE)

    def __str__(self):
        return self.term

    class Meta:
        verbose_name_plural = "Term"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class TermHierarchy(models.Model):
    term = models.ForeignKey(
        Term,
        related_name="key_child",
        on_delete=models.CASCADE,
        verbose_name="Term ID",  # noqa E501
    )  # noqa E501
    term_parent = models.ForeignKey(
        Term,
        related_name="key_parent",
        on_delete=models.CASCADE,
        verbose_name="Term Parent ID",
    )  # noqa E501

    class Meta:
        verbose_name_plural = "Term - Hierarchy"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


# Commute word to keyge
class WordTerm(models.Model):
    word = models.CharField(max_length=400, unique=True)
    term = models.ForeignKey(Term, on_delete=models.CASCADE)
    status = models.BooleanField(default=False, verbose_name="Active?")
    commute = models.BooleanField(default=False, verbose_name="Commute?")

    def __str__(self):
        linker = str(self.term) + " - " + str(self.word)
        return linker

    class Meta:
        verbose_name_plural = "Word to Terms"
        indexes = [
            models.Index(
                fields=[
                    "term",
                ]
            ),
        ]

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class WordMap(models.Model):
    cword = models.CharField(max_length=15, unique=True)
    datasource = models.ForeignKey(Datasource, on_delete=models.CASCADE)
    connector = models.ForeignKey(Connector, on_delete=models.CASCADE)
    term_1 = models.ForeignKey(
        Term,
        related_name="term_wordmap_1",
        blank=True,
        null=True,  # noqa E501
        on_delete=models.CASCADE,
    )
    term_2 = models.ForeignKey(
        Term,
        related_name="term_wordmap_2",
        blank=True,
        null=True,  # noqa E501
        on_delete=models.CASCADE,
    )
    word_1 = models.CharField(max_length=100)
    word_2 = models.CharField(max_length=100)
    qtd_links = models.IntegerField(default=0)

    def __str__(self):
        linker = str(self.word_1) + " - " + str(self.word_2)
        return linker

    class Meta:
        verbose_name_plural = "Word Map"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501


class TermMap(models.Model):
    ckey = models.CharField(max_length=15, primary_key=True)
    connector = models.ForeignKey(Connector, on_delete=models.CASCADE)
    term_1 = models.ForeignKey(
        Term, related_name="term_termmap_1", on_delete=models.CASCADE
    )
    term_2 = models.ForeignKey(
        Term, related_name="term_termmap_2", on_delete=models.CASCADE
    )
    qtd_links = models.IntegerField(default=0)

    class Meta:
        verbose_name_plural = "Terms Map"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE {} CASCADE".format(cls._meta.db_table)
            )  # noqa E501
