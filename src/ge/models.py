from pyexpat import model
from django.db import models, connection


"""
Important:
Whenever you add a new table, you will need to update the Maintenance and Admin processes
"""


# Data Master to collecting data

class Database(models.Model):
    database = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)
    category = models.CharField(max_length=20)
    website = models.CharField(max_length=200)

    def __str__(self):
        return self.database

    class Meta:
        verbose_name_plural = "Database"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class Dataset(models.Model):
    dataset = models.CharField(max_length=20, unique=True)
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    description = models.CharField(max_length=200, default="")
    update_ds = models.BooleanField(default=True, verbose_name="Activate")
    source_path = models.CharField(max_length=300, default="") 
    source_web = models.BooleanField(default=True, verbose_name='Source path from Internet')
    source_compact = models.BooleanField(default=False)
    source_file_name = models.CharField(max_length=200)
    source_file_format = models.CharField(max_length=200)
    source_file_sep = models.CharField(max_length=3, default = ",")
    source_file_skiprow = models.IntegerField(default=0) 
    target_file_name = models.CharField(max_length=200)
    target_file_format = models.CharField(max_length=200)
    target_file_keep = models.BooleanField(default=False, verbose_name='Keep file')

    def __str__(self):
        return self.dataset

    class Meta:
        verbose_name_plural = "Dataset"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class PrefixOpc(models.Model):
    pre_value = models.CharField(max_length=5, primary_key=True, verbose_name='Value Prefix')

    def __str__(self):
        return self.pre_value

    class Meta:
        verbose_name_plural = "Keyge - Prefix"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class DSTColumn(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    status = models.BooleanField(default=False, verbose_name='Active?')
    column_number = models.IntegerField(default=0, verbose_name='Column Sequence')
    column_name = models.CharField(max_length=40, blank=True, verbose_name='Column Name')
    # pre_choice = models.BooleanField(default=False, verbose_name='Prefix?')
    # pre_value = models.CharField(max_length=5, blank=True, verbose_name='Value Prefix')
    pre_value = models.ForeignKey(PrefixOpc, on_delete=models.CASCADE, default='None', verbose_name='Prefix')
    single_word = models.BooleanField(default=False, verbose_name='Single Word')

    class Meta:
        verbose_name_plural = "Dataset - Columns"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class WFControl(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    last_update = models.DateTimeField(verbose_name="Last Update Dataset")
    source_file_version = models.CharField(max_length=500)
    source_file_size = models.BigIntegerField(default=0)
    target_file_size = models.BigIntegerField(default=0)
    chk_collect = models.BooleanField(default=False, verbose_name='Collect Processed')
    chk_prepare = models.BooleanField(default=False, verbose_name='Prepare Processed')
    chk_map = models.BooleanField(default=False, verbose_name='Map Processed')
    chk_reduce = models.BooleanField(default=False, verbose_name='Reduce Processed')

    class Meta:
        verbose_name_plural = "Dataset - Workflow"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class LogsCollector(models.Model):
    source_file_name = models.CharField(max_length=200)
    date = models.DateTimeField(auto_now=False, auto_now_add=False, blank=True, default='')
    dataset = models.CharField(max_length=200)
    database = models.CharField(max_length=200)
    version = models.CharField(max_length=200)
    status = models.BooleanField(default=True)
    size = models.IntegerField(default=0)

    class Meta:
        verbose_name_plural = "Process Log"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


# Master Data to terms control

class Group(models.Model):
    group = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)

    def __str__(self):
        return self.group

    class Meta:
        verbose_name_plural = "Keyge - Group"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class Category(models.Model):
    category = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)

    def __str__(self):
        return self.category

    class Meta:
        verbose_name_plural = "Keyge - Category"
    
    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class Keyge(models.Model):
    keyge = models.CharField(max_length=40, unique=True)
    description = models.CharField(max_length=400)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)

    def __str__(self):
        return self.keyge
    
    class Meta:
        verbose_name_plural = "Keyge"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class KeyHierarchy(models.Model):
    keyge = models.ForeignKey(Keyge, related_name='key_child', on_delete=models.CASCADE, verbose_name='Keyge ID')
    keyge_parent = models.ForeignKey(Keyge, related_name='key_parent', on_delete=models.CASCADE, verbose_name='Keyge Parent ID')

    class Meta:
        verbose_name_plural = "Keyge - Hierarchy"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


# Commute word to keyge
class KeyWord(models.Model):
    word = models.CharField(max_length=400, primary_key=True)
    keyge = models.ForeignKey(Keyge, on_delete=models.CASCADE)
    status = models.BooleanField(default=False, verbose_name='Active?')
    commute = models.BooleanField(default=False, verbose_name='Commute?')

    def __str__(self):
        linker = str(self.keyge) + " - " + str(self.word)
        return linker
    
    class Meta:
        verbose_name_plural = "Keyge - Word"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))



class WordMap(models.Model):
    cword = models.CharField(max_length=15, primary_key=True)
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    keyge1 = models.ForeignKey(Keyge, related_name='key_wordmap_1', blank=True, null=True,
                               on_delete=models.CASCADE)
    keyge2 = models.ForeignKey(Keyge, related_name='key_wordmap_2', blank=True, null=True,
                               on_delete=models.CASCADE)
    word1 = models.CharField(max_length=100)
    word2 = models.CharField(max_length=100)
    count = models.IntegerField(default=0)

    def __str__(self):
        linker = str(self.word1) + " - " + str(self.word2)
        return linker
    
    class Meta:
        verbose_name_plural = "Links - Word"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))


class KeyLink(models.Model):
    ckey = models.CharField(max_length=15, primary_key=True)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    keyge1 = models.ForeignKey(Keyge, related_name='key_keylinks_1',
                               on_delete=models.CASCADE)
    keyge2 = models.ForeignKey(Keyge, related_name='key_keylinks_2',
                               on_delete=models.CASCADE)
    count = models.IntegerField(default=0)

    class Meta:
        verbose_name_plural = "Links - Keyge"

    @classmethod
    def truncate(cls):
        with connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {} CASCADE'.format(cls._meta.db_table))