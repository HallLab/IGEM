from django.contrib import admin

from .models import DSTColumn, Database, Dataset, Group, Category, Keyge, KeyWord, KeyLink, WFControl, PrefixOpc, WordMap

class DatabaseAdmin(admin.ModelAdmin):
    model = Database
    list_display = ('database', 'category', 'description')
    list_filter = ['category']
    search_fields = ['database']



class ChoiceDSTColumn(admin.TabularInline):
    model = DSTColumn
    fieldsets = [
        ('Transformation Columns',              {'fields': ['column_number','column_name','status','pre_value','single_word'],'classes': ['collapse']})]
    extra = 0


class DatasetAdmin(admin.ModelAdmin):
    fieldsets = [
        (None,              {'fields': ['database','dataset','description','update_ds']}),
        # ('Log',             {'fields': [last_update','source_file_size','target_file_size','source_file_version'],'classes': ['collapse']}),
        ('Attributes',      {'fields': ['source_web','source_path','source_file_name','source_file_format','source_file_sep','source_file_skiprow','source_compact','target_file_name','target_file_format','target_file_keep'],'classes': ['collapse']}), 
    ]
    
    inlines = [ChoiceDSTColumn]

    # model = Dataset
    list_display = ('database', 'dataset', 'update_ds','target_file_keep', 'description')
    list_filter = ['update_ds','database']
    search_fields = ['dataset','description']

class KeygeAdmin(admin.ModelAdmin):
    model = Keyge
    list_display = ('keyge','get_group','get_category','description')
    list_filter = ['group_id','category_id']
    search_fields = ['keyge','description']

    @admin.display(description='Group Name', ordering='group__group')
    def get_group(self, obj):
        return obj.group.group
    
    @admin.display(description='Category Name', ordering='category__category')
    def get_category(self, obj):
        return obj.category.category
    

class KeyLinkAdmin(admin.ModelAdmin):
    model = KeyLink
    list_display = ('dataset','keyge1','keyge2','count')
    list_filter = ['dataset']
    #search_fields = ['keyge1']


class KeyWordAdmin(admin.ModelAdmin):
    model = KeyWord
    list_display = ('get_keyge','word','status','commute')
    list_filter = ['status','commute']
    search_fields = ['word']

    @admin.display(description='Keyge', ordering='keyge__keyge')
    def get_keyge(self, obj):
        return obj.keyge.keyge


class WordMapAdmin(admin.ModelAdmin):
    model = WordMap
    list_display = ('dataset', 'word1', 'word2', 'count')
    list_filter = ['dataset']


class WFControlAdmin(admin.ModelAdmin):
    model = WFControl
    list_display = ('get_dsStatus','dataset','last_update','source_file_version','chk_collect','chk_prepare','chk_map','chk_reduce')
    list_filter = ['dataset','chk_collect','chk_prepare','chk_map','chk_reduce']
    search_fields = ['dataset__dataset']

    @admin.display(description='DS Status', ordering='dataset__update_ds')
    def get_dsStatus(self, obj):
        return obj.dataset.update_ds


class DSTCAdmin(admin.ModelAdmin):
    model = DSTColumn
    list_display = ('dataset','status','column_number','column_name','pre_value','single_word')
    list_filter = ['dataset']


admin.site.register(Database, DatabaseAdmin)
admin.site.register(Dataset, DatasetAdmin)
admin.site.register(Group)
admin.site.register(Category)
admin.site.register(PrefixOpc)
admin.site.register(WFControl, WFControlAdmin)
admin.site.register(Keyge, KeygeAdmin)
admin.site.register(KeyWord, KeyWordAdmin)
admin.site.register(WordMap, WordMapAdmin)
admin.site.register(KeyLink, KeyLinkAdmin)
admin.site.register(DSTColumn, DSTCAdmin)



