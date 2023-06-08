from django.contrib import admin

from .models import (
    Connector,
    Datasource,
    DSTColumn,
    PrefixOpc,
    Term,
    TermCategory,
    TermGroup,
    TermMap,
    WFControl,
    WordMap,
    WordTerm,
)


class DatasourceAdmin(admin.ModelAdmin):
    model = Datasource
    list_display = ("datasource", "category", "description")
    list_filter = ["category"]
    search_fields = ["datasource"]


class ChoiceDSTColumn(admin.TabularInline):
    model = DSTColumn
    fieldsets = [
        (
            "Transformation Columns",
            {
                "fields": [
                    "column_number",
                    "column_name",
                    "status",
                    "pre_value",
                    "single_word",
                ],
                "classes": ["collapse"],
            },
        )
    ]
    extra = 0


class ConnectorAdmin(admin.ModelAdmin):
    fieldsets = [
        (
            None,
            {
                "fields": [  # noqa E501
                    "datasource",
                    "connector",
                    "description",
                    "update_ds",
                ]
            },
        ),  # noqa E501
        (
            "Attributes",
            {
                "fields": [
                    "source_web",
                    "source_path",
                    "source_file_name",
                    "source_file_format",
                    "source_file_sep",
                    "source_file_skiprow",
                    "source_compact",
                    "target_file_name",
                    "target_file_format",
                    "target_file_keep",
                ],
                "classes": ["collapse"],
            },
        ),
    ]
    inlines = [ChoiceDSTColumn]

    # model = Connector
    list_display = (
        "datasource",
        "connector",
        "update_ds",
        "target_file_keep",
        "description",
    )
    list_filter = ["update_ds", "datasource"]
    search_fields = ["connector", "description"]


class TermAdmin(admin.ModelAdmin):
    model = Term
    list_display = ("term", "get_termgroup", "get_termcategory", "description")
    list_filter = ["term_group_id", "term_category_id"]
    search_fields = ["term", "description"]

    @admin.display(
        description="Term Group Name", ordering="term_group__term_group"
    )  # noqa E501
    def get_termgroup(self, obj):
        return obj.term_group.term_group

    @admin.display(
        description="Term Category Name",
        ordering="term_category__term_category",  # noqa E501
    )
    def get_termcategory(self, obj):
        return obj.term_category.term_category


class TermMapAdmin(admin.ModelAdmin):
    model = TermMap
    list_display = ("connector", "term_1", "term_2", "qtd_links")
    list_filter = ["connector"]
    autocomplete_fields = ["connector", "term_1", "term_2"]


class WordTermAdmin(admin.ModelAdmin):
    model = WordTerm
    list_display = ("id", "get_term", "word", "status", "commute")
    list_filter = ["status", "commute"]
    search_fields = ["word"]
    list_select_related = ["term"]
    # raw_id_fields = ["term"]
    autocomplete_fields = ["term"]

    @admin.display(description="Term", ordering="term__term")
    def get_term(self, obj):
        return obj.term.term


class WordMapAdmin(admin.ModelAdmin):
    model = WordMap
    list_display = ("connector", "word_1", "word_2", "qtd_links")
    list_filter = ["connector"]
    raw_id_fields = ["connector", "datasource", "term_1", "term_2"]


class WFControlAdmin(admin.ModelAdmin):
    model = WFControl
    list_display = (
        "get_dsStatus",
        "connector",
        "status",
        "last_update",
        # "source_file_version",
        "chk_collect",
        "chk_prepare",
        "chk_map",
        "chk_reduce",
    )
    list_filter = [
        "status",
        "chk_collect",
        "chk_prepare",
        "chk_map",
        "chk_reduce",
        "connector",
    ]  # noqa E501
    search_fields = ["connector__connector"]

    @admin.display(description="DS Status", ordering="connector__update_ds")
    def get_dsStatus(self, obj):
        return obj.connector.update_ds


class DSTCAdmin(admin.ModelAdmin):
    model = DSTColumn
    list_display = (
        "connector",
        "status",
        "column_number",
        "column_name",
        "pre_value",
        "single_word",
    )
    list_filter = ["connector"]


admin.site.register(Datasource, DatasourceAdmin)
admin.site.register(Connector, ConnectorAdmin)
admin.site.register(TermGroup)
admin.site.register(TermCategory)
admin.site.register(PrefixOpc)
admin.site.register(WFControl, WFControlAdmin)
admin.site.register(Term, TermAdmin)
admin.site.register(WordTerm, WordTermAdmin)
admin.site.register(WordMap, WordMapAdmin)
admin.site.register(TermMap, TermMapAdmin)
admin.site.register(DSTColumn, DSTCAdmin)
