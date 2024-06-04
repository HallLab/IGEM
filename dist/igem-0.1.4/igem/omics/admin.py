from django.contrib import admin

from .models import snpgene

# Register your models here.


class snpgeneAdmin(admin.ModelAdmin):
    model = snpgene
    list_display = (
        "rsid",
        "chrom",
        "start",
        "end",
        "loctype",
        "rsorienttochrom",
        "contigallele",
        "contig",
        "geneid",
        "genesymbol",
    )

    list_filter = ["chrom"]

    search_fields = [
        "rsid",
        "chrom",
        "start",
        "geneid",
        "genesymbol",
    ]


admin.site.register(snpgene, snpgeneAdmin)
