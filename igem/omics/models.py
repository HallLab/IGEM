from django.db import models
from ge.models import Term


# Create your models here.
class snpgene(models.Model):
    rsid = models.CharField(max_length=15, unique=True, verbose_name="SNP ID")
    observed = models.CharField(max_length=30, verbose_name="observed")
    genomicassembly = models.CharField(max_length=20, verbose_name="Assembly")
    chrom = models.CharField(max_length=5, verbose_name="Chromosome")
    start = models.CharField(max_length=15, verbose_name="Start")
    end = models.CharField(max_length=15, verbose_name="End")
    loctype = models.CharField(max_length=5, verbose_name="Local Type")
    rsorienttochrom = models.CharField(
        max_length=5, verbose_name="Orient Chrom"
    )  # noqa E501
    contigallele = models.CharField(
        max_length=20, verbose_name="Contig Allele"
    )  # noqa E501
    contig = models.CharField(max_length=20, verbose_name="Contig")
    geneid = models.CharField(max_length=15, verbose_name="Gene ID")
    genesymbol = models.CharField(max_length=30, verbose_name="Gene Symbol")


# Model to keep the Gene Range
class GeneMap(models.Model):
    assembly = models.CharField(max_length=20)
    gene_id = models.IntegerField()
    symbol = models.CharField(max_length=50)
    chromosome = models.IntegerField()
    nucleotide_version = models.CharField(max_length=50)
    start_position = models.IntegerField()
    end_position = models.IntegerField()
    orientation = models.CharField(max_length=1)
    term = models.ForeignKey(
        Term,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        default=None
        )

    def __str__(self):
        return self.symbol
