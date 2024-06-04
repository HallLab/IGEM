from django.db import models


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
