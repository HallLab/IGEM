# Loading data

Every IGEM workflow starts here: getting genotypes, phenotypes, and
summary statistics into memory in a form the analytical modules
(`describe`, `modify`, `analyze`) can consume directly. The `data`
module is intentionally *only* about I/O — wrappers, lazy loading,
and schema normalization. Anything that *computes* (means,
correlations, GWAS) lives elsewhere.

```{tip}
Everything on this page is **local**. The `data` module never talks
to the IGEM server, so it works exactly the same in laptop, embedded,
and HPC modes. No `server-url` configuration is needed.
```

```{note}
**No CLI commands for this module.** Loading produces an in-memory
wrapper (`Genotypes`, `Phenotypes`, or a `pandas.DataFrame`) that
has to flow into the next analytical step within the same Python
session — there is no useful terminal artifact to print, so a CLI
form would only ever say *"loaded N samples"* and exit. The `igem`
CLI is reserved for operations that *do* produce one: `igem report
run` (CSV / table), `igem health` (status line), `igem config`
(TOML file). Use Python or a notebook for everything in this
module.
```

By the end of this page you will know how to:

- Load **genotypes** from PLINK, VCF, or Zarr / VCZ stores.
- Load **phenotypes** from CSV / TSV / NHANES XPT, with role metadata
  (outcomes / covariates / exposures) and survey-design columns
  attached.
- Load **arbitrary tabular files** (annotations, gene lists, SNP→gene
  maps) into a plain DataFrame.
- Load **GWAS / PheWAS summary statistics** from PLINK 2, REGENIE,
  BOLT-LMM, or GWAS Catalog into a single canonical schema.

All loaders are accessible two ways. From the `IGEM` facade — the
form used in the rest of this guide — you get an extra log line for
provenance:

```python
from igem import IGEM

with IGEM() as igem:
    geno = igem.data.read_plink("cohort")
    phen = igem.data.read_phenotypes("nhanes.csv", outcomes=["GLUCOSE"])
```

Or as plain free functions when you do not need an `IGEM` instance:

```python
from igem.modules.data import read_plink, read_phenotypes

geno = read_plink("cohort")
phen = read_phenotypes("nhanes.csv", outcomes=["GLUCOSE"])
```

The two forms are interchangeable; the rest of this page uses the
facade form.

---

## 1. Genotypes

`Genotypes` is a thin wrapper around an [sgkit]-format
`xarray.Dataset`. The underlying Dataset is **lazy**: it only
materialises the variants and samples you reference, so opening a
50 000-sample biobank file is instant — the cost is in the analyses
that follow, scoped by what you actually compute.

[sgkit]: https://sgkit-dev.github.io/sgkit/latest/

### Read PLINK 1.x

The triplet `prefix.bed` / `prefix.bim` / `prefix.fam`:

```python
geno = igem.data.read_plink("path/to/cohort")
# [data] read_plink(path/to/cohort)
# [data] loaded 2_504 samples × 1_103_547 variants from PLINK
```

### Read VCF / BCF / vcf.gz

VCF is read by **converting it to VCZ** (Zarr) once and reusing the
cache on every subsequent call. The conversion is the expensive
step; subsequent loads are fast.

```python
geno = igem.data.read_vcf(
    "cohort.vcf.gz",
    show_progress=True,        # bio2zarr progress bar
    worker_processes=4,        # parallelise the conversion
)
```

Re-run the same command later — the VCZ cache (`cohort.vcz` next to
the input by default) is reused automatically. Pass
`force=True` to rebuild it, or `vcz_path=` to write the cache to a
different location.

### Read Zarr / VCZ directly

If your pipeline already produced a VCZ store (via
`bio2zarr.vcf.convert` or `sgkit.save_dataset`), skip the conversion:

```python
geno = igem.data.read_zarr("cohort.vcz")
```

### Inspect

```python
geno.n_samples            # int
geno.n_variants           # int
geno.samples              # pandas.Index of sample IDs
geno.variants             # pandas.DataFrame: variant_id, contig, position, ref, alt
```

Multi-allelic sites collapse their alt alleles into a comma-separated
string in `geno.variants["alt"]`, so the table always has one row
per variant.

### Subset (lazy)

`select` is the canonical way to scope down before any heavy
computation. Subsetting stays **lazy** under the hood (it uses
`xarray.isel`), so chaining multiple selections does not
materialise intermediate arrays.

```python
sub = geno.select(
    samples=["S0001", "S0042", "S0103"],
    variants=["rs429358", "rs7412"],   # APOE
)
sub.n_samples, sub.n_variants
# (3, 2)
```

Boolean masks are also accepted:

```python
import numpy as np
mask = np.array([v.startswith("rs") for v in geno.variants["variant_id"]])
rs_only = geno.select(variant_mask=mask)
```

### Materialise

When you finally need a NumPy array, call `to_numpy`. The shape is
`(n_variants, n_samples, ploidy)`:

```python
calls = sub.to_numpy()
calls.shape   # (2, 3, 2)
```

```{warning}
`.to_numpy()` loads the genotype call array into RAM. **Always
subset first** on biobank-scale data — calling it on the full
Dataset can easily consume tens of GB.
```

---

## 2. Phenotypes

`Phenotypes` wraps a `pandas.DataFrame` and tracks the **role** of
each column — which is the outcome, which are covariates, which are
exposures, and which are survey-design columns. Downstream analysis
modules read those roles directly, so the column lists are
specified once at load time and never repeated.

### Read CSV / TSV / Parquet / DataFrame

```python
phen = igem.data.read_phenotypes(
    "nhanes.csv",
    sample_id_col="SEQN",            # default: "sample_id"
    outcomes=["GLUCOSE"],
    covariates=["AGE", "SEX"],
    exposures=["BMI"],
    weights_col="WTMEC",             # survey design (optional)
    strata_col="SDMVSTRA",
    cluster_col="SDMVPSU",
)
# [data] read_phenotypes(source='nhanes.csv')
# [data] loaded 4 samples (outcomes=1, covariates=2, exposures=1)
```

The format is inferred from the suffix:

| Suffix                  | Reader              |
| ----------------------- | ------------------- |
| `.xpt`                  | SAS XPT (NHANES)    |
| `.tsv`, `.txt`          | tab-separated       |
| `.csv` (and any other)  | `pd.read_csv`       |

You can also pass a `pandas.DataFrame` directly to skip file I/O:

```python
phen = igem.data.read_phenotypes(my_df, outcomes=["GLUCOSE"])
```

If a column you declare as outcome / covariate / exposure / weights /
strata / cluster is **not in the frame**, the loader fails fast with
a `ValueError` — better to catch the typo at load time than during
analysis.

### Read NHANES XPT shortcut

NHANES participant IDs are stored in `SEQN`, not `sample_id`, so the
default `read_phenotypes` invocation always needs that override.
`read_nhanes_xpt` pre-fills it:

```python
phen = igem.data.read_nhanes_xpt(
    "DEMO_J.XPT",
    outcomes=["GLUCOSE"],
)
```

### Inspect

```python
phen.n_samples
phen.samples                     # pandas.Index named "SEQN"
phen.df                          # the underlying frame
phen.outcomes_df()               # SEQN + outcome columns
phen.covariates_df()             # SEQN + covariate columns
phen.exposures_df()              # SEQN + exposure columns
```

### Subset

`select_samples` keeps the role metadata intact:

```python
sub = phen.select_samples(["S001", "S003"])
sub.outcomes        # ['GLUCOSE'] — preserved from the parent
sub.weights_col     # 'WTMEC'    — preserved from the parent
```

---

## 3. Generic tabular data

`read_table` is the catch-all for **anything that is not a phenotype
or a sumstats file** — annotation tables, gene lists, SNP-to-gene
maps, manifest files, ad-hoc result tables. It returns a plain
`pandas.DataFrame` with no wrapping and no validation.

```python
genes = igem.data.read_table("biofilter_pathway_genes.tsv")
# [data] read_table(biofilter_pathway_genes.tsv)
# [data] loaded 412 rows × 3 columns
```

Format is inferred from the suffix:

| Suffix                        | Reader              |
| ----------------------------- | ------------------- |
| `.parquet`                    | `pd.read_parquet`   |
| `.xpt`                        | SAS XPT             |
| `.tsv`, `.tsv.gz`, `.txt`, `.txt.gz` | `pd.read_table` (tab) |
| anything else (`.csv`, `.csv.gz`, ...) | `pd.read_csv`     |

Gzip-compressed CSV / TSV are decompressed transparently. Any
`**kwargs` you pass go straight to the underlying pandas reader —
useful for non-default separators or comment lines:

```python
df = igem.data.read_table("table.csv", sep=";", comment="#")
```

```{tip}
Use `read_table` only when you **just need a DataFrame**. For
phenotype data with role metadata, use `read_phenotypes` so the
analytical modules can pick up the outcome / covariate / exposure
information automatically.
```

---

## 4. GWAS / PheWAS summary statistics

`read_sumstats` reads the per-variant output of a GWAS / PheWAS run
(`BETA`, `SE`, `P` per SNP) and normalises every supported tool to
the same **canonical schema**:

```text
variant_id, chrom, pos, effect_allele, other_allele,
beta, se, pval, n, eaf
```

So a downstream module can consume the result without caring whether
the upstream tool was PLINK, REGENIE, or BOLT-LMM.

### By preset

Pass `preset=` for the four built-in tool conventions:

```python
sumstats = igem.data.read_sumstats(
    "trait.glm.linear",
    preset="plink2",
)
# [data] read_sumstats(trait.glm.linear, preset='plink2')
# [data] loaded 1_103_547 variants from sumstats
#        (columns: ['variant_id', 'chrom', 'pos', 'effect_allele',
#                   'other_allele', 'beta', 'se', 'pval', 'n', 'eaf'])
```

Available presets:

| Preset           | Source                                          | Notes                                  |
| ---------------- | ----------------------------------------------- | -------------------------------------- |
| `plink2`         | PLINK 2 `.glm.linear` / `.glm.logistic`         |                                        |
| `regenie`        | REGENIE step 2                                  | Auto-converts `LOG10P` → plain p-value |
| `bolt`           | BOLT-LMM                                        | No `n` column emitted by default       |
| `gwas-catalog`   | GWAS Catalog harmonized TSV                     |                                        |

### By custom schema

For tools without a built-in preset (SAIGE, fastGWA, METAL, GCTA-COJO,
or your own format), pass `schema=` mapping canonical names to source
column names:

```python
sumstats = igem.data.read_sumstats(
    "trait.saige.tsv",
    schema={
        "variant_id":    "MarkerID",
        "chrom":         "CHR",
        "pos":           "POS",
        "effect_allele": "Allele2",
        "other_allele":  "Allele1",
        "beta":          "BETA",
        "se":            "SE",
        "pval":          "p.value",
        "n":             "N",
        "eaf":           "AF_Allele2",
    },
)
```

### Preset + schema override

When *one* column deviates from the tool default but the rest is
standard, combine `preset=` with a partial `schema=`:

```python
# PLINK 2 file with a custom variant ID column.
sumstats = igem.data.read_sumstats(
    "trait.glm.linear",
    preset="plink2",
    schema={"variant_id": "RSID"},
)
```

```{note}
**Default separator.** Sumstats files in the wild use tab-separated
layout regardless of suffix (`.glm.linear`, `.regenie`, `.bolt`,
…). For any suffix `read_table` does not recognize on its own,
`read_sumstats` defaults to `sep="\t"`. Pass an explicit `sep=` in
`**read_kwargs` if your file is comma- or semicolon-separated.
```

```{tip}
**`read_sumstats` is for files of association results.** It does
**not** *compute* summary statistics from raw data — that is the job
of `igem.describe` (means, missingness, correlations from a
`Phenotypes` or `Genotypes`). The two modules complement each other:
`describe` produces stats, `data` reads stats produced elsewhere.
```

---

## 5. Format reference

A flat lookup of every reader and the formats it accepts:

| What you have                              | Reader                       | Returns           |
| ------------------------------------------ | ---------------------------- | ----------------- |
| `prefix.bed/.bim/.fam`                     | `read_plink(prefix)`         | `Genotypes`       |
| `cohort.vcf` / `.vcf.gz` / `.bcf`          | `read_vcf(path)`             | `Genotypes`       |
| `cohort.vcz` (Zarr / VCZ store)            | `read_zarr(path)`            | `Genotypes`       |
| `phen.csv` / `.tsv` / `.xpt` / DataFrame   | `read_phenotypes(source, …)` | `Phenotypes`      |
| `DEMO_J.XPT` (NHANES)                      | `read_nhanes_xpt(path, …)`   | `Phenotypes`      |
| Any TSV/CSV/Parquet annotation             | `read_table(path)`           | `pd.DataFrame`    |
| GWAS sumstats (PLINK 2 / REGENIE / BOLT / GWAS Catalog / custom) | `read_sumstats(path, preset=…)` | `pd.DataFrame` (canonical schema) |

---

## What's next

With data in hand, the natural next stops are:

- [Describing data](describing-data.md) — descriptive stats,
  type discovery, missingness, correlations, group-stratified
  summaries, and genotype QC.
- [Modifying data](modifying-data.md) — cleaning, transformations,
  outlier removal, type coercion, and feature filtering.
- [Reporting data](reporting-data.md) — reach the IGEM server to
  pull biological annotations that drive the
  [filter-then-test loop](../getting-started/concepts.md#the-filter-then-test-loop).
- [Cookbook](../cookbook/index.md) — end-to-end recipes that combine
  loading with description, modeling, and HPC submission.
