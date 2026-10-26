# Modifying data

The `modify` module applies pure transformations on `Phenotypes`
and `Genotypes` wrappers. It covers type harmonization (binary /
categorical / continuous coercion plus the CLARITE-style
classifier), value-level operations (log / log1p / sqrt /
rank-INT / Box-Cox / z-score / discretisation / value recoding /
outlier replacement), column and row filtering by structural
criteria (min-N, min-cell-count, percent-zero, NaN), multi-frame
assembly (vertical concatenation, horizontal merge, column
migration), and the standard genotype QC pipeline (biallelic /
call-rate / MAF / HWE / heterozygosity / SNV-only / region /
sample-list / variant-list / LD pruning). Every function returns
a new wrapper with role metadata and lazy backing stores intact —
the input is never mutated.

```{tip}
Every function in this module is **pure** — it returns a new
`Phenotypes` / `Genotypes` (or DataFrame for `auto_classify`) and
never mutates the input. Role metadata
(`outcomes` / `covariates` / `exposures` / survey design) and lazy
backing stores (sgkit / dask) are preserved across the pipeline.
```

```{note}
**No CLI commands for this module.** Modifications produce in-memory
wrappers consumed by the next analytical step within the same Python
session — there is no useful single-line terminal artifact, so a CLI
form would only ever say *"new wrapper has N samples"* and exit. The
`igem` CLI is reserved for operations that *do* produce one.
```

By the end of this page you will know how to:

- **Classify and harmonize variable types** — auto-detect kinds and
  coerce columns to binary / categorical / continuous.
- **Transform** continuous variables — log, rank-INT, Box-Cox,
  z-score, custom callables.
- **Filter columns** by name, by minimum N, by category balance, by
  excess zeros.
- **Filter rows / samples** — drop incomplete observations, keep or
  drop sample-IDs by list.
- **Combine frames** — vertical (multi-cohort), horizontal
  (multi-source), or migrate columns between frames.
- **Apply standard genotype QC** — call rate, MAF, HWE, biallelic,
  heterozygosity outliers, SNV-only.
- **Select variants** by ID, by genomic region.
- **Prune in linkage disequilibrium** — sliding-window r² pruning
  for SAIGE step 1, PCA, kinship, and independent-SNP analyses.

All examples below use the `IGEM` facade, consistent with the rest
of the user guide:

```python
from igem import IGEM

with IGEM() as igem:
    phen = igem.data.read_phenotypes("nhanes.csv", outcomes=["GLUCOSE"])
    cleaned = igem.modify.transform(phen, "BMI", method="log")
    cleaned = igem.modify.drop_missing(cleaned)
```

Free functions in `igem.modules.modify` remain importable when an
`IGEM` instance is not needed.

---

## 1. Type management

Statistical models treat continuous, binary, and categorical
variables very differently. `modify` exposes one report function
that classifies columns and three coercion functions that act on
the results.

### Auto-classification

`auto_classify` returns a per-column report (one row per non-id
column) labelling each variable's kind:

```python
igem.modify.auto_classify(phen)
#       column  n_unique     dtype         kind
# 0        AGE        47     int64    continuous
# 1        BMI        38   float64    continuous
# 2        SEX         2    object        binary
# 3      RACE6         5    object   categorical
# 4   ZIP_CODE      4123    object       unknown
```

Classification rule:

$$
\text{kind} =
\begin{cases}
\text{constant}    & \text{if } n_{\text{unique}} \leq 1 \\
\text{binary}      & \text{if } n_{\text{unique}} = 2 \\
\text{categorical} & \text{if } \text{cat\_min} \leq n_{\text{unique}} \leq \text{cat\_max} \\
\text{continuous}  & \text{if } n_{\text{unique}} \geq \text{cont\_min} \;\;\text{and column is numeric} \\
\text{unknown}     & \text{otherwise (gap region or high-cardinality non-numeric)}
\end{cases}
$$

Defaults `cat_min=3`, `cat_max=6`, `cont_min=15` follow CLARITE.
The function is a **pure report** — no dtype coercion happens.
Dispatch to `make_binary` / `make_categorical` / `make_continuous`
to actually act on the classification.

```{tip}
Use the report's `column` lists with the `only=` argument to drive
explicit coercion::

    report = igem.modify.auto_classify(phen)
    binary_cols     = report[report["kind"] == "binary"]["column"].tolist()
    continuous_cols = report[report["kind"] == "continuous"]["column"].tolist()
    phen = igem.modify.make_binary(phen, only=binary_cols)
    phen = igem.modify.make_continuous(phen, only=continuous_cols)
```

### Coerce to binary

`make_binary` validates that each target column has exactly 2
distinct non-NaN values, then coerces:

- numeric `{0, 1}` → nullable `Int64`
- everything else → `pandas.CategoricalDtype` with the 2 observed
  levels (sorted)

```python
phen = igem.modify.make_binary(phen, only=["CASE_CONTROL"])
```

A column with more or fewer than 2 unique values raises
`ValueError` — the function refuses to silently fabricate a binary
representation.

### Coerce to categorical / continuous

```python
phen = igem.modify.make_categorical(phen, only=["RACE6", "EDUCATION"])
phen = igem.modify.make_continuous(phen, only=["AGE", "BMI"])
```

`make_continuous` uses `pd.to_numeric(errors="raise")` — non-coercible
strings raise rather than silently becoming NaN. Clean the input
with `recode` first if mixed types are expected.

---

## 2. Value-level transformations

### Numeric transformations

`transform` applies an elementwise transformation to a numeric
column. It accepts either a **whitelisted method** (string) or a
**custom callable**:

```python
# Whitelist (95% of use cases):
phen = igem.modify.transform(phen, "BMI", method="log")
phen = igem.modify.transform(phen, "GLUCOSE", method="rank_int")

# Custom (research-mode):
phen = igem.modify.transform(phen, "BMI", func=lambda s: s ** 0.5)
```

Available `method` values:

| Method       | Operation                                              | Domain                              |
| ------------ | ------------------------------------------------------ | ----------------------------------- |
| `"log"`      | $\log(x)$                                              | $x > 0$ (NaN otherwise)             |
| `"log1p"`    | $\log(1 + x)$                                          | $x > -1$ (NaN otherwise)            |
| `"sqrt"`     | $\sqrt{x}$                                             | $x \geq 0$ (NaN otherwise)          |
| `"rank_int"` | Rank Inverse Normal Transform (see below)              | any (NaNs preserved)                |
| `"boxcox"`   | Box-Cox with auto-fit $\lambda$ (`scipy.stats.boxcox`) | $x > 0$ strictly (raises otherwise) |
| `"zscore"`   | $(x - \bar{x}) / \sigma$                               | any                                 |

**Rank Inverse Normal Transform (RINT)** maps any continuous
distribution to an approximately standard-normal distribution,
which is the prerequisite for many GxE / EWAS test statistics
(linear models, MELT, etc.):

$$
\text{RINT}(x_i) = \Phi^{-1}\!\left(\frac{r_i - 0.5}{n}\right)
$$

where $r_i$ is the average rank of $x_i$ among the $n$ non-NaN
observations, and $\Phi^{-1}$ is the standard-normal inverse CDF.
Ties receive the mean rank ([Beasley et al., 2009]).

**Box-Cox** finds the parameter $\lambda$ that maximises the
log-likelihood under a Gaussian assumption, applying:

$$
y(\lambda) =
\begin{cases}
\dfrac{x^\lambda - 1}{\lambda} & \text{if } \lambda \neq 0 \\
\log x                          & \text{if } \lambda = 0
\end{cases}
$$

Strictly positive input is required — for inputs with zeros, shift
by a small constant first or switch to `log1p`.

`replace=True` overwrites the source column; the default writes to
`f"{col}_<method>"` (or `f"{col}_transformed"` for callable `func`).

[Beasley et al., 2009]: https://link.springer.com/article/10.1007/s10519-009-9281-0

### Discretisation

`discretize` bins a continuous column into ordered categories — the
inverse direction of "make continuous". Two modes:

```python
# Equal-count bins (quantiles):
phen = igem.modify.discretize(phen, "AGE", method="quantiles", n_bins=4)

# Custom edges (clinical thresholds for BMI):
phen = igem.modify.discretize(
    phen, "BMI",
    method="bins",
    bin_edges=[0, 18.5, 25, 30, 100],
    labels=["underweight", "normal", "overweight", "obese"],
)
```

```{note}
**Naming choice.** This function is `discretize`, not `categorize`,
to avoid the collision with `clarite.modify.categorize` (which
*classifies* dtypes, not bins values). For the CLARITE-equivalent
classifier, see :func:`auto_classify`.
```

### Value mapping

`recode` applies a value-substitution mapping to a column with
optional missing-value handling:

```python
# Map sentinel values to labels, with 99 → NaN:
phen = igem.modify.recode(
    phen, "SMOKING_STATUS",
    mapping={1: "never", 2: "former", 3: "current"},
    missing_values=[7, 9, 99],
)
```

Values not in `mapping` and not in `missing_values` are left
untouched. Default writes in place (`replace=True`) — set
`new_col=` and `replace=False` to keep the source column.

### Outlier replacement

`remove_outliers` flags outliers in numeric columns and **replaces
them with NaN** (does not drop the rows — pair with `drop_missing`
if you need that). Two detection methods:

```python
# Tukey 1.5 × IQR (default):
phen = igem.modify.remove_outliers(phen, cols=["LDL", "TRIG"])

# 3-σ Gaussian:
phen = igem.modify.remove_outliers(
    phen, cols=["LDL"], method="gaussian", cutoff=3.0,
)
```

**IQR rule** (Tukey, 1977):

$$
x_i \text{ is outlier} \iff
x_i < Q_1 - k \cdot \text{IQR}
\;\;\text{or}\;\;
x_i > Q_3 + k \cdot \text{IQR},
\quad \text{IQR} = Q_3 - Q_1
$$

Default $k = 1.5$ marks "outliers"; setting $k = 3$ identifies only
"extreme outliers" by Tukey's nomenclature.

**Gaussian rule**:

$$
x_i \text{ is outlier} \iff \frac{|x_i - \bar{x}|}{\sigma} > \text{cutoff}
$$

Default `cutoff = 3.0` corresponds to the conventional 3-σ rule
(roughly 0.27% expected false-positive rate under exact normality).

```{tip}
This pairs with :func:`igem.describe.summarize` — the `n_outliers`
column in the summary is computed with the same Tukey rule and
flags which columns are worth running through `remove_outliers`.
```

---

## 3. Column filtering

Four functions to drop columns by structural criteria. All accept
`only=` (restrict scope to listed columns) and `skip=` (exclude
listed columns). Role metadata is filtered automatically — dropped
columns are also removed from `outcomes` / `covariates` /
`exposures` / survey-design slots.

### By explicit name

```python
# Keep only listed columns (sample_id is always preserved):
phen = igem.modify.colfilter(phen, only=["BMI", "GLUCOSE", "AGE", "SEX"])

# Drop a few:
phen = igem.modify.colfilter(phen, skip=["INTERNAL_NOTE", "RAW_TIMESTAMP"])
```

### By minimum N (sparseness)

`colfilter_min_n` drops any column with fewer than `n` non-NaN
values:

```python
# CLARITE convention for EWAS / PheWAS:
phen = igem.modify.colfilter_min_n(phen, n=200)
```

In multiple-testing settings, sparse variables inflate the type-I
error rate at the tail of the distribution; CLARITE's default of
$n = 200$ is a reasonable conservative threshold for population
studies (Hall et al., 2014).

### By minimum cell count (rare categories)

`colfilter_min_cat_n` drops binary / categorical columns where any
**level** has fewer than `n` occurrences. A column is treated as
binary or categorical when it is a `pandas.CategoricalDtype` **or**
has between 2 and `cat_max` unique non-NaN values (heuristic
consistent with `auto_classify`):

```python
phen = igem.modify.colfilter_min_cat_n(phen, n=50)
```

This is the right pre-filter before interaction tests — a
genotype × exposure cell with $N < 5$ has essentially no power
([Wong et al., 2003]) and adding such variables to a PheWAS sweep
mostly adds noise.

[Wong et al., 2003]: https://doi.org/10.1093/aje/kwg074

### By excess zeros

`colfilter_percent_zero` drops continuous columns dominated by
zeros:

```python
phen = igem.modify.colfilter_percent_zero(phen, max_zero_pct=90.0)
```

The default $90\%$ matches CLARITE. Useful for occupational-exposure
ratings, physical-activity counters, and other measurements where a
feature dominated by zeros carries little discriminative signal for
modeling.

---

## 4. Row filtering and sample selection

### Drop incomplete observations

Two functions, same idea, slightly different default scopes:

```python
# Default: drop rows with NaN in any role column
# (sample_id + outcomes + covariates + exposures + survey).
phen = igem.modify.drop_missing(phen)

# CLARITE-style: scan every column by default.
phen = igem.modify.rowfilter_incomplete_obs(phen)

# Either accepts only=/skip= for explicit scope.
phen = igem.modify.drop_missing(phen, cols=["BMI", "GLUCOSE"])
```

### Filter samples by ID list

`filter_samples` works on either `Phenotypes` or `Genotypes`:

```python
# Drop samples with withdrawn consent:
phen = igem.modify.filter_samples(phen, withdrawn_ids, keep=False)
geno = igem.modify.filter_samples(geno, withdrawn_ids, keep=False)

# Restrict cohort to a subset:
phen = igem.modify.filter_samples(phen, cohort_ids, keep=True)
```

Sample-IDs not present in the wrapper are ignored silently. Role
metadata and lazy backing (sgkit/dask graphs for genotypes) are
preserved.

---

## 5. Multi-frame assembly

### Stack samples (multi-cohort)

`merge_observations` concatenates two phenotype frames vertically,
keeping only columns present in both. Sample-ID overlap is rejected
to prevent silent duplication:

```python
# Combine NHANES cycles or biobank chunks:
combined = igem.modify.merge_observations(nhanes_2017, nhanes_2018)
```

Roles are unioned across the inputs and filtered to the intersection
of columns. Pre-deduplicate sample-IDs externally if the two frames
genuinely cover overlapping individuals.

### Stack variables (multi-source)

`merge_variables` joins two frames horizontally on `sample_id_col`:

```python
# Add lab measurements to a questionnaire frame:
phen = igem.modify.merge_variables(questionnaire, biomarkers, how="outer")
```

Default `how="outer"` keeps all sample-IDs (unmatched cells are
filled with NaN). Other modes follow `pandas.merge` semantics:
`"inner"` (intersection), `"left"`, `"right"`. Overlapping non-id
columns receive `_x`/`_y` suffixes — clean those up with `colfilter`
afterwards.

### Migrate columns between frames

`move_variables` extracts columns from a source frame and appends
them to a destination frame, returning both:

```python
new_quest, new_bio = igem.modify.move_variables(
    questionnaire, biomarkers,
    only=["GLUCOSE", "INSULIN"],
)
```

Both frames must share the same `sample_id_col` and have matching
sample-ID order — call `filter_samples` on both first if needed.
The `sample_id_col` itself is never moved.

---

## 6. Genotype QC

The standard GWAS QC pipeline ([Anderson et al., 2010]) uses six
filters in sequence. `modify` exposes them as composable wrappers
over sgkit's lazy operations.

[Anderson et al., 2010]: https://doi.org/10.1038/nprot.2010.116

### Allele structure: biallelic only

```python
geno = igem.modify.filter_biallelic(geno)
```

Drops variants with more than two distinct non-empty alleles —
prerequisite for tools that assume biallelic variants (GLM, REGENIE
step 1, classical HWE test). No-op when the dataset's `alleles`
dimension is already size 2.

### SNVs only (drop indels)

```python
geno = igem.modify.keep_snvs_only(geno)
```

Keeps only single-nucleotide variants (every non-empty allele has
length 1). Drops insertions, deletions, and multi-base substitutions.
Common pre-step in GWAS pipelines that use SNV-only LD reference
panels.

### Sample-level call rate

```python
geno = igem.modify.filter_missingness_samples(geno, max_missing=0.05)
```

Drops samples whose fraction of missing calls across variants
exceeds `max_missing`. Equivalent constraint:

$$
\text{call rate}_i = \frac{n_{\text{called},\,i}}{N_{\text{variants}}} \geq 1 - \text{max\_missing}
$$

Conventional cut-off in GWAS: $\text{call rate} \geq 0.95$ to $0.97$
(cohort-dependent).

### Variant-level call rate

```python
geno = igem.modify.filter_missingness_variants(geno, max_missing=0.05)
```

Same definition applied per variant across samples. Run after the
sample-level filter, since a few low-quality samples can otherwise
pull many variants below threshold.

### Minor allele frequency (MAF)

```python
geno = igem.modify.filter_maf(geno, min_maf=0.01)
```

Drops variants below the MAF threshold. For a biallelic variant
with reference-allele frequency $p$:

$$
\text{MAF} = \min(p, 1 - p), \qquad
p = \frac{2\,n_{\text{hom\_ref}} + n_{\text{het}}}{2\,n_{\text{called}}}
$$

Common cut-offs: $0.01$ (common variants), $0.05$ (very common), or
$0.001$ (rare-variant studies with sufficient power).

### Hardy-Weinberg equilibrium

```python
geno = igem.modify.filter_hwe(geno, min_pvalue=1e-6)
```

Drops variants whose HWE exact-test p-value is below the threshold
([Wigginton et al., 2005]). Strong departure from HWE in controls
is a classic genotyping-error signal. Conventional thresholds:
$10^{-6}$ (loose) to $10^{-3}$ (stricter, controls-only).

[Wigginton et al., 2005]: https://doi.org/10.1086/429864

### Heterozygosity outliers

`filter_heterozygosity_outliers` closes the standard QC loop with
:func:`igem.describe.heterozygosity`: that function reports per-sample
het rate and z-score; this one drops the flagged outliers:

```python
geno = igem.modify.filter_heterozygosity_outliers(geno, outlier_sd=3.0)
```

For each sample $i$, $H_i = n_{\text{het},\,i} / n_{\text{called},\,i}$,
and:

$$
z_i = \frac{H_i - \bar{H}}{s_H}, \qquad
\text{flag if } |z_i| > \text{outlier\_sd}
$$

Default `outlier_sd = 3` matches PLINK convention. Extreme high
heterozygosity flags possible sample contamination; extreme low
flags inbreeding or cryptic relatedness.

```{tip}
**Recommended QC sequence**::

    geno = igem.modify.filter_biallelic(geno)
    geno = igem.modify.keep_snvs_only(geno)
    geno = igem.modify.filter_missingness_samples(geno, max_missing=0.03)
    geno = igem.modify.filter_heterozygosity_outliers(geno)
    geno = igem.modify.filter_missingness_variants(geno, max_missing=0.02)
    geno = igem.modify.filter_maf(geno, min_maf=0.01)
    geno = igem.modify.filter_hwe(geno, min_pvalue=1e-6)
```

---

## 7. Variant selection

### By ID list

```python
# Custom panel from a gene-set or pathway:
geno = igem.modify.filter_variants(geno, panel_rsids, keep=True)

# Blacklist: drop flagged probes from a manufacturer QC report:
geno = igem.modify.filter_variants(geno, blacklist_rsids, keep=False)
```

IDs not in the dataset are ignored silently.

### By genomic region

```python
# Restrict to one contig:
geno = igem.modify.filter_by_region(geno, "1")

# Locus-specific fine-mapping:
geno = igem.modify.filter_by_region(
    geno, "19", start=44_900_000, end=45_000_000,  # APOE region
)

# Drop chrX:
auto = igem.modify.filter_by_region(geno, "1")  # then 2..22 sequentially
# (or: combine variant masks externally for multi-chrom selection)
```

`start` and `end` are inclusive; either can be `None` to bound only
on one side.

---

## 8. Linkage disequilibrium pruning

`prune_ld` produces a near-LD-independent variant subset using
greedy sliding-window pruning — the prerequisite for SAIGE step 1
(null-model fitting), kinship estimation, PCA, and any analysis
that assumes independent SNPs.

```python
# SAIGE-canonical defaults (variants window):
geno_pruned = igem.modify.prune_ld(geno, window=50, step=5, r2=0.5)

# Position-based (PLINK --indep-pairwise-kb style):
geno_pruned = igem.modify.prune_ld(
    geno, window=1000, step=50, r2=0.05, unit="kb",
)
```

The algorithm slides a window of size `window` (in variants or kb)
across the genome with stride `step`, computes pairwise $r^2$ within
each window, and drops one of any pair whose $r^2$ exceeds the
threshold. This is the same procedure as
PLINK `--indep-pairwise window step r2` ([Purcell et al., 2007])
and the prep step recommended in the SAIGE
analytic guide.

The squared correlation between two variants $X_a, X_b$ (treated as
diploid dosages $\in \{0, 1, 2\}$) is:

$$
r^2_{ab} = \frac{\big(\sum_i (x_{ai} - \bar{x}_a)(x_{bi} - \bar{x}_b)\big)^2}
                {\sum_i (x_{ai} - \bar{x}_a)^2 \;\sum_i (x_{bi} - \bar{x}_b)^2}
$$

Defaults `(window=50, step=5, r2=0.5)` reproduce PLINK
`--indep-pairwise 50 5 0.5`. For more aggressive pruning before
PCA, use `(1000, 50, 0.05)` with `unit="kb"`.

[Purcell et al., 2007]: https://doi.org/10.1086/519795

```{note}
Implementation wraps :func:`sgkit.ld_prune`. If `call_dosage` is not
already on the dataset, it is computed from `call_genotype` by
summing along the `ploidy` axis (NaN where any ploidy slot is
missing). The dataset must be biallelic — pair with
:func:`filter_biallelic` first when working with multi-allelic
input.
```

---

## 9. Quick reference

### Phenotype helpers

| Function                                       | Purpose                                                    |
| ---------------------------------------------- | ---------------------------------------------------------- |
| `igem.modify.auto_classify(phen)`              | Per-column type classification (report, no coercion)       |
| `igem.modify.make_binary(phen, only=…)`        | Coerce binary columns                                      |
| `igem.modify.make_categorical(phen, only=…)`   | Coerce to `pandas.CategoricalDtype`                        |
| `igem.modify.make_continuous(phen, only=…)`    | Coerce to numeric                                          |
| `igem.modify.transform(phen, col, method=…)`   | log / log1p / sqrt / rank_int / boxcox / zscore / callable |
| `igem.modify.discretize(phen, col, method=…)`  | Binning into ordered categories                            |
| `igem.modify.recode(phen, col, mapping)`       | Value mapping with missing-sentinel handling               |
| `igem.modify.remove_outliers(phen, …)`         | IQR or Gaussian outlier → NaN                              |
| `igem.modify.colfilter(phen, …)`               | Keep / drop columns by name                                |
| `igem.modify.colfilter_min_n(phen, n=200)`     | Drop columns with too few non-NaN                          |
| `igem.modify.colfilter_min_cat_n(phen, n=200)` | Drop categories with rare levels                           |
| `igem.modify.colfilter_percent_zero(phen, …)`  | Drop continuous columns dominated by zeros                 |
| `igem.modify.drop_missing(phen)`               | Drop rows with NaN in role columns                         |
| `igem.modify.rowfilter_incomplete_obs(phen)`   | Drop rows with NaN anywhere                                |
| `igem.modify.merge_observations(top, bottom)`  | Vertical concat (intersection of columns)                  |
| `igem.modify.merge_variables(left, right)`     | Horizontal merge by sample_id                              |
| `igem.modify.move_variables(src, dst, …)`      | Migrate columns between frames                             |

### Genotype helpers

| Function                                           | Purpose                                       |
| -------------------------------------------------- | --------------------------------------------- |
| `igem.modify.filter_biallelic(geno)`               | Keep biallelic variants only                  |
| `igem.modify.keep_snvs_only(geno)`                 | Drop indels and multi-base substitutions      |
| `igem.modify.filter_missingness_samples(geno, …)`  | Drop low-call-rate samples                    |
| `igem.modify.filter_missingness_variants(geno, …)` | Drop low-call-rate variants                   |
| `igem.modify.filter_maf(geno, min_maf=0.01)`       | Minor allele frequency threshold              |
| `igem.modify.filter_hwe(geno, min_pvalue=1e-6)`    | Hardy-Weinberg equilibrium p-value threshold  |
| `igem.modify.filter_heterozygosity_outliers(geno)` | Drop het-rate outliers (PLINK 3-σ convention) |
| `igem.modify.filter_variants(geno, ids)`           | Keep / drop by variant_id list                |
| `igem.modify.filter_by_region(geno, chrom, …)`     | Keep variants in a genomic region             |
| `igem.modify.prune_ld(geno, …)`                    | Sliding-window LD pruning (SAIGE-canonical)   |

### Cross-cutting

| Function                                       | Purpose                                         |
| ---------------------------------------------- | ----------------------------------------------- |
| `igem.modify.filter_samples(obj, ids, keep=…)` | Filter `Genotypes` or `Phenotypes` by sample-ID |

---

## Related pages

- [Describing data](describing-data.md) — read-only descriptive
  statistics that pair naturally with `modify`'s outlier replacement,
  near-zero-variance filtering, and missingness reports.
- [Analyzing data](analyzing-data.md) — regression-based inference:
  `association_study`, `interaction_study`, multiple-testing
  correction.
- [Reporting data](reporting-data.md) — biological annotation of
  variables and variants from the IGEM server.
- [Cookbook](../cookbook/index.md) — end-to-end recipes that
  combine multiple modules.
