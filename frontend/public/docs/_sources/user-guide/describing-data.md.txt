# Describing data

The `describe` module computes read-only descriptive statistics on
`Phenotypes` and `Genotypes` wrappers. Per-column summaries (mean,
SD, missingness, skewness, near-zero variance, Tukey outliers),
pairwise correlations (full matrix or pairs filtered by threshold),
two-way contingency tables for joint distributions, group-stratified
summaries, and the standard genotype QC quantities (call rate, minor
allele frequency, Hardy-Weinberg equilibrium, heterozygosity
outliers) all return new pandas DataFrames or dicts. The input
wrapper is never mutated — the output **is** the value, ready to
flow into a notebook display, a plot, or a report header.

```{tip}
Everything on this page is **local** and **read-only**. Functions
return new pandas DataFrames or dicts — they never mutate the input
`Phenotypes` / `Genotypes` wrapper. No server calls, no file writes.
```

```{note}
**No CLI commands for this module.** Each call returns an in-memory
DataFrame / dict that flows into the next step (model, plot, report).
There is no useful single-line terminal artifact, so a CLI form would
just print a table and exit. Use Python or a notebook.
```

By the end of this page you will know how to:

- Compute **per-column** summary statistics on phenotypes (numeric and
  categorical), with optional **survey weighting**.
- Get a **dataset-level** overview — variable type counts, missingness,
  role counts, survey-design status.
- Identify **missing-data patterns**, **outliers**, **near-zero variance**
  variables, and **skewness**.
- Compute pairwise **correlations** (full matrix or pairs above a
  threshold) and **2-way contingency tables** for joint distributions.
- Run **group-stratified** summaries (e.g., AGE by SEX, BMI by exposure
  level).
- Compute genotype **QC statistics** — call rate, MAF, HWE,
  heterozygosity outliers — using sgkit underneath.

All examples below use the `IGEM` facade — same pattern as
[Loading data](loading-data.md):

```python
from igem import IGEM

with IGEM() as igem:
    phen = igem.data.read_phenotypes("nhanes.csv", outcomes=["GLUCOSE"])
    stats = igem.describe.summarize(phen)
```

Phenotype methods are quiet (the output **is** the value — verbose
logs would be noise during EDA). Genotype methods log a header /
footer because their sgkit operations can be expensive on
biobank-scale inputs.

The same calls are also available as plain free functions when an
`IGEM` instance is not needed:

```python
from igem.modules.describe import summarize
stats = summarize(phen)
```

The two forms are interchangeable; the rest of this page uses the
facade form.

---

## 1. Per-column summary

`summarize` returns one row per column with the union of statistics
that apply to its kind. It is the workhorse of phenotype description.

```python
stats = igem.describe.summarize(phen)
stats[["column", "kind", "n", "n_missing", "mean", "std", "median", "near_zero_var", "n_outliers"]]
#   column        kind  n  n_missing   mean    std  median  near_zero_var  n_outliers
# 0    AGE  continuous  5          0  35.00   7.91  35.000          False         0.0
# 1    BMI  continuous  5          1  26.38   4.11  26.250          False         0.0
# 2    SEX      binary  5          0    NaN    NaN     NaN          False         NaN
```

Output columns:

| Column          | Always present? | Notes                                       |
| --------------- | --------------- | ------------------------------------------- |
| `column`        | yes             | Source column name                          |
| `dtype`         | yes             | pandas dtype string                         |
| `kind`          | yes             | `continuous`, `binary`, or `categorical`    |
| `n`, `n_missing`, `missing_pct` | yes | Always raw counts (never weighted)   |
| `n_unique`      | yes             | NaN excluded                                |
| `mean`, `std`, `min`, `q25`, `median`, `q75`, `max` | numeric only |              |
| `mode`, `mode_count` | non-numeric only |                                       |
| `near_zero_var` | yes             | True for constants and low-CV continuous (see formula below) |
| `n_outliers`    | numeric only    | Tukey 1.5×IQR fence (see formula below); NaN for non-numeric |

The `sample_id` column is excluded by default; pass it explicitly to
`cols=` if you want it summarised.

**Outlier criterion** — `n_outliers` counts observations outside the
Tukey fence ([Tukey, 1977]):

$$
\text{outlier} \iff x < Q_1 - 1.5 \cdot \text{IQR} \;\;\text{or}\;\; x > Q_3 + 1.5 \cdot \text{IQR}, \quad \text{IQR} = Q_3 - Q_1
$$

The fence requires at least 4 valid observations and a positive IQR;
otherwise `n_outliers` is `NaN` or `0`.

**Near-zero variance** — `near_zero_var` is `True` when the column
is a constant or when its **coefficient of variation** is below a
small threshold:

$$
\text{near\_zero\_var} \iff n_{\text{unique}} \leq 1 \;\;\text{or}\;\; \frac{\sigma}{|\mu|} < 10^{-3}
\quad (\mu \neq 0)
$$

Useful as an early filter for ML pipelines — features with
essentially no variance carry no information and can destabilise
model fitting.

[Tukey, 1977]: https://www.worldcat.org/title/exploratory-data-analysis/oclc/3058187

### Type classification

`kind` is decided by the column shape, not by dtype alone:

- `binary` — exactly 2 distinct non-NaN values, regardless of dtype
  (covers `{0, 1}` ints, `{True, False}` bools, `{"yes", "no"}` strs).
- `continuous` — any other numeric (non-bool) column.
- `categorical` — anything else.

This matters downstream: binary outcomes go to logistic models,
continuous to linear, categorical may need dummy encoding.

### Survey-weighted statistics

For survey data (e.g., NHANES), pass `weighted=True`. The weighted
mean and variance follow the standard survey-statistics definitions
([NHANES Analytic Guidelines]):

$$
\bar{x}_w = \frac{\sum_i w_i x_i}{\sum_i w_i}, \qquad
\sigma^2_w = \frac{\sum_i w_i (x_i - \bar{x}_w)^2}{\sum_i w_i - 1}
$$

Weighted quantiles use the inverse of the weighted ECDF. All three
are computed via `statsmodels.stats.weightstats.DescrStatsW` with
the `weights_col` declared at load time:

[NHANES Analytic Guidelines]: https://wwwn.cdc.gov/nchs/nhanes/tutorials/default.aspx

```python
phen = igem.data.read_phenotypes(
    "nhanes.csv",
    outcomes=["GLUCOSE"],
    weights_col="WTMEC",
)

stats_unweighted = igem.describe.summarize(phen)
stats_weighted   = igem.describe.summarize(phen, weighted=True)
```

Counts (`n`, `n_missing`) stay raw — survey-statistics convention is
to report unweighted Ns alongside weighted distributional stats. If
`weighted=True` and `weights_col` is not set, `summarize` raises
`ValueError`.

---

## 2. Dataset-level overview

`dataset_summary` collapses `summarize` into a single dict — useful
for one-line audit prints and for report headers:

```python
igem.describe.dataset_summary(phen)
# {
#   "n_samples": 4,
#   "n_columns": 6,
#   "n_continuous": 3, "n_binary": 1, "n_categorical": 2,
#   "n_with_missing": 0, "total_missing_pct": 0.0,
#   "n_outcomes": 1, "n_covariates": 2, "n_exposures": 1,
#   "has_survey_design": True,
# }
```

`has_survey_design` is True when any of `weights_col`, `strata_col`,
`cluster_col` was set at load time.

`total_missing_pct` is the dataset-wide missing rate over all
non-identifier cells:

$$
\text{total\_missing\_pct} = 100 \cdot \frac{\sum_c n_{\text{missing},\,c}}{\sum_c n_c}
$$

where the sum runs over all summarised columns $c$.

---

## 3. Missingness

`missing_report` extracts the missing-data view, sorted by `missing_pct`
(percentage of rows where the column is NaN — `100 × n_missing /
n_samples`) descending so the worst offenders surface first:

```python
igem.describe.missing_report(phen)
#       column    dtype  n_missing  missing_pct
# 0        BMI  float64          1         20.0
# 1        AGE    int64          0          0.0
# 2        SEX   object          0          0.0
```

Unlike `summarize`, `missing_report` includes the `sample_id` column
by default (since identifier columns can have unintended missingness).

---

## 4. Correlations

### Full matrix

`correlation_matrix` computes the pairwise correlation matrix between
all numeric (non-bool, non-sample-id) columns:

```python
igem.describe.correlation_matrix(phen)                       # Pearson (default)
#         AGE   BMI   LDL
# AGE    1.00  0.85 -0.12
# BMI    0.85  1.00 -0.05
# LDL   -0.12 -0.05  1.00
```

The diagonal is always `1.0` (a column correlates perfectly with
itself) and the matrix is symmetric (`r(AGE, BMI) == r(BMI, AGE)`).

**Pearson** (default) measures the strength of a linear relationship
between two numeric variables:

$$
r_{XY} = \frac{\sum_i (x_i - \bar{x})(y_i - \bar{y})}{\sqrt{\sum_i (x_i - \bar{x})^2}\,\sqrt{\sum_i (y_i - \bar{y})^2}}
$$

It assumes approximate linearity and is sensitive to outliers and
non-normal marginals. Switch to a rank-based method when those
assumptions are violated:

```python
igem.describe.correlation_matrix(phen, method="spearman")    # rank-based
igem.describe.correlation_matrix(phen, method="kendall")     # ordinal concordance
```

**Spearman's $\rho$** is Pearson's $r$ applied to the ranks of $X$
and $Y$ — it captures any monotonic association, not only linear,
and is robust to outliers ([Spearman, 1904]).

**Kendall's $\tau$** measures concordance between observation pairs:

$$
\tau = \frac{C - D}{\binom{n}{2}}
$$

where $C$ is the number of concordant pairs (both variables move in
the same direction) and $D$ the discordant pairs. $\tau$ has a
direct probabilistic interpretation and handles ties more naturally
than $\rho$ ([Kendall, 1938]).

Passing a non-numeric column explicitly via `cols=` raises `ValueError`.

[Spearman, 1904]: https://doi.org/10.2307/1412159
[Kendall, 1938]: https://doi.org/10.1093/biomet/30.1-2.81

### Pairs above threshold

For long-format output filtered by correlation strength (more
ergonomic than a matrix when feeding into pipelines):

```python
igem.describe.correlation_pairs(phen, threshold=0.7)
#   var1  var2     r
# 0  AGE   BMI  0.85
# 1  AGE   LDL -0.78
```

`absolute=True` (default) keeps anti-correlated pairs. Set to `False`
to keep only `r >= threshold`. Output is sorted by `|r|` descending.

```{tip}
Use `correlation_pairs` when you want to **act** on correlations
(remove redundant features, populate a report). Use `correlation_matrix`
when you want to **see** them (notebook display, heatmap).
```

---

## 5. Frequency tables

### Single-column counts

`value_counts` returns a frequency table per column with `value`,
`count`, and `pct`:

```python
igem.describe.value_counts(phen, cols=["SEX"])
# {"SEX":
#    value  count  pct
#  0     M      3 60.0
#  1     F      2 40.0
# }
```

The percentage is the count over the denominator chosen by `dropna`:

$$
\text{pct} = 100 \cdot \frac{\text{count}}{n_d}, \qquad
n_d = \begin{cases}
  n & \text{if } \texttt{dropna=False} \\
  n - n_{\text{missing}} & \text{if } \texttt{dropna=True}
\end{cases}
$$

Use `top=` to cap rows per column and `dropna=` to control whether
NaN appears as its own bucket and how the denominator is computed.

### Two-way contingency

`crosstab` produces a 2-way table — essential for **GxE / GxG joint
distributions** and **rare-cell detection** before interaction tests:

```python
igem.describe.crosstab(phen, "GENO", "EXPOSED")
# EXPOSED   0  1
# GENO
# 0         2  1
# 1         1  2
# 2         1  2
```

Optional flags follow `pandas.crosstab` conventions:

| Flag                     | Effect                                          |
| ------------------------ | ----------------------------------------------- |
| `normalize="index"`      | Row-normalize (each row sums to 1)              |
| `normalize="columns"`    | Column-normalize                                |
| `normalize="all"`        | Grand-total normalize                           |
| `margins=True`           | Add an `All` row and column with totals         |

For rare-cell detection, just compare the count crosstab to a
threshold:

```python
counts = igem.describe.crosstab(phen, "GENO", "EXPOSED")
rare = counts < 5     # cells with N < 5 may be underpowered
```

---

## 6. Skewness

For continuous variables, `skewness` returns the **third
standardized moment** plus a z-score / p-value test of the null
"skew is zero" — useful before deciding whether to log-transform,
rank-INT, or leave as-is:

$$
\gamma_1 = \frac{\mathbb{E}\!\left[(X - \mu)^3\right]}{\sigma^3}
\qquad
\begin{cases}
\gamma_1 > 0 & \text{right-skewed (long upper tail)} \\
\gamma_1 = 0 & \text{symmetric} \\
\gamma_1 < 0 & \text{left-skewed (long lower tail)}
\end{cases}
$$

```python
igem.describe.skewness(phen)
#   column   n  skew  zscore   pvalue
# 0    AGE  10  0.00    0.00  1.0e+00
# 1    BMI   8  1.85    3.21  1.3e-03   # right-skewed
```

The z-score / p-value follow the [D'Agostino, 1970] transformation
of the sample skewness to an approximately standard-normal statistic
under the null of normality, as implemented in `scipy.stats.skewtest`.
The test requires at least 8 valid observations; below that, `zscore`
and `pvalue` are `NaN` but `skew` is still computed.

A common rule of thumb (Bulmer, 1979): $|\gamma_1| < 0.5$ approximately
symmetric, $0.5 \leq |\gamma_1| < 1$ moderately skewed,
$|\gamma_1| \geq 1$ highly skewed — strong candidate for
transformation.

`dropna=False` (default) propagates NaN — a column with any missing
value gets all-NaN stats. Pass `dropna=True` to drop missing
observations first.

[D'Agostino, 1970]: https://doi.org/10.1093/biomet/57.3.679

---

## 7. Group-stratified summaries

`summarize_by` is `summarize` applied per group — one row per
`(group_value, column)` combination, in long format:

```python
igem.describe.summarize_by(phen, by="SEX")
#   SEX  column  ...  mean
# 0   M     AGE  ...  35.5
# 1   M     BMI  ...  28.0
# 2   F     AGE  ...  37.5
# 3   F     BMI  ...  24.0
```

The `by` column is excluded from the per-group target columns (its
summary inside its own group is trivial). With `dropna_group=True`
(default), rows where the group value is NaN are skipped.

To pivot to wide format with one row per column and group-suffixed
stat columns:

```python
long = igem.describe.summarize_by(phen, by="SEX")
wide = long.set_index(["SEX", "column"]).unstack("SEX")
```

```{tip}
Use this for **EWAS / PheWAS exploration** (compare exposure groups
on outcome) and **GxE preliminary checks** (does the exposure
distribution differ by genotype?). For group-comparison **inference**
(p-values, confidence intervals on mean differences), see `analyze`.
```

---

## 8. Genotype QC

The genotype side of `describe` wraps sgkit's variant- and
sample-level statistics into pandas DataFrames suitable for QC
filtering and visualization. These methods log a header / footer
because the underlying sgkit ops can be expensive on biobank-scale
inputs.

The standard GWAS QC pipeline ([Anderson et al., 2010]) is built
on the four metrics computed in this section: **call rate** (per
sample and per variant), **minor allele frequency**, **Hardy-Weinberg
equilibrium**, and **heterozygosity**.

[Anderson et al., 2010]: https://doi.org/10.1038/nprot.2010.116

### Per-variant stats

```python
igem.describe.variant_stats(geno).head()
# [describe] variant_stats over 1_103_547 variants
#   variant_id contig  position  n_called  n_het  n_hom_ref  n_hom_alt  call_rate   maf  hwe_pvalue
# 0      rs001      1     12345      2502      0       1252       1250       1.00  0.50    0.92
# 1      rs002      1     12678      2480     12       2460          8       0.99  0.005   0.81
# [describe] variant_stats: 1_103_547 rows, 10 columns
```

**Call rate** is the fraction of samples with a non-missing call at
variant $v$:

$$
\text{call rate}_v = \frac{n_{\text{called},\,v}}{N_{\text{samples}}}
$$

**Minor allele frequency (MAF)** is the relative frequency of the
less common allele. For a biallelic variant with reference allele
frequency $p$ and alternate allele frequency $q = 1 - p$:

$$
\text{MAF}_v = \min(p, 1 - p), \qquad p = \frac{2\,n_{\text{hom\_ref}} + n_{\text{het}}}{2\,n_{\text{called}}}
$$

For a multiallelic site, MAF is the minimum across all observed
allele frequencies, and the column reflects sgkit's
`variant_allele_frequency` reduction.

**Hardy-Weinberg equilibrium (HWE)** tests the null
$H_0: P(\text{AA}) = p^2,\; P(\text{Aa}) = 2pq,\; P(\text{aa}) = q^2$
against the observed genotype counts ([Wigginton et al., 2005]).
The implementation in `sgkit.hardy_weinberg_test` uses the
mid-p exact test for biallelic variants. HWE is attempted in a
`try/except`; small or non-biallelic inputs leave `hwe_pvalue`
absent rather than failing the call. Strong departure from HWE in
controls is a classic genotyping-error signal; common cut-offs are
$p_{\text{HWE}} < 10^{-6}$ in cases (loose) or $< 10^{-3}$ in
controls (stricter).

[Wigginton et al., 2005]: https://doi.org/10.1086/429864

### Per-sample stats

```python
igem.describe.sample_stats(geno).head()
#   sample_id  n_called  n_het  n_hom_ref  n_hom_alt  call_rate
# 0     S0001    1103412  410123    420112     273177     0.9998
```

The same call-rate definition applied across the variants axis:

$$
\text{call rate}_i = \frac{n_{\text{called},\,i}}{N_{\text{variants}}}
$$

Samples with low call rate are typically dropped first in QC
(common cut-off: $< 0.95$ or $< 0.97$, cohort-dependent), since
their reduced data contributes noise to subsequent variant-level
QC steps.

### Heterozygosity outliers

`heterozygosity` is the standard QC step downstream of call rate.
For each sample $i$, the **observed heterozygosity rate** is

$$
H_i = \frac{n_{\text{het},\,i}}{n_{\text{called},\,i}}
$$

and the **z-score** measures how far the sample sits from the
cohort mean in units of cohort standard deviation:

$$
z_i = \frac{H_i - \bar{H}}{s_H}, \qquad
\bar{H} = \frac{1}{N}\sum_{j} H_j, \quad
s_H^2 = \frac{1}{N - 1}\sum_{j} (H_j - \bar{H})^2
$$

`is_outlier=True` when $|z_i|$ exceeds `outlier_sd` (default `3.0`,
matching the PLINK convention — extreme heterozygosity beyond
3 SD is a strong signal of sample contamination on the high side
or inbreeding / cryptic relatedness on the low side).

```python
het = igem.describe.heterozygosity(geno)
outliers = het[het["is_outlier"]]
outliers[["sample_id", "het_rate", "het_zscore"]]
#       sample_id  het_rate  het_zscore
# 142       S0142     0.412        4.21    # high → possible contamination
# 877       S0877     0.180       -3.85    # low  → possible inbreeding / substructure
```

```{tip}
Standard GWAS QC sequence: filter low-call-rate samples first
(`sample_stats`), then drop heterozygosity outliers, then filter
variants by call rate / MAF / HWE (`variant_stats`).
```

### Aggregated overview

`genotype_summary` is the dict equivalent of `dataset_summary` for
genotypes — flat scalars suitable for log lines and report headers:

```python
igem.describe.genotype_summary(geno)
# {
#   "n_samples": 2504, "n_variants": 1103547,
#   "variant_call_rate_mean": 0.9985, "variant_call_rate_min": 0.952,
#   "sample_call_rate_mean":  0.9991, "sample_call_rate_min":  0.971,
#   "maf_mean": 0.142,
#   "n_variants_maf_lt_0.01": 12340,
#   "n_variants_maf_lt_0.05": 87651,
# }
```

---

## 9. Quick reference

### Phenotype helpers

| Function                                    | Returns                   | Use case                                |
| ------------------------------------------- | ------------------------- | --------------------------------------- |
| `igem.describe.summarize(phen)`             | DataFrame (1 row / col)   | Per-column overview                     |
| `igem.describe.summarize(phen, weighted=True)` | DataFrame              | Survey-weighted stats (NHANES)          |
| `igem.describe.summarize_by(phen, by="g")`  | DataFrame (long)          | Group-stratified summary                |
| `igem.describe.dataset_summary(phen)`       | dict                      | Dataset-level overview                  |
| `igem.describe.missing_report(phen)`        | DataFrame                 | Missing-value audit                     |
| `igem.describe.correlation_matrix(phen)`    | DataFrame (square)        | Pairwise correlation matrix             |
| `igem.describe.correlation_pairs(phen, threshold=0.75)` | DataFrame (long) | Strong pairs filtered             |
| `igem.describe.value_counts(phen, cols=…)`  | dict[col → DataFrame]     | Per-column frequencies                  |
| `igem.describe.crosstab(phen, v1, v2)`      | DataFrame                 | 2-way contingency / joint distribution  |
| `igem.describe.skewness(phen)`              | DataFrame                 | Skew + skewtest p-value                 |

### Genotype helpers

| Function                              | Returns         | Use case                                 |
| ------------------------------------- | --------------- | ---------------------------------------- |
| `igem.describe.variant_stats(geno)`   | DataFrame       | Per-variant QC: call rate, MAF, HWE      |
| `igem.describe.sample_stats(geno)`    | DataFrame       | Per-sample QC: call rate, het counts     |
| `igem.describe.heterozygosity(geno)`  | DataFrame       | Per-sample het rate + outlier flag       |
| `igem.describe.genotype_summary(geno)`| dict            | Aggregated genotype overview             |

---

## Related pages

- [Modifying data](modifying-data.md) — pure transformations on
  `Phenotypes` / `Genotypes`: type harmonization, log / rank-INT /
  Box-Cox, outlier replacement, column / row filtering, multi-frame
  assembly, and the full genotype QC pipeline including LD pruning.
- [Analyzing data](analyzing-data.md) — regression-based inference:
  `association_study`, `interaction_study`, multiple-testing
  correction, knowledge-graph annotation.
- [Reporting data](reporting-data.md) — biological annotation of
  variables and variants from the IGEM server.
- [Cookbook](../cookbook/index.md) — recipes that combine
  description with cleaning, modeling, and reporting.
