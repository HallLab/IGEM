# Analyzing data

The `analyze` module fits regression models for inference. Two
unified entrypoints — **`association_study`** for EWAS / PheWAS /
GWAS and **`interaction_study`** for pairwise GxE / GxG via
likelihood-ratio test — accept lists of outcomes and regressors,
auto-detect the regression family (linear / logistic), support
five genotype encodings (additive / dominant / recessive /
codominant / edge), and parallelise the scan via joblib. Survey-
aware estimation (sample weights with cluster-robust standard
errors) is one flag away. Results land in an immutable
`RegressionResults` container with chainable
`.with_correction → .passing → .top → .annotate → .summary`
methods, ready to be filtered, annotated against the IGEM knowledge
graph, and persisted.

```{tip}
Every function in this module returns a :class:`RegressionResults` —
an immutable, chainable container with built-in
``.with_correction → .passing → .top → .annotate → .summary``
fluency. Inputs are never mutated; each transformation returns a
fresh result.
```

```{note}
**No CLI commands for this module.** Inference produces in-memory
results for downstream filtering, plotting, or report generation
within the same Python session. Use Python or a notebook.
```

By the end of this page you will know how to:

- Choose between **`association_study`** (unified EWAS / PheWAS /
  GWAS), **`interaction_study`** (pairwise GxE / GxG via LRT), and
  the legacy specialized entrypoints (`ewas`, `gwas`, `lrt`).
- Run **single-outcome** and **multi-outcome PheWAS** with
  per-outcome multiple-testing correction.
- Test **genotype** regressors under five different encoding
  conventions (additive / dominant / recessive / codominant / edge).
- Apply **survey-aware** estimation (weights + cluster-robust SE)
  for NHANES-style designs.
- Parallelise the regression scan with **`n_jobs`**.
- Read the **canonical output schema** — Wald / LRT p-values, ΔAIC,
  convergence flags — and chain transformations on it.

All examples below use the `IGEM` facade, consistent with the rest
of the user guide:

```python
from igem import IGEM

with IGEM() as igem:
    phen = igem.data.read_phenotypes(
        "nhanes.csv", outcomes=["GLUCOSE"],
    )

    res = igem.analyze.interaction_study(
        phen,
        outcomes="GLUCOSE",                  # what we model
        covariates=["AGE", "SEX"],           # adjustment set
        interactions=[                       # candidate pairs to test
            ("BMI", "SMOKING"),
            ("BMI", "DIET_QUALITY"),
            ("PHYSICAL_ACTIVITY", "DIET_QUALITY"),
        ],
        n_jobs=-1,                           # parallelise across cores
    ).with_correction("fdr_bh")              # Benjamini-Hochberg FDR
```

Each interaction pair becomes one likelihood-ratio test; the result
is a `RegressionResults` indexed by `(outcome, term1, term2)` ready
for further filtering or annotation.

Free functions in `igem.modules.analyze` remain importable when an
`IGEM` instance is not needed.

---

## 1. When to use what

| Function                   | Best for                                                    |
| -------------------------- | ----------------------------------------------------------- |
| `association_study(...)`   | EWAS / PheWAS / small-to-medium GWAS with full flexibility (multi-outcome, all encodings, parallel, custom backend) |
| `interaction_study(...)`   | Pairwise GxE / GxG screening via LRT                        |
| `ewas(...)`                | Single-outcome EWAS, ergonomic shortcut over `association_study` |
| `gwas(...)`                | Biobank-scale additive linear GWAS via sgkit's vectorised regression — order-of-magnitude faster than `association_study(geno=...)` for the additive-linear case |
| `lrt(...)`                 | Standalone likelihood-ratio test on two arbitrary nested models (utility) |

The first two are the **canonical entrypoints** — everything else
is either a thin wrapper (`ewas`) or an optimised special case
(`gwas`).

---

## 2. `association_study` — unified entrypoint

### The minimum-viable call

```python
res = igem.analyze.association_study(
    phen, outcome="GLUCOSE", regression_variables=["BMI"],
)
```

For each `(outcome, regressor)` pair the function fits two nested
models — **full** with the regressor of interest plus covariates,
and **null** with covariates only — and reports both Wald and LRT
statistics in a long-format DataFrame.

**Linear regression** (continuous outcome):

$$
y_i = \beta_0 + \beta\, x_i + \boldsymbol{\gamma}^\top \boldsymbol{c}_i + \epsilon_i,
\qquad \epsilon_i \sim \mathcal{N}(0, \sigma^2)
$$

**Logistic regression** (binary outcome):

$$
\mathrm{logit}\!\big(\mathbb{P}(y_i = 1)\big) = \beta_0 + \beta\, x_i + \boldsymbol{\gamma}^\top \boldsymbol{c}_i
$$

The family is **auto-detected** from the outcome dtype unless
overridden. With $n$ samples and $k$ parameters, statsmodels fits
via maximum-likelihood and reports:

| Field           | Definition                                            |
| --------------- | ----------------------------------------------------- |
| `beta`          | $\hat{\beta}$, coefficient for the regressor          |
| `se`            | $\sqrt{\hat{V}_{\hat{\beta}}}$ from the inverse Fisher information |
| `ci_low`/`ci_high` | $\hat{\beta} \pm 1.96 \cdot \text{SE}$              |
| `beta_pvalue`   | Wald p-value: $p = 2\,\Phi\big(-|\hat{\beta}/\text{SE}|\big)$ |
| `lrt_pvalue`    | Likelihood-ratio test p-value (see §3 formulas)       |
| `diff_aic`      | $\Delta\mathrm{AIC} = \mathrm{AIC}_F - \mathrm{AIC}_N$, ([Akaike, 1974]) |
| `n`             | Sample count after listwise deletion                  |
| `converged`     | statsmodels convergence flag                          |
| `error`         | First line of the error message if the fit failed     |
| `variable_type` | `continuous` / `binary` / `categorical` / `genotype`  |

[Akaike, 1974]: https://doi.org/10.1109/TAC.1974.1100705

### Multi-outcome PheWAS

Pass a list of outcomes — each outcome × regressor pair is fitted
independently. Combine with `groupby` correction so multiple-testing
adjustment is applied **within** each outcome:

```python
res = igem.analyze.association_study(
    phen,
    outcomes=["GLUCOSE", "LDL", "HDL", "TG", "BP_SYS"],
    regression_variables=phen.exposures,
    n_jobs=-1,                  # parallelise across cores
).with_correction("fdr_bh", groupby="outcome")
```

### Genotype regressors

Pass a `Genotypes` wrapper via `geno=` — `regression_variables`
becomes the list of `variant_id`s to test. Five encodings cover the
common genetic-architecture assumptions:

```python
res_add = igem.analyze.association_study(
    phen, outcome="GLUCOSE",
    regression_variables=["rs429358", "rs7412"],
    geno=geno,
    encoding="additive",       # default
)

res_dom = igem.analyze.association_study(
    phen, outcome="GLUCOSE",
    regression_variables=["rs429358"],
    geno=geno,
    encoding="dominant",
)
```

| Encoding      | Transformation                                  | Notes |
| ------------- | ----------------------------------------------- | ----- |
| `additive`    | dosage $x \in \{0, 1, 2\}$                      | Linear allele-count effect (default) |
| `dominant`    | $x' = \mathbb{1}[x \geq 1]$                     | Any alt allele triggers              |
| `recessive`   | $x' = \mathbb{1}[x = 2]$                        | Two alt alleles required             |
| `codominant`  | One-hot het / hom-alt vs hom-ref                | Multi-df LRT joins both contrasts    |
| `edge`        | per-variant lookup from `edge_encoding_info`    | Biologically-informed scoring        |

**Edge encoding** lets you replace the dosage with a biologically
meaningful score per genotype. Pair with the IGEM knowledge graph to
score variants by functional impact, conservation, or pathway
membership:

```python
edge_info = pd.DataFrame(
    {
        "score_0": [0.0,  0.0],     # hom-ref
        "score_1": [0.5,  1.0],     # het
        "score_2": [1.0,  1.5],     # hom-alt
    },
    index=["rs429358", "rs7412"],
)
res = igem.analyze.association_study(
    phen, "GLUCOSE",
    regression_variables=["rs429358", "rs7412"],
    geno=geno,
    encoding="edge",
    edge_encoding_info=edge_info,
)
```

```{tip}
**`gwas()` vs `association_study(geno=...)`**. Both fit the same
model for the additive-linear case, but `gwas()` uses sgkit's
vectorised SVD-based regression — single matrix decomposition over
all variants — while `association_study` loops per variant with
statsmodels. At biobank scale (10⁶+ variants) the speed difference
is hours vs days. Use `gwas()` for additive-linear GWAS at scale;
use `association_study(geno=...)` when you need a non-additive
encoding, logistic outcome, or a custom regression backend.
```

### Survey-aware analysis

For NHANES-style complex sample designs, set `weights_col` /
`cluster_col` / `strata_col` at load time and pass `use_survey=True`:

```python
phen = igem.data.read_phenotypes(
    "nhanes.csv", outcomes=["GLUCOSE"],
    weights_col="WTMEC", cluster_col="SDMVPSU", strata_col="SDMVSTRA",
)

res = igem.analyze.association_study(
    phen, "GLUCOSE", phen.exposures,
    use_survey=True,
)
```

The default backend (`regression_kind="auto"` → `glm_engine`) applies:

- **Sample weights** as `freq_weights` in the GLM, giving the
  weighted point estimate

  $$
  \hat{\boldsymbol{\beta}}_w = (X^\top W X)^{-1} X^\top W \boldsymbol{y},
  \qquad W = \mathrm{diag}(w_i)
  $$

- **Cluster-robust ("sandwich") variance** when `cluster_col` is set
  ([White, 1980]; [Liang & Zeger, 1986]):

  $$
  \hat{V}_{\mathrm{CR}}\!\left(\hat{\boldsymbol{\beta}}\right) =
  (X^\top X)^{-1}
  \!\left(\sum_g X_g^\top \hat{\boldsymbol{u}}_g \hat{\boldsymbol{u}}_g^\top X_g\right)\!
  (X^\top X)^{-1}
  $$

  where $g$ indexes clusters and $\hat{\boldsymbol{u}}_g$ are the
  cluster's residuals.

[White, 1980]: https://doi.org/10.2307/1912934
[Liang & Zeger, 1986]: https://doi.org/10.1093/biomet/73.1.13

```{note}
**Stratified Taylor-series variance** (the gold-standard NHANES
estimator) is not implemented in `glm_engine`. For production
NHANES analysis pass ``regression_kind="r_survey"`` to use R's
`survey` package via `rpy2`. Install with
`poetry install --with r-survey`.
```

### Parallelism

```python
res = igem.analyze.association_study(
    phen, outcomes=phen.outcomes, regression_variables=phen.exposures,
    n_jobs=-1,                  # all cores
)
```

`n_jobs` follows joblib semantics — `1` is sequential, `-1` uses
every available core, integers in between are explicit pool sizes.
Tasks are distributed at the `(outcome, regressor)` granularity, so
parallelism scales with the number of pairs.

### Other parameters

| Parameter                     | Effect                                            |
| ----------------------------- | ------------------------------------------------- |
| `min_n=200`                   | Skip pairs with fewer complete cases (default matches CLARITE; failed pairs land in `result.errors`) |
| `standardize_data=True`       | Z-score continuous regressors before fitting (effect-size comparable across features) |
| `report_categorical_betas=True` | Emit one row per category dummy in addition to the LRT summary |
| `regression_kind="weighted_glm"` | Force survey-weighted GLM (auto-selected when `use_survey=True`) |
| `regression_kind=callable`    | Custom regression backend (must return :class:`igem.modules.analyze._engines.EngineResult`) |

---

## 3. `interaction_study` — pairwise interactions

For every pair of variables $(v_1, v_2)$ and each outcome, fit two
nested models and compare via LRT:

- **Restricted** (additive only):
  $\mathrm{outcome} \sim v_1 + v_2 + \boldsymbol{c}$
- **Full** (additive + interaction):
  $\mathrm{outcome} \sim v_1 + v_2 + v_1{:}v_2 + \boldsymbol{c}$

The likelihood-ratio test statistic ([Wilks, 1938]):

$$
\Lambda = -2\,\big(\ell_R - \ell_F\big) \;\sim_{H_0}\; \chi^2_{\,df}
$$

with $df$ equal to the number of additional parameters in the full
model:

- $df = 1$ for continuous × continuous
- $df = (k_1 - 1)(k_2 - 1)$ for categorical × categorical with
  arities $k_1$ and $k_2$
- mixed cases reduce accordingly (e.g. continuous × 3-level
  categorical gives $df = 2$)

[Wilks, 1938]: https://doi.org/10.1214/aoms/1177732360

```python
res = igem.analyze.interaction_study(
    phen, "GLUCOSE",
    interactions=[("SMOKING", "RACE"), ("BMI", "AGE")],
    n_jobs=-1,
)
res.df[["term1", "term2", "n", "lrt_chi2", "lrt_df", "lrt_pvalue"]]
```

### Three input forms for `interactions=`

```python
# All unordered pairs of phen.exposures (capped by max_pairs).
interactions=None

# Anchor: every pair (anchor, other) with other ∈ phen.exposures \ {anchor}.
interactions="BMI"

# Explicit list of tuples.
interactions=[("BMI", "AGE"), ("SMOKING", "RACE")]
```

When `interactions=None` and the universe of pairs exceeds
`max_pairs` (default 1000), the function raises rather than running
silently — pre-filter via the knowledge graph or pass an explicit
list.

### `report_betas=True`

By default the result has one summary row per pair (LRT-only). With
`report_betas=True` each interaction term gets its own row with the
coefficient, SE, and Wald p-value of that contrast — useful for
inspecting the direction of interaction effects in categorical
pairs.

```{tip}
**This is IGEM's core value proposition**. The combinatorial
explosion of pairwise GxE / GxG tests dominates power-loss to
multiple-testing correction. By filtering candidate pairs upstream
through biological knowledge (gene–gene relationships, pathway
co-membership) the universe of tests shrinks to a tractable size,
and `interaction_study` runs LRT on the shortlisted pairs.
```

---

## 4. Multiple-testing correction

Apply correction post-hoc on a `RegressionResults`:

```python
out = (
    igem.analyze.association_study(phen, ["GLUCOSE", "LDL"], phen.exposures)
    .with_correction("fdr_bh", groupby="outcome")
    .passing(p_corrected=0.05)
    .top(20)
)
```

Six methods are exposed via statsmodels' `multipletests`:

| `method`         | Adjustment                              | Reference                       |
| ---------------- | --------------------------------------- | ------------------------------- |
| `bonferroni`     | $p^{\mathrm{adj}}_i = \min(1, m\,p_i)$  | Bonferroni, 1936               |
| `holm`           | Step-down Bonferroni                    | Holm, 1979                     |
| `sidak`          | $p^{\mathrm{adj}}_i = 1 - (1 - p_i)^m$  | Šidák, 1967                    |
| `hommel`         | Step-up                                 | Hommel, 1988                   |
| `fdr_bh`         | $p^{\mathrm{adj}}_{(i)} = \min_{k \geq i}\!\frac{m\,p_{(k)}}{k}$ | [Benjamini & Hochberg, 1995]   |
| `fdr_by`         | BH adjusted for arbitrary dependence    | [Benjamini & Yekutieli, 2001]  |

[Benjamini & Hochberg, 1995]: https://doi.org/10.1111/j.2517-6161.1995.tb02031.x
[Benjamini & Yekutieli, 2001]: https://doi.org/10.1214/aos/1013699998

`bonferroni` controls **FWER** strictly but is conservative;
`fdr_bh` controls the **false discovery rate**, generally
recommended for EWAS / PheWAS where many tests are expected to be
correlated and a moderate false-positive rate is acceptable.

### Per-outcome correction (`groupby=`)

Critical for multi-outcome PheWAS. With `groupby="outcome"` (or any
column of the result), the correction is applied **within each
group** rather than across the entire result, preventing the false
inflation of the test count when multiple outcomes are scanned in
one call:

```python
# Wrong (inflates m by n_outcomes):
res.with_correction("fdr_bh")

# Right:
res.with_correction("fdr_bh", groupby="outcome")
```

---

## 5. Family auto-detection

The family is inferred from the outcome column unless overridden:

```python
from igem.modules.analyze import infer_family
infer_family(phen.df["GLUCOSE"])      # → "linear"  (continuous numeric)
infer_family(phen.df["DIABETES"])     # → "logistic" (binary 0/1 or bool)
```

Rule:

| Outcome dtype / values                 | Family       |
| -------------------------------------- | ------------ |
| `bool`                                 | `logistic`   |
| numeric with non-NaN values $\subseteq \{0, 1\}$ | `logistic`   |
| numeric with $\geq 3$ distinct values  | `linear`     |
| object / category                      | `ValueError` (encode first with `igem.modify.recode`) |

Override explicitly when needed:

```python
# Force linear regression on a binary outcome (e.g. for linear-probability
# model in robustness checks).
res = igem.analyze.association_study(
    phen, "DIABETES", phen.exposures, family="linear",
)
```

---

## 6. Choosing a regression engine

`regression_kind` selects the inference backend:

| Value             | Engine                                      |
| ----------------- | ------------------------------------------- |
| `"auto"` (default) | `glm_engine` — statsmodels GLM             |
| `"glm"`           | Same as auto without survey weights         |
| `"weighted_glm"`  | `glm_engine` with `freq_weights` populated  |
| `"r_survey"`      | `r_survey_engine` — R's `survey` via rpy2 (Taylor-series stratified variance, gold standard NHANES). Requires `poetry install --with r-survey` |
| callable          | Custom backend returning :class:`EngineResult` |

A custom callable is the escape hatch for research backends:

```python
def my_engine(y, X, family, *, weights=None, cluster=None):
    fit = my_special_fitter(y, X, family)
    from igem.modules.analyze._engines import EngineResult
    return EngineResult(
        params=fit.params, bse=fit.bse, pvalues=fit.pvalues,
        conf_int=fit.conf_int, log_likelihood=fit.llf,
        aic=fit.aic, n=fit.nobs, converged=fit.converged,
    )

res = igem.analyze.association_study(
    phen, "GLUCOSE", phen.exposures, regression_kind=my_engine,
)
```

---

## 7. Result chaining

`RegressionResults` is **immutable** — every transformation returns
a new instance. The result composes naturally into a fluent
pipeline:

```python
publishable = (
    igem.analyze.association_study(phen, "GLUCOSE", phen.exposures, n_jobs=-1)
    .with_correction("fdr_bh")
    .passing(p_corrected=0.05)
    .annotate(igem)               # gene-symbol / HGNC merge from the IGEM KG
    .top(20, by="lrt_pvalue")
)
publishable.to_csv("ewas_top20_annotated.csv")
```

| Method                      | Effect                                        |
| --------------------------- | --------------------------------------------- |
| `.with_correction(method, groupby=None)` | Adds `p_corrected` column        |
| `.passing(p=…, p_corrected=…)` | Filter rows by raw or corrected p-value     |
| `.top(n, by=None)`          | Sort + head; `by=None` auto-picks `beta_pvalue` |
| `.annotate(client)`         | Left-join gene-annotation columns from the KG |
| `.summary()`                | One-line dict for log lines / report headers  |
| `.to_csv(path)`             | Persist                                        |

---

## 8. Specialised entrypoints

### `ewas` — single-outcome EWAS shortcut

A thin wrapper of `association_study` that pins to a single
outcome, defaults `regression_variables` to `phen.exposures`, and
emits the **legacy result schema** (`variable, n, beta, se, ci_low,
ci_high, p_value`) for retrocompatibility with notebooks written
before the unified entrypoint:

```python
res = igem.analyze.ewas(phen, "GLUCOSE")  # equivalent to:
# igem.analyze.association_study(phen, "GLUCOSE", phen.exposures)
# adapted to the legacy column names.
```

### `gwas` — sgkit-vectorised additive-linear GWAS

Order-of-magnitude faster than `association_study(geno=...)` for
the additive-linear case at biobank scale. Restricted to:

- Continuous outcome (linear). Logistic raises `NotImplementedError`.
- Additive encoding only.

```python
res = igem.analyze.gwas(geno, phen, "GLUCOSE")
```

For other encodings or logistic outcome, route through
`association_study(geno=geno, ...)`.

### `lrt` — standalone likelihood-ratio test

For comparing two arbitrary nested models on the same outcome and
data — useful when you need the LRT machinery outside of
association / interaction:

```python
out = igem.analyze.lrt(
    phen, "GLUCOSE",
    full=["AGE", "SEX", "BMI", "SMOKING"],
    nested=["AGE", "SEX"],
)
out["chi2"], out["df"], out["p_value"]
```

---

## 9. Quick reference

### Main entrypoints

| Function                                                | Purpose                                          |
| ------------------------------------------------------- | ------------------------------------------------ |
| `igem.analyze.association_study(phen, outcomes, …)`     | Unified EWAS / PheWAS / GWAS                     |
| `igem.analyze.interaction_study(phen, outcomes, …)`     | Pairwise GxE / GxG via LRT                       |
| `igem.analyze.ewas(phen, outcome)`                      | Single-outcome EWAS shortcut (legacy schema)     |
| `igem.analyze.gwas(geno, phen, outcome)`                | Biobank-scale additive linear GWAS via sgkit     |
| `igem.analyze.lrt(phen, outcome, full=, nested=)`       | Standalone likelihood-ratio test                 |

### Result chaining

| Method                                                     | Returns                  |
| ---------------------------------------------------------- | ------------------------ |
| `.with_correction("fdr_bh", groupby="outcome")`            | `RegressionResults`      |
| `.passing(p=0.01)`                                         | `RegressionResults`      |
| `.top(20)`                                                 | `RegressionResults`      |
| `.annotate(igem)`                                          | `RegressionResults`      |
| `.summary()`                                               | `dict`                   |
| `.to_csv(path)`                                            | `Path`                   |

### Module-level utilities

| Function                                              | Purpose                                       |
| ----------------------------------------------------- | --------------------------------------------- |
| `igem.modules.analyze.infer_family(series)`           | Returns `"linear"` / `"logistic"`             |
| `igem.modules.analyze.apply_correction(pvalues, method)` | Direct numpy-array correction (no wrapper) |
| `igem.modules.analyze.list_methods()`                 | Available correction methods                  |

---

## Related pages

- [Describing data](describing-data.md) — per-column summaries,
  correlations, contingency tables, and genotype QC quantities that
  inform regressor / outcome selection before running an
  association.
- [Modifying data](modifying-data.md) — log / rank-INT / Box-Cox /
  z-score transformations on phenotypes; outlier replacement;
  filtering and merging for multi-cohort analysis.
- [Reporting data](reporting-data.md) — pre-filter the candidate
  set of GxG / GxE pairs against the IGEM knowledge graph to keep
  `interaction_study`'s test universe tractable.
- [Cookbook](../cookbook/index.md) — end-to-end recipes for
  typical EWAS, PheWAS, and GxE workflows.
