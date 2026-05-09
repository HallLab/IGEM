"""
Regenerate the figures embedded in ``user-guide/plotting-data.md``.

Outputs PNGs to ``docs/sphinx/_static/plot_examples/``. Reuses the
same synthetic-data builders as the showcase notebook
``docs/caderno/notebooks/plot_module_examples.ipynb`` so the page and
the notebook stay visually aligned.

Run from any cwd::

    poetry run python docs/sphinx/scripts/generate_plot_images.py

Re-run whenever a primitive's defaults / colours / labels change. The
PNGs are committed so the Sphinx build does not depend on the IGEM
client venv.
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sgkit

from igem import IGEM
from igem.modules.analyze.results import RegressionResults
from igem.modules.data import Genotypes, Phenotypes


REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = REPO_ROOT / "docs" / "sphinx" / "_static" / "plot_examples"


# ----------------------------------------------------------------------
# Synthetic-data builders (kept in sync with the showcase notebook)
# ----------------------------------------------------------------------
def make_phen(seed: int = 7, n: int = 200) -> Phenotypes:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "sample_id": [f"S{i:03d}" for i in range(n)],
            "BMI": rng.normal(27.0, 4.0, n),
            "AGE": rng.integers(20, 80, n).astype(float),
            "SEX": rng.choice(["M", "F"], n),
            "RACE": rng.choice(["white", "black", "asian", "other"], n),
        }
    )
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=["BMI"],
        covariates=["AGE", "SEX"],
    )


def make_phen_clean(phen: Phenotypes) -> Phenotypes:
    df = phen.df.loc[phen.df["BMI"] <= 35].copy()
    df["BMI"] = np.log(df["BMI"])
    return Phenotypes(
        df,
        sample_id_col="sample_id",
        outcomes=phen.outcomes,
        covariates=phen.covariates,
    )


def make_assoc_results(
    n_tests: int, *, with_chrom: bool = False, seed: int = 1,
) -> RegressionResults:
    r = np.random.default_rng(seed)
    pvals = r.uniform(low=1e-10, high=1.0, size=n_tests)
    pvals[0] = 1e-9
    beta = r.normal(0.0, 0.3, n_tests)
    se = np.abs(r.normal(0.05, 0.02, n_tests))
    df = pd.DataFrame(
        {
            "outcome": ["BMI"] * n_tests,
            "variable": [f"rs{i:05d}" for i in range(n_tests)],
            "variable_type": ["continuous"] * n_tests,
            "n": [1000] * n_tests,
            "beta": beta,
            "se": se,
            "ci_low": beta - 1.96 * se,
            "ci_high": beta + 1.96 * se,
            "beta_pvalue": pvals,
            "lrt_pvalue": pvals,
            "diff_aic": r.normal(0.0, 1.0, n_tests),
            "converged": [True] * n_tests,
            "error": [None] * n_tests,
        }
    )
    if with_chrom:
        df["chromosome"] = r.integers(1, 23, n_tests).astype(str)
        df["start_position"] = r.integers(1, 250_000_000, n_tests)
    return RegressionResults(
        df=df, family="linear", outcome="BMI",
        covariates=["AGE", "SEX"],
        formula_template="BMI ~ {variable} + AGE + SEX",
        errors=pd.DataFrame(),
    )


def make_interaction_results(terms: list[str], seed: int = 11) -> RegressionResults:
    r = np.random.default_rng(seed)
    pairs = [(a, b) for i, a in enumerate(terms) for b in terms[i + 1:]]
    df = pd.DataFrame(
        {
            "outcome": ["BMI"] * len(pairs),
            "term1": [a for a, _ in pairs],
            "term2": [b for _, b in pairs],
            "n": [1000] * len(pairs),
            "lrt_chi2": r.uniform(0.5, 20, len(pairs)),
            "lrt_df": [1] * len(pairs),
            "lrt_pvalue": r.uniform(1e-6, 1.0, len(pairs)),
            "diff_aic": r.normal(0, 5, len(pairs)),
            "converged": [True] * len(pairs),
            "error": [None] * len(pairs),
        }
    )
    return RegressionResults(
        df=df, family="linear", outcome="BMI", covariates=["AGE"],
        formula_template="BMI ~ {term1} + {term2} + {term1}:{term2} + AGE",
        errors=pd.DataFrame(),
    )


def make_geno(seed: int = 3) -> Genotypes:
    ds = sgkit.simulate_genotype_call_dataset(
        n_variant=200, n_sample=100, seed=seed,
    )
    n_variants = ds.sizes["variants"]
    n_samples = ds.sizes["samples"]
    ds["variant_id"] = (
        "variants",
        np.array([f"snp{i:04d}" for i in range(n_variants)]),
    )
    ds["sample_id"] = (
        "samples",
        np.array([f"S{i:03d}" for i in range(n_samples)]),
    )
    return Genotypes(ds)


# ----------------------------------------------------------------------
# Save helper
# ----------------------------------------------------------------------
def save(fig, name: str) -> None:
    out = OUT_DIR / f"{name}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches="tight", dpi=140)
    plt.close(fig)
    print(f"  wrote {out.relative_to(REPO_ROOT)}")


# ----------------------------------------------------------------------
# Figures
# ----------------------------------------------------------------------
def main() -> int:
    print(f"Generating images into {OUT_DIR.relative_to(REPO_ROOT)}\n")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    phen = make_phen()
    phen_clean = make_phen_clean(phen)
    phen_filtered = Phenotypes(
        phen.df.iloc[:120].copy(),
        sample_id_col="sample_id",
        outcomes=phen.outcomes,
        covariates=phen.covariates,
    )
    results = make_assoc_results(200)
    results_chrom = make_assoc_results(200, with_chrom=True, seed=2)
    results_corr = results.with_correction("fdr_bh")
    interaction = make_interaction_results([f"v{i}" for i in range(8)])
    geno = make_geno()

    with IGEM() as igem:
        # --- bridges --------------------------------------------------
        save(
            igem.plot.from_results(results_chrom, kind="manhattan"),
            "from_results_manhattan",
        )
        save(
            igem.plot.from_results(results, kind="qq"),
            "from_results_qq",
        )
        save(
            igem.plot.from_results(results, kind="top", n_top=12),
            "from_results_top",
        )
        save(
            igem.plot.from_results(results, kind="manhattan_bonferroni"),
            "from_results_manhattan_bonferroni",
        )
        save(
            igem.plot.from_results(results_corr, kind="manhattan_fdr"),
            "from_results_manhattan_fdr",
        )

        figs = igem.plot.from_describe(phen)
        save(figs[0], "from_describe_grid")
        figs_violin = igem.plot.from_describe(phen, continuous_kind="violin")
        save(figs_violin[0], "from_describe_violin")

        save(
            igem.plot.from_modify(phen, phen_clean, var="BMI"),
            "from_modify_overlay",
        )
        save(
            igem.plot.from_modify(
                phen, phen_clean, var="BMI", layout="side_by_side",
            ),
            "from_modify_side_by_side",
        )
        save(
            igem.plot.from_modify(phen, phen_filtered, var="RACE"),
            "from_modify_categorical",
        )
        save(
            igem.plot.from_modify(geno, geno, metric="maf", layout="side_by_side"),
            "from_modify_geno_maf",
        )

        save(
            igem.plot.from_interaction(interaction, kind="heatmap"),
            "from_interaction_heatmap",
        )
        save(
            igem.plot.from_interaction(
                interaction, kind="heatmap", cluster=True, annotate=True,
            ),
            "from_interaction_heatmap_cluster",
        )
        save(
            igem.plot.from_interaction(interaction, kind="top_pairs", n_top=10),
            "from_interaction_top_pairs",
        )

        # --- primitives -----------------------------------------------
        fig, axes = plt.subplots(1, 4, figsize=(14, 3.4))
        for ax, kind in zip(axes, ["hist", "box", "violin", "qq"]):
            igem.plot.distribution(
                phen.df["BMI"], continuous_kind=kind, ax=ax, title=kind,
            )
        fig.suptitle("distribution(BMI) — four continuous_kind options")
        fig.tight_layout()
        save(fig, "primitives_distribution_grid")

        save(
            igem.plot.distribution(phen.df["RACE"], title="RACE"),
            "primitives_distribution_categorical",
        )

        # signed_neglog10 heatmap example with mock β
        rng = np.random.default_rng(0)
        df_sign = interaction.df.copy()
        df_sign["mock_beta"] = rng.normal(0, 1, len(df_sign))
        save(
            igem.plot.heatmap(
                df_sign,
                sign_col="mock_beta",
                transform="signed_neglog10",
                cmap="RdBu_r",
                title=r"sign($\beta$) $\cdot -\log_{10}$(p)",
            ),
            "primitives_heatmap_signed",
        )

        # miami_plot — discovery vs replication
        results_top = make_assoc_results(150, seed=10)
        results_bottom = make_assoc_results(150, seed=11)
        save(
            igem.plot.miami_plot(
                results_top.df,
                results_bottom.df,
                top_label="discovery",
                bottom_label="replication",
                cutoffs=[(5e-8, "#C0392B", "--")],
            ),
            "primitives_miami",
        )

    plt.close("all")
    print(f"\nAll images regenerated in {OUT_DIR.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
