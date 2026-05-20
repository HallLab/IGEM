import type { Metadata } from "next";
import { CopyableCode } from "@/components/CopyableCode";

export const metadata: Metadata = {
  title: "Publications — IGEM",
  description:
    "How to cite IGEM and the foundational methods research it builds on (PLATO, CLARITE, Biofilter).",
};

const bibtex = `@software{igem_2026,
  author  = {Hall Lab},
  title   = {{IGEM}: Biologically-informed interaction filtering for {GWAS} and {EWAS}},
  year    = {2026},
  url     = {https://geneexposure.org},
  version = {0.1.0},
  note    = {Early access},
}`;

const foundational = [
  {
    name: "PLATO",
    citation: "Hall et al., Nature Communications, 2017",
    description:
      "Integrated framework for GxG, GxE, and phenome-wide association studies, applied to type 2 diabetes in the eMERGE Network.",
  },
  {
    name: "CLARITE",
    citation: "Lucas et al., Frontiers in Genetics, 2019",
    description:
      "High-throughput pipeline for exposome quality control and environment-wide association studies.",
  },
  {
    name: "Biofilter",
    citation: "Bush et al., 2009 · Pendergrass et al., 2013",
    description:
      "Established knowledge-driven filtering for gene–gene interactions — the methodology IGEM extends to GxE and ExE.",
  },
];

export default function PublicationsPage() {
  return (
    <div>
      <section className="border-b border-brand-tint dark:border-zinc-800">
        <div className="mx-auto max-w-5xl px-6 pt-16 pb-12">
          <h1 className="text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl dark:text-slate-50">
            Publications
          </h1>
          <p className="mt-4 max-w-3xl text-lg leading-relaxed text-slate-600 dark:text-slate-300">
            How to cite IGEM, the methods research it builds on, and a
            growing list of studies that use it.
          </p>
        </div>
      </section>

      <section className="mx-auto max-w-5xl px-6 py-16">
        <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
          Cite IGEM
        </h2>
        <p className="mt-3 max-w-3xl text-slate-600 dark:text-slate-300">
          If IGEM contributed to your research, please cite the platform
          using the BibTeX entry below. Update the version field to match
          the release you used.
        </p>
        <CopyableCode code={bibtex} className="mt-6" />
      </section>

      <section className="bg-brand-tint/40 dark:bg-zinc-900/40 border-y border-brand-tint dark:border-zinc-800">
        <div className="mx-auto max-w-5xl px-6 py-16">
          <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
            Foundational work
          </h2>
          <p className="mt-3 max-w-3xl text-slate-600 dark:text-slate-300">
            IGEM is the third generation of an integrated software line
            developed by the Hall Lab for high-dimensional interaction
            research. It combines the analytical surface of PLATO and
            CLARITE with a knowledge-driven filter inherited from
            Biofilter.
          </p>
          <div className="mt-8 grid gap-6 md:grid-cols-3">
            {foundational.map((paper) => (
              <article
                key={paper.name}
                className="rounded-lg border border-brand-tint bg-white/70 p-5 shadow-sm backdrop-blur-sm dark:border-zinc-800 dark:bg-zinc-900/60"
              >
                <h3 className="text-lg font-semibold text-brand dark:text-brand-accent">
                  {paper.name}
                </h3>
                <p className="mt-1 text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
                  {paper.citation}
                </p>
                <p className="mt-3 text-sm leading-relaxed text-slate-600 dark:text-slate-300">
                  {paper.description}
                </p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-5xl px-6 py-16">
        <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
          Powered by IGEM
        </h2>
        <p className="mt-3 max-w-3xl text-slate-600 dark:text-slate-300">
          Studies that used IGEM in their analytical pipeline. This
          section grows with the community — if your paper used IGEM,
          we&apos;d love to feature it here.
        </p>
        <div className="mt-8 rounded-lg border border-dashed border-brand-tint bg-brand-tint/20 p-8 text-center dark:border-zinc-800 dark:bg-zinc-900/40">
          <p className="text-sm text-slate-600 dark:text-slate-400">
            No featured studies yet. To submit a paper for inclusion,
            open an issue at{" "}
            <a
              href="https://github.com/HallLab/IGEM/issues"
              target="_blank"
              rel="noopener noreferrer"
              className="font-medium text-brand underline-offset-4 hover:underline dark:text-brand-accent"
            >
              github.com/HallLab/IGEM
            </a>
            .
          </p>
        </div>
      </section>
    </div>
  );
}
