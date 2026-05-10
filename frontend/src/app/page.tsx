import Image from "next/image";
import Link from "next/link";
import { CopyableCode } from "@/components/CopyableCode";

export default function Home() {
  return (
    <div>
      <div className="mx-auto w-full max-w-[1600px]">
        <Image
          src="/banner.jpg"
          alt="IGEM — Integrating Genomics and Exposomics to Decode Gene-Environment Interactions"
          width={1983}
          height={793}
          priority
          className="block h-auto w-full"
        />
      </div>

      <section className="mx-auto max-w-5xl px-6 pt-16 pb-20">
        <div className="max-w-3xl">
          <h1 className="text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl dark:text-slate-50">
            Biologically-informed interaction filtering
          </h1>
          <p className="mt-6 text-lg leading-relaxed text-slate-600 dark:text-slate-300">
            IGEM uses a curated biomedical knowledge graph to reduce the
            GxG and GxE search space before multi-test correction —
            surfacing interaction hypotheses with biological grounding
            rather than testing every possible pair.
          </p>
          <div className="mt-8 flex flex-wrap gap-4">
            <a
              href="/docs/"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center justify-center rounded-md bg-brand px-5 py-2.5 text-sm font-medium text-white shadow-sm transition hover:bg-brand-hover"
            >
              Documentation
            </a>
            <Link
              href="/downloads"
              className="inline-flex items-center justify-center rounded-md border border-brand/25 bg-white/70 px-5 py-2.5 text-sm font-medium text-brand backdrop-blur-sm transition hover:border-brand/50 hover:bg-brand-tint dark:bg-transparent"
            >
              Download Parquet snapshots
            </Link>
          </div>
        </div>
      </section>

      <section className="border-y border-brand-tint bg-brand-tint/40 dark:border-zinc-800 dark:bg-zinc-900/30">
        <div className="mx-auto max-w-5xl px-6 py-20">
          <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
            From combinatorial chaos to grounded hypotheses
          </h2>
          <p className="mt-3 max-w-3xl text-slate-600 dark:text-slate-300">
            Pairwise interaction screens collapse under their own
            combinatorics. IGEM uses a curated knowledge graph as a
            pre-correction filter — testing fewer, smarter hypotheses
            instead of diluting power across billions of pairs.
          </p>
          <Image
            src="/overview.jpg"
            alt="IGEM reduces ~billions of possible gene-exposure interactions to a focused set of biologically grounded hypotheses, using a curated biomedical knowledge graph as the filter."
            width={1536}
            height={1024}
            className="mt-8 h-auto w-full rounded-lg"
          />
        </div>
      </section>

      <section className="mx-auto max-w-5xl px-6 py-20">
        <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
          One pip install. Zero infrastructure to manage.
        </h2>
        <p className="mt-4 max-w-3xl leading-relaxed text-slate-600 dark:text-slate-300">
          IGEM ships as a single Python package — no database to
          provision, no ETL to run, no graph to keep up to date.
          Biological knowledge lives on a managed remote server (or, for
          HPC, an offline Parquet snapshot via DuckDB), and your cohort
          data stays on your machine. From loading PLINK genotypes to
          producing biologically annotated Manhattan plots, the entire
          pipeline runs in a single Python session.
        </p>
        <Image
          src="/client-stack.jpg"
          alt="IGEM Client stack: six capability modules (Load, Describe, Modify, Analyze, Plot, Report) wrapping an end-to-end workflow from raw genotypes and phenotypes to biological interpretation. The same workflow runs in remote, embedded snapshot, or containerised modes."
          width={1536}
          height={1024}
          className="mt-6 h-auto w-full rounded-lg"
        />
        <p className="mt-3 text-sm text-slate-500 dark:text-slate-500">
          Six capability modules cover the end-to-end workflow, with the
          same code path running against a remote server, an offline
          snapshot, or inside a container.
        </p>
      </section>

      <section className="border-y border-brand-tint bg-brand-tint/40 dark:border-zinc-800 dark:bg-zinc-900/30">
        <div className="mx-auto max-w-5xl px-6 py-16">
          <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
            Built on a decade of methods research
          </h2>
          <p className="mt-4 max-w-3xl leading-relaxed text-slate-600 dark:text-slate-300">
            IGEM is the third generation of an integrated software line
            developed by the Hall Lab — extending the knowledge-driven
            filtering established by{" "}
            <strong className="font-semibold text-slate-700 dark:text-slate-200">
              Biofilter
            </strong>{" "}
            for GxG to GxE and ExE, on top of the analytical surface of{" "}
            <strong className="font-semibold text-slate-700 dark:text-slate-200">
              PLATO
            </strong>{" "}
            and{" "}
            <strong className="font-semibold text-slate-700 dark:text-slate-200">
              CLARITE
            </strong>
            .
          </p>
          <Link
            href="/publications"
            className="mt-6 inline-flex items-center gap-2 text-sm font-medium text-brand transition hover:gap-3 dark:text-brand-accent"
          >
            See publications and how to cite
            <span aria-hidden="true">→</span>
          </Link>
        </div>
      </section>

      <section className="bg-brand">
        <div className="mx-auto max-w-3xl px-6 py-20 text-center">
          <h2 className="text-3xl font-semibold tracking-tight text-white">
            Ready in five minutes
          </h2>
          <p className="mt-4 text-lg leading-relaxed text-white/80">
            Install with pip, point the client at the public IGEM
            server, and run your first knowledge-graph query.
          </p>
          <div className="mx-auto mt-8 max-w-md text-left">
            <CopyableCode code="pip install igem" />
          </div>
          <a
            href="/docs/getting-started/quickstart.html"
            target="_blank"
            rel="noopener noreferrer"
            className="mt-6 inline-flex items-center gap-2 text-sm font-medium text-white transition hover:text-brand-accent"
          >
            Read the Quickstart
            <span aria-hidden="true">→</span>
          </a>
        </div>
      </section>
    </div>
  );
}
