import Image from "next/image";
import Link from "next/link";

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

      <div className="mx-auto max-w-5xl px-6 pt-16 pb-20">
        <section className="max-w-3xl">
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
        </section>

        <section className="mt-20">
          <h2 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">
            How IGEM works
          </h2>
          <Image
            src="/overview.jpg"
            alt="IGEM reduces ~billions of possible gene-exposure interactions to a focused set of biologically grounded hypotheses, using a curated biomedical knowledge graph as the filter."
            width={1536}
            height={1024}
            className="mt-6 h-auto w-full rounded-lg"
          />
        </section>

        <section className="mt-20 grid gap-8 sm:grid-cols-3">
          <div>
            <h2 className="mb-2 font-semibold text-slate-900 dark:text-slate-100">
              Knowledge graph
            </h2>
            <p className="text-sm leading-relaxed text-slate-600 dark:text-slate-400">
              ~980k chemicals, ~217k metabolites, ~46k human genes, and
              millions of cross-references across HMDB, ChEBI, MeSH,
              UniProt, MONDO and HPO.
            </p>
          </div>
          <div>
            <h2 className="mb-2 font-semibold text-slate-900 dark:text-slate-100">
              Privacy-preserving
            </h2>
            <p className="text-sm leading-relaxed text-slate-600 dark:text-slate-400">
              Your data stays local. The Python client only sends
              identifiers to the backend — never raw genotypes or
              phenotypes.
            </p>
          </div>
          <div>
            <h2 className="mb-2 font-semibold text-slate-900 dark:text-slate-100">
              Open snapshots
            </h2>
            <p className="text-sm leading-relaxed text-slate-600 dark:text-slate-400">
              Read-only Parquet snapshots are published publicly so you
              can pin a version and reproduce analyses without running
              the ETL.
            </p>
          </div>
        </section>
      </div>
    </div>
  );
}
