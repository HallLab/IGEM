import time

from igem_backend.ge import GE

# db_uri = "sqlite:///ge.db"
db_uri = "postgresql://dev:dev@localhost/igem"

# Configure below
data_sources_to_process = [
    # Genes
    # -----
    "gene_hgnc",
    "gene_ncbi",
    "gene_ensembl",
    #
    # Proteins
    # --------
    "protein_pfam",
    "protein_uniprot",
    #
    # Pathways
    # --------
    "pathway_reactome",
    "pathway_kegg",
    #
    # Gene Ontology
    # -------------
    "go",
    #
    # Disease
    # -------
    "disease_mondo",
    "disease_mesh",
    #
    # Chemical
    # --------
    "chemical_chebi",
    "chemical_mesh",
    # "chemical_hmdb",
    #
    # Phenotype
    # ---------
    "phenotype_hpo",
    #
    # Anatomy
    # -------
    "anatomy_uberon",
    #
    # NLP
    # ---
    # "nlp_hmdb",
    #
    # Relationships
    # -------------
    "relationship_uniprot",
    "relationship_mondo",
    "relationship_reactome",
    "relationship_hpo_genes",
    "relationship_ctd_gene_disease",
    "relationship_ctd_chem_gene",
]

run_steps = [
    "extract",
    "transform",
    "load",
    # "all"
]  # noqa E501

if __name__ == "__main__":
    ge = GE(db_uri=db_uri, debug_mode=True)
    # ge = GE(db_uri=db_uri)

    start_total = time.time()

    print()

    for source in data_sources_to_process:
        for step in run_steps:

            start_process = time.time()

            if step != "all":
                try:
                    print(f"▶ Running ETL - Source: {source} | Step: {step}")
                    ge.etl.run(
                        data_sources=[source],
                        steps=[step],
                        force_steps=[step],
                    )
                except Exception as e:
                    print(f"❌ Error processing {source} [{step}]: {e}")
            elif step == "all":
                try:
                    print(f"▶ Running ETL - Source: {source} | Step: {step}")
                    ge.etl.run(
                        data_sources=[source],
                        # steps=[step],
                        # force_steps=[step],
                    )
                except Exception as e:
                    print(f"❌ Error processing {source} [{step}]: {e}")

            end_process = time.time() - start_process
            msg = str(
                f"processed Time Total: {end_process:.2f}s"  # noqa E501
            )  # noqa E501
            print(msg)

    end_time = time.time() - start_total
    msg = str(f"job Time Total: {end_time:.2f}s")  # noqa E501  # noqa E501
    print(msg)

    print("✅ All ETL tasks finished.")
    print("------------------------------")
