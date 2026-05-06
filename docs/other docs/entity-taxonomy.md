# IGEM Entity Taxonomy — Design Proposal

> **Status:** Draft — open for team discussion
> **Author:** Andre Rico
> **Date:** 2026-04-22

---

## Overview

All biological and environmental knowledge in IGEM is represented as **Entities**.
Each Entity belongs to an **EntityType** (what kind of entity it is), and each EntityType
belongs to an **EntityDomain** (the scientific domain it operates in).

```
EntityDomain  ──→  EntityType  ──→  Entity  ──→  EntityAlias
 (Genomics)         (Gene)           (TP53)        (p53, TRP53, 7157...)
 (Exposome)         (Chemical)       (Benzene)     (71-43-2, C6H6...)
 (Knowledge)        (Pathway)        (Apoptosis)   (R-HSA-109581...)
```

This two-level hierarchy allows:
- **Cross-domain OxO relationships** (GxE, GxG, ExE) to be modeled uniformly via `EntityRelationship`
- **Domain-specific attribute tables** linked to Entity by `entity_id`
- **Flexible reclassification** — the domain or type of any entity can be updated in the table without schema changes

---

## EntityDomain

| id | name | description |
|----|------|-------------|
| 1 | Genomics | Entities derived from genome sequencing and molecular biology: genes, proteins, variants, transcripts, epigenomic marks |
| 2 | Exposome | Entities representing environmental, chemical, and clinical exposures and outcomes: chemicals, diseases, phenotypes, environmental exposures, metabolites |
| 3 | Knowledge | Biological knowledge structures that organize and connect other entities: pathways, ontologies, interaction networks |

---

## EntityType

| id | name | domain | initial sources | domain tables | notes |
|----|------|--------|-----------------|---------------|-------|
| 1 | Variants | Genomics | dbSNP, ClinVar | `VariantMasters`, `VariantMolecularEffects` | SNPs, InDels, CNVs, SVs |
| 2 | Genes | Genomics | HGNC, NCBI Gene | `GeneMaster`, `GeneGroup`, `EntityLocation` | Includes locus group and locus type (HGNC) |
| 3 | Proteins | Genomics | UniProt, Pfam | `ProteinMaster`, `ProteinPfam` | Canonical proteins and isoforms |
| 4 | Transcriptomics | Genomics | Ensembl, GTEx | *(Onda 2)* | RNA transcripts and expression profiles |
| 5 | Epigenomics | Genomics | ENCODE, Roadmap | *(Onda 2)* | Methylation, histone marks, chromatin states |
| 6 | Pathways | Knowledge | Reactome, KEGG | `PathwayMaster` | Molecular pathways and networks |
| 7 | Gene Ontology | Knowledge | GO Consortium | `GOMaster`, `GORelation` | MF, BP, CC terms |
| 8 | Diseases | Exposome | CTD, OMIM, MONDO | `DiseaseMaster`, `DiseaseGroup` | ICD10, MONDO, OMIM, Orphanet |
| 9 | Chemicals | Exposome | CTD, ChEBI, PubChem | `ChemicalMaster` | Drugs, toxins, environmental compounds (CAS, InChI, SMILES) |
| 10 | Phenotypes | Exposome | HPO, GWAS Catalog | `PhenotypeMaster` | Observable traits, HPO terms, EFO terms |
| 11 | Exposome | Exposome | NHANES, CDC | `ExposureMaster` | Measured environmental exposures; may link to a Chemical entity |
| 12 | Metabolomics | ❓ | HMDB, KEGG | *(Onda 2)* | **Open:** Genomics (it's an omic) or Exposome (metabolites are measured like exposures)? |
| 13 | Microbiome | ❓ | MGnify, HMP | *(Onda 2)* | **Open:** Genomics (16S sequencing) or Exposome (environmental modifier)? |

---

## Chemicals vs Exposome — Key Design Decision

### The central problem

When we think about "benzene in IGEM", two different questions arise:

1. **What is benzene?** → a chemical substance, formula C₆H₆, CAS 71-43-2, defined molecular structure
2. **How does benzene affect humans?** → measured in the urine of workers, in urban air, in ng/mL, via inhalation

The first question is about **chemical identity**. The second is about **human exposure**.
They are different things, and the problem is that we tend to conflate them.

---

### Chemical — substance identity

A Chemical exists independently of any context. Benzene is benzene in the laboratory,
in gasoline, and in cigarettes.

```
ChemicalMaster
├── cas_number:     71-43-2
├── formula:        C₆H₆
├── inchi_key:      UHOVQNZJYSORNB-UHFFFAOYSA-N
├── pubchem_cid:    241
├── ctd_id:         D001554   ← MeSH ID in CTD
└── chemical_class: aromatic hydrocarbon
```

**Primary sources:** CTD, ChEBI, PubChem

---

### Exposome — the measured encounter

An Exposure only exists when there is **who**, **how much**, **how**, and **where**.
It is not the substance itself — it is the record of an encounter between a substance
and a population, in the context of a study.

```
ExposureMaster
├── nhanes_code:        LBXBZF
├── exposure_type:      chemical
├── exposure_route:     inhalation
├── media:              blood
├── measurement_unit:   ng/mL
└── chemical_entity_id: → Entity(Benzene)  ← optional link to Chemical
```

**Primary sources:** NHANES, CDC, epidemiological studies

---

### Concrete cases that clarify the distinction

| Entity | Type | Why |
|--------|------|-----|
| Benzene (CAS 71-43-2) | Chemical | Substance with a defined molecular structure |
| Urinary benzene — NHANES 2017 | Exposome | Population measurement in a biological matrix (urine) |
| Lead (CAS 7439-92-1) | Chemical | Heavy metal, defined structure |
| Blood lead level (BPB) | Exposome | Epidemiological measurement of lead |
| Aspirin (acetylsalicylic acid) | Chemical | Drug with a defined structure |
| "Daily aspirin use" | Exposome | Clinical/lifestyle exposure — has no CAS number |
| PM2.5 (fine particulate matter) | **Exposome only** | A mixture without a single molecular structure — no CAS number |
| UV-B radiation | **Exposome only** | Physical factor — not a chemical substance |
| Sedentary behavior | **Exposome only** | Lifestyle factor — does not exist as a Chemical |

The last three cases are the critical point: **not every exposure is a Chemical**.
PM2.5, noise, heat, psychosocial stress, diet, and sedentary behavior exist only in
the Exposome domain, with no chemical counterpart. This is why the two types must
remain separate entities.

---

### How they connect in IGEM

```
CTD  →  Gene ──EntityRelationship──→ Chemical      (GxE: gene regulates/metabolizes benzene)
CTD  →  Chemical ─EntityRelationship─→ Disease     (ExE: benzene causes leukemia)

NHANES → Exposure(urinary benzene) → chemical_entity_id → Chemical(benzene)
NHANES → Exposure(PM2.5)           → chemical_entity_id → NULL  (mixture, no Chemical)
NHANES → Exposure(sedentary behav) → chemical_entity_id → NULL  (lifestyle)
```

The `chemical_entity_id` in `ExposureMaster` is optional precisely because many
exposures have no corresponding Chemical entity.

---

## OxO Relationship Coverage

The `EntityRelationship` table connects any two entities, enabling:

| Relationship | Domain pair | Example |
|---|---|---|
| GxG | Genomics × Genomics | Gene ↔ Gene (protein interaction) |
| GxE | Genomics × Exposome | Gene ↔ Disease (CTD), Gene ↔ Chemical (CTD) |
| ExE | Exposome × Exposome | Chemical ↔ Disease (CTD), Phenotype ↔ Disease |
| GxK | Genomics × Knowledge | Gene ↔ Pathway (Reactome), Gene ↔ GO term |
| ExK | Exposome × Knowledge | Disease ↔ Pathway |

---

## Open Questions for Team

1. **Microbiome** → Genomics or Exposome?
2. **Metabolomics** → Genomics or Exposome?
3. Should `EntityDomain` be a DB table (flexible, editable at runtime) or a code-level enum (stricter, requires migration to change)?
4. Any EntityType missing from the list above that we anticipate needing?
