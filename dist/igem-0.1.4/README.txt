
IGEM - Integrative Genome-Exposome Method
=========================================

An Architecture for Efficient Bioinformatics Analysis
-----------------------------------------------------


Abstract:
IGEM software is a robust and scalable architecture designed for bioinformatics analysis. IGEM incorporates various modules that seamlessly work together to enable efficient data processing, analysis, and visualization. This paper explores the architecture of IGEM, including its core components, the two versions available (Server and Client), the ETL (Extraction, Transformation, and Loading) process, term replacement techniques, and the utilization of master data. Additionally, it highlights the powerful analysis functions offered by IGEM, such as dataset loading, quality control functionalities, and association and interaction analyses. The flexibility and capabilities of IGEM make it a valuable tool for researchers and practitioners in the field of omics research.

1. Introduction
The IGEM software provides a comprehensive suite of tools for bioinformatics analysis. Its architecture is built upon a scalable and efficient framework that supports the integration and analysis of diverse omics datasets. In this paper, we delve into the various aspects of the IGEM architecture, highlighting its key components, functionalities, and advantages.

2. IGEM Architecture
The architecture of IGEM revolves around its core modules, which enable seamless data processing, analysis, and visualization. At the heart of IGEM lies the GE-db, a multi-database that serves as the foundation of the knowledge base. This knowledge base is vital for conducting meaningful analyses and extracting valuable insights from external sources.

3. IGEM Versions: Server and Client
To cater to different user needs, IGEM is available in two distinct versions: the IGEM Server and the IGEM Client. The IGEM Server version provides a comprehensive suite of tools for handling large-scale omics data and performing advanced analytics. On the other hand, the IGEM Client version offers a streamlined and lightweight experience, suitable for individual researchers or smaller teams focusing on specific analyses.

4. ETL Process: Collect, Prepare, Map, Reduce
The ETL (Extraction, Transformation, and Loading) process is a crucial component of IGEM, ensuring the acquisition and preparation of data for analysis. The ETL process consists of four steps: collect, prepare, map, and reduce. In the collect step, active datasets are selected and the latest data is extracted and stored. The prepare step transforms the data into a well-structured format, while the map step establishes relationships between terms. Finally, the reduce step identifies and records terms per line, ensuring accurate and up-to-date information is stored.

5. Replacing Terms: Pre-computed Mapping and IGEM Search Engine
To ensure consistency and accuracy in the data, IGEM employs a pre-computed term mapping approach combined with a powerful search engine. Prior to the ETL process, a mapping table is created, associating different variations and synonyms of terms with their standardized counterparts. During the term replacement step, IGEM's search engine matches terms in the data with their standardized form, ensuring coherence and alignment within the dataset.

6. IGEM Master Data
IGEM utilizes master data entries to effectively configure and manage the integration of external datasets. These entries provide essential information about each dataset, including unique identifiers, database details, field-level parameters, and hierarchical relationships among terms. Configuring field-level parameters ensures accurate interpretation of data, while establishing term hierarchies enhances organization and accessibility.

7. Analysis Functions: Server and Client Versions
Both the IGEM Server and Client versions offer a range of analysis functions to enhance the software's capabilities. Users can load datasets, apply quality control processes, and perform association and interaction analyses. Association analysis allows users to explore relationships between variables, while interaction analysis focuses on ExE and GxE interactions. Pairwise analysis further refines the investigation of specific pairs exhibiting
significant interactions.

8. Conclusion
The IGEM software provides a robust and scalable architecture for efficient bioinformatics analysis. Its modular design, flexible functionality, and powerful analysis capabilities make it a valuable tool for researchers and practitioners in the field. By leveraging the IGEM architecture, users can seamlessly integrate omics datasets, perform comprehensive analyses, and gain valuable insights into biological systems. Further advancements and enhancements to the IGEM software will continue to propel bioinformatics research forward, driving discoveries and breakthroughs in the field of omics research.


Questions
---------

feel free to open an `Issue <https://github.com/HallLab/igem/issues>`_.

Citing IGEM
--------------


https://igem.readthedocs.io/en/latest/