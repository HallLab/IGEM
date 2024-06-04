===============
EPC Application
===============


The EPC (Extend Process Call) module in the IGEM software provides a comprehensive set of functionalities that enable users to create an end-to-end pipeline for data analysis. 
This module offers various tools and functions to load external datasets, perform data description, and modify the data to adapt it to different types of analyses such as EWAS, Association Study, and ExE Pairwise analysis. 
Here is an overview of the key functionalities offered by the EPC module:

Loading External Datasets
-------------------------
Allows users to seamlessly load external datasets into the script. 
It supports loading data from CSV and TSV files. This functionality enables researchers to integrate their data with the IGEM ecosystem for further analysis.

Data Description
----------------
Users can obtain a comprehensive description of their datasets. This includes calculating correlations between variables, generating frequency tables for categorical variables, determining data types of variables, calculating the percentage of missing values, computing skewness of variables, and generating summary statistics for variables. 
These descriptive statistics provide valuable insights into the dataset and help researchers understand its characteristics.

Data Modification
-----------------
Offers a wide range of data modification functions to prepare the dataset for specific analyses. 
Users can categorize variables based on defined criteria, filter columns based on specific conditions, convert variables to binary or categorical format, merge observations or variables based on specified conditions, move variables within the dataset, record specific values for variables, remove outliers, filter rows with incomplete observations, and perform transformations on variables. 
These data modification functions enable researchers to tailor the dataset to their analysis requirements.

Data Analysis
-------------
The EPC module includes functionalities specifically designed for conducting:
   * Environment-Wide Association Studies (EWAS). Researchers can leverage these functions to analyze the association between epigenetic modifications and phenotypic traits. The EPC module provides dedicated tools to perform statistical tests, correct p-values, and generate graphical representations such as Manhattan plots.
   * Association Study by providing tools to analyze the relationships between variables in the dataset. Users can perform association tests and explore the strength and significance of associations between variables. This functionality is particularly useful for identifying potential relationships and dependencies within the data.
   * ExE (Exposure by Exposure) Pairwise analysis, allowing researchers to examine the pairwise relationships between exposures. By applying this analysis, users can identify potential interactions or dependencies between different exposures in the dataset.

Survey Design and Modeling
--------------------------
Users can define survey designs with specific sampling strategies and create survey models for analyzing survey data. 
These features cater to researchers working with survey datasets and provide specialized tools for accurate analysis.

Plot Functions
--------------
The EPC module provides various plot functions to visualize the data and gain deeper insights. These plot functions include:

   * Distributions: Generate visual representations of variable distributions, such as histograms and kernel density plots. These plots help researchers understand the underlying distribution of variables in the dataset.
   * Histograms: Create histograms to visualize the distribution of a single variable. This plot provides a visual summary of the frequency distribution of values in the dataset.
   * Manhattan Plot: Generate a Manhattan plot, commonly used in genetic association studies, to visualize the genomic location of associations. This plot displays the significance of associations along the genome.
   * Manhattan Plot with Bonferroni Correction: Similar to the Manhattan plot, this function incorporates Bonferroni correction to account for multiple hypothesis testing. It helps identify significant associations while controlling for the family-wise error rate.
   * Manhattan Plot with False Discovery Rate (FDR): This function applies the False Discovery Rate (FDR) correction to the associations in the Manhattan plot. It allows researchers to control the expected proportion of false discoveries while identifying significant associations.
   * Top Results Plot: Create a plot displaying the top results of an analysis, such as the most significant associations or the highest-ranked variables. This plot helps researchers focus on the most important findings in the data.

By utilizing the functionalities offered by the EPC module, users can create a streamlined and comprehensive pipeline for data analysis within the IGEM software. This module empowers researchers to load external datasets, describe the data, modify it to suit specific analyses, and perform advanced statistical tests and visualizations.


.. toctree::

   load
   analyze
   describe
   modify
   plot
   survey
