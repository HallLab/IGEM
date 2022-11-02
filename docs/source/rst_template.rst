============
Installation
============
Primeiro nivel

Basic Install
-------------
segundo nivel

Troubleshooting rpy2
^^^^^^^^^^^^^^^^^^^^
terceiro nivel que fica fechado com +

Single Clusters
~~~~~~~~~~~~~~~
texto um pouco diferente do terceiro nivel


Linha de comando::
    $ pip install xyz


italico: *texto em italico*
italico para code:  `Rscript -e 'install.packages("survey")'


Bloco de codigo:
.. code-block:: python

    import os
    os.environ["R_HOME"] = r"C:\Program Files\R\R-4.0.2"
    os.environ["PATH"]   = r"C:\Program Files\R\R-4.0.2\bin\x64" + ";" + os.environ["PATH"]


.. code-block:: none


Texto com link externo:
1.
Lucas AM, et al (2019)
`CLARITE facilitates the quality control and analysis process for EWAS of metabolic-related traits. <https://www.frontiersin.org/article/10.3389/fgene.2019.01240>`_
*Frontiers in Genetics*: 10, 1240



Titulo com texto abaixo
Analyze
  Functions related to calculating association study results



Texto com link e bloco de linha de comando:
2. Using the :ref:`command line tool <cli:CLI Reference>`

.. code-block:: bash

   clarite-cli load from_tsv data/nhanes.txt results/data.txt --index SEQN
   cd results
   clarite-cli modify colfilter-min-n data data_filtered -n 250
   clarite-cli modify rowfilter-incomplete-obs data_filtered data_filtered_complete
   clarite-cli plot distributions data_filtered_complete plots.pdf


Link:
3. `Using the GUI`_

.. _`Using the GUI`: https://clarite-gui.readthedocs.io/en/stable



temos o automodule (ver em Regression Clarite)


Destacar um comando na linha 
The :code:`--help` option will show documentation



Criar uma nova estrutura, 1. criar uma pasta r adiconar um arquivo index: dentro com toctree adiconar os demais arquivos

