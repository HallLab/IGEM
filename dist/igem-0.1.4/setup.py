# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['igem',
 'igem.epc',
 'igem.epc.clarite',
 'igem.epc.clarite.analyze',
 'igem.epc.clarite.describe',
 'igem.epc.clarite.load',
 'igem.epc.clarite.modify',
 'igem.epc.clarite.plot',
 'igem.epc.clarite.survey',
 'igem.epc.tests',
 'igem.epc.tests.test_analyze',
 'igem.epc.tests.test_describe',
 'igem.epc.tests.test_load',
 'igem.epc.tests.test_modify',
 'igem.epc.tests.test_plot',
 'igem.ge',
 'igem.ge.db',
 'igem.ge.filter',
 'igem.ge.forms',
 'igem.ge.management',
 'igem.ge.management.commands',
 'igem.ge.management.commands.olds',
 'igem.ge.migrations',
 'igem.ge.tests',
 'igem.ge.utils',
 'igem.ge.views',
 'igem.omics',
 'igem.omics.db',
 'igem.omics.management',
 'igem.omics.management.commands',
 'igem.omics.migrations',
 'igem.omics.tests',
 'igem.server',
 'igem.server.etl',
 'igem.server.management',
 'igem.server.management.commands',
 'igem.server.management.commands.olds',
 'igem.server.migrations',
 'igem.server.sql',
 'igem.server.tests',
 'igem.src']

package_data = \
{'': ['*'],
 'igem': ['.pytest_cache/*',
          '.pytest_cache/v/cache/*',
          'base_static/global/css/*',
          'base_static/global/js/*',
          'base_templates/global/*',
          'base_templates/global/partials/*',
          'psa/*'],
 'igem.epc.tests': ['py_test_output/*',
                    'r_test_output/*',
                    'r_test_output/analyze/*',
                    'r_test_output/interactions/*',
                    'test_data_files/*',
                    'test_data_files/nhanes_subset/*'],
 'igem.ge': ['templates/ge/pages/*', 'templates/ge/partials/*'],
 'igem.ge.db': ['.mypy_cache/*',
                '.mypy_cache/3.10/*',
                '.mypy_cache/3.10/_typeshed/*',
                '.mypy_cache/3.10/collections/*',
                '.mypy_cache/3.10/concurrent/*',
                '.mypy_cache/3.10/concurrent/futures/*',
                '.mypy_cache/3.10/ctypes/*',
                '.mypy_cache/3.10/email/*',
                '.mypy_cache/3.10/igem/epc/*',
                '.mypy_cache/3.10/igem/epc/clarite/*',
                '.mypy_cache/3.10/igem/epc/clarite/analyze/*',
                '.mypy_cache/3.10/igem/epc/clarite/describe/*',
                '.mypy_cache/3.10/igem/epc/clarite/load/*',
                '.mypy_cache/3.10/igem/epc/clarite/modify/*',
                '.mypy_cache/3.10/igem/epc/clarite/plot/*',
                '.mypy_cache/3.10/igem/epc/clarite/survey/*',
                '.mypy_cache/3.10/igem/ge/*',
                '.mypy_cache/3.10/igem/ge/db/*',
                '.mypy_cache/3.10/igem/ge/filter/*',
                '.mypy_cache/3.10/igem/server/*',
                '.mypy_cache/3.10/igem/server/etl/*',
                '.mypy_cache/3.10/igem/server/sql/*',
                '.mypy_cache/3.10/importlib/*',
                '.mypy_cache/3.10/importlib/metadata/*',
                '.mypy_cache/3.10/json/*',
                '.mypy_cache/3.10/logging/*',
                '.mypy_cache/3.10/multiprocessing/*',
                '.mypy_cache/3.10/numpy/*',
                '.mypy_cache/3.10/numpy/_typing/*',
                '.mypy_cache/3.10/numpy/compat/*',
                '.mypy_cache/3.10/numpy/core/*',
                '.mypy_cache/3.10/numpy/fft/*',
                '.mypy_cache/3.10/numpy/lib/*',
                '.mypy_cache/3.10/numpy/linalg/*',
                '.mypy_cache/3.10/numpy/ma/*',
                '.mypy_cache/3.10/numpy/matrixlib/*',
                '.mypy_cache/3.10/numpy/polynomial/*',
                '.mypy_cache/3.10/numpy/random/*',
                '.mypy_cache/3.10/numpy/testing/*',
                '.mypy_cache/3.10/numpy/testing/_private/*',
                '.mypy_cache/3.10/numpy/typing/*',
                '.mypy_cache/3.10/os/*',
                '.mypy_cache/3.10/unittest/*'],
 'igem.ge.tests': ['test_data_files/*',
                   'test_data_files/etl/*',
                   'test_data_files/results/*',
                   'test_data_files/sync/*'],
 'igem.server.tests': ['test_data_files/*', 'test_data_files/backup/*']}

install_requires = \
['clarite>=2.3.4',
 'django-thread>=0.0.1,<0.0.2',
 'django>=4.1.5,<5.0.0',
 'lxml>=4.9.2,<5.0.0',
 'mypy>=0.991,<0.992',
 'patool>=1.12,<2.0',
 'psycopg2>=2.9.5,<3.0.0',
 'requests>=2.28.2,<3.0.0',
 'scikit-learn>=1.2.2,<2.0.0',
 'types-requests>=2.28.11.8,<3.0.0.0']

setup_kwargs = {
    'name': 'igem',
    'version': '0.1.4',
    'description': '',
    'long_description': "\nIGEM - Integrative Genome-Exposome Method\n=========================================\n\nAn Architecture for Efficient Bioinformatics Analysis\n-----------------------------------------------------\n\n\nAbstract:\nIGEM software is a robust and scalable architecture designed for bioinformatics analysis. IGEM incorporates various modules that seamlessly work together to enable efficient data processing, analysis, and visualization. This paper explores the architecture of IGEM, including its core components, the two versions available (Server and Client), the ETL (Extraction, Transformation, and Loading) process, term replacement techniques, and the utilization of master data. Additionally, it highlights the powerful analysis functions offered by IGEM, such as dataset loading, quality control functionalities, and association and interaction analyses. The flexibility and capabilities of IGEM make it a valuable tool for researchers and practitioners in the field of omics research.\n\n1. Introduction\nThe IGEM software provides a comprehensive suite of tools for bioinformatics analysis. Its architecture is built upon a scalable and efficient framework that supports the integration and analysis of diverse omics datasets. In this paper, we delve into the various aspects of the IGEM architecture, highlighting its key components, functionalities, and advantages.\n\n2. IGEM Architecture\nThe architecture of IGEM revolves around its core modules, which enable seamless data processing, analysis, and visualization. At the heart of IGEM lies the GE-db, a multi-database that serves as the foundation of the knowledge base. This knowledge base is vital for conducting meaningful analyses and extracting valuable insights from external sources.\n\n3. IGEM Versions: Server and Client\nTo cater to different user needs, IGEM is available in two distinct versions: the IGEM Server and the IGEM Client. The IGEM Server version provides a comprehensive suite of tools for handling large-scale omics data and performing advanced analytics. On the other hand, the IGEM Client version offers a streamlined and lightweight experience, suitable for individual researchers or smaller teams focusing on specific analyses.\n\n4. ETL Process: Collect, Prepare, Map, Reduce\nThe ETL (Extraction, Transformation, and Loading) process is a crucial component of IGEM, ensuring the acquisition and preparation of data for analysis. The ETL process consists of four steps: collect, prepare, map, and reduce. In the collect step, active datasets are selected and the latest data is extracted and stored. The prepare step transforms the data into a well-structured format, while the map step establishes relationships between terms. Finally, the reduce step identifies and records terms per line, ensuring accurate and up-to-date information is stored.\n\n5. Replacing Terms: Pre-computed Mapping and IGEM Search Engine\nTo ensure consistency and accuracy in the data, IGEM employs a pre-computed term mapping approach combined with a powerful search engine. Prior to the ETL process, a mapping table is created, associating different variations and synonyms of terms with their standardized counterparts. During the term replacement step, IGEM's search engine matches terms in the data with their standardized form, ensuring coherence and alignment within the dataset.\n\n6. IGEM Master Data\nIGEM utilizes master data entries to effectively configure and manage the integration of external datasets. These entries provide essential information about each dataset, including unique identifiers, database details, field-level parameters, and hierarchical relationships among terms. Configuring field-level parameters ensures accurate interpretation of data, while establishing term hierarchies enhances organization and accessibility.\n\n7. Analysis Functions: Server and Client Versions\nBoth the IGEM Server and Client versions offer a range of analysis functions to enhance the software's capabilities. Users can load datasets, apply quality control processes, and perform association and interaction analyses. Association analysis allows users to explore relationships between variables, while interaction analysis focuses on ExE and GxE interactions. Pairwise analysis further refines the investigation of specific pairs exhibiting\nsignificant interactions.\n\n8. Conclusion\nThe IGEM software provides a robust and scalable architecture for efficient bioinformatics analysis. Its modular design, flexible functionality, and powerful analysis capabilities make it a valuable tool for researchers and practitioners in the field. By leveraging the IGEM architecture, users can seamlessly integrate omics datasets, perform comprehensive analyses, and gain valuable insights into biological systems. Further advancements and enhancements to the IGEM software will continue to propel bioinformatics research forward, driving discoveries and breakthroughs in the field of omics research.\n\n\nQuestions\n---------\n\nfeel free to open an `Issue <https://github.com/HallLab/igem/issues>`_.\n\nCiting IGEM\n--------------\n\n\nhttps://igem.readthedocs.io/en/latest/",
    'author': 'Andre Rico',
    'author_email': '97684721+AndreRicoPSU@users.noreply.github.com',
    'maintainer': 'None',
    'maintainer_email': 'None',
    'url': 'None',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.8.0,<3.11.0',
}


setup(**setup_kwargs)
