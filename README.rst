FLEXvalue (TM)
*********

This library can be used to calculate the avoided costs, TRC, and PAC.

Try it out for yourself by following our `tutorial <https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/tutorial.ipynb>`_ or with your own data `here <https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab.ipynb>`_.

Avoided Cost Data
#################

This uses avoided cost data that is stored in a SQLITE table, which can be
downloaded as a SQLite file `here (flexvalue_2020.db) <https://storage.googleapis.com/flexvalue-public-resources/2020.db>`_.

A separate series of pythons scripts were used to generate that sqlite file from a source XLSX file provided by the `CEC <https://www.cpuc.ca.gov/general.aspx?id=5267>`_ As of this writing (2021-03-05), the most recent update to the avoided cost data is 2020, which corresponds to the public filename of the SQLite file. 

Installation from Source
########################

Docker
------

.. code-block:: shell

  docker-compose build

  # for running the CLI commands
  ./flexvalue.sh --help

  # for opening the tutorial
  docker-compose up jupyter

Local
-----

.. code-block:: shell
  
  pip install -e .

  # for running cli commands
  flexvalue --help

  # tutorial (assuming you have jupyter installed
  jupyter notebooks/

CLI Commands
############

If you are calling these commands using the repo code and docker, replace `flexvalue` with `./flexvalue.sh`.

Before calculating any results, you will need to download the avoided cost data for a given year. 

.. code-block:: shell

    flexvalue download-avoided-costs-data-db --year 2020

To get an example set of FLEXvalue (TM) results, run the following commands in order.

.. code-block:: shell

    flexvalue generate-example-inputs
    flexvalue get-results --user-inputs-filepath test_data/example_user_inputs_deer.csv --report-filepath reports/example_report_deer.html
    flexvalue get-results --user-inputs-filepath test_data/example_user_inputs_metered.csv  --metered-load-shape-filepath ../test_data/example_metered_load_shape.csv --report-filepath reports/example_report_metered.html

To help generate your user input file, use the following command to see what utilities, climate zones, and deer load shapes are available.

.. code-block:: shell

    flexvalue valid-utility-climate-zone-combos
    flexvalue valid-deer-load-shapes

License
#######

This project is licensed under `Apache 2.0 <LICENSE.md>`_.
