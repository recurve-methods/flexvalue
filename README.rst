FLEXvalue™ 
**************

`Read The Docs Link <https://recurve-analytics-inc-flexvalue.readthedocs-hosted.com/en/latest/>`_

.. image:: https://readthedocs.com/projects/recurve-analytics-inc-flexvalue/badge/?version=latest&token=03dc3e4930d430d47b5d1169ec38ad7df5d2bc70f69689d1e845b56596bcf590
    :target: https://recurve-analytics-inc-flexvalue.readthedocs-hosted.com/en/latest/?badge=latest
    :alt: Documentation Status

This library provides California aggregators, program administrators, utilities, and regulators a pathway to consistently and transparently gauge the value of their projects, portfolios, and programs. This uses the CPUC’s published avoided cost data to enable market actors across the state to assess demand flexibility value from either pre-defined or custom/measured load shapes.

Use the following colab notebook which allows you to put in your own inputs and get results: 

.. image:: https://colab.research.google.com/assets/colab-badge.svg
    :target: https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab.ipynb

Want to compare our results to the CET tool? This notebook allows you to compare results: 

.. image:: https://colab.research.google.com/assets/colab-badge.svg
    :target: https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab_cet_scan_compare.ipynb

You can also check out our full tutorial: 

.. image:: https://colab.research.google.com/assets/colab-badge.svg
    :target: https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/tutorial.ipynb

Avoided Cost Data
#################

This uses avoided cost data that is stored in a SQLITE table, which can be
downloaded as a SQLite file `here (flexvalue_2020.db) <https://storage.googleapis.com/flexvalue-public-resources/db/v1/2020.db>`_.

A separate series of pythons scripts were used to generate that sqlite file from a source XLSX file provided by the `CPUC <https://www.cpuc.ca.gov/general.aspx?id=5267>`_. As of this writing (2021-03-05), the most recent update to the avoided cost data is 2020, which corresponds to the public filename of the SQLite file. 

deer_load_shapes
----------------

The DEER load shapes are normalized 8,760 hourly savings profiles that correspond to different end-use sectors and technologies (residential HVAC or commercial lighting for example). A full list is provided in reference . Annual deemed kWh savings values are typically assigned to a specific DEER load shape. The resulting 8,760 hourly savings values are then multiplied by the hourly electric avoided costs to produce the electric cost effectiveness benefits.

Recurve has made a few changes to column names to incorporate residential and commercial naming conventions but otherwise the format of the DEER load shapes does not need to be updated. Additional ETL is needed because the electric avoided costs begin on a day of the week that does not align with the DEER load shapes. Recurve has conducted extensive testing and has found that the DEER load shapes need to be shifted by -2 days in order to provide the best alignment with the CPUC’s existing Cost Effectiveness Tool.

There are different versions of the DEER load shape files for each of the four California IOUs. At this point Recurve have not explicitly tested these different versions to gauge any differences. Recurve is currently using  the PG&E version. This database may need to be updated to incorporate each version of the DEER load shapes. It is also not known at this point if a different shift will need to be incorporated upon release of the next electric avoided cost calculator.

acc_electricity
---------------

The electric avoided cost calculator compiles hourly marginal utility avoided costs for electric savings. Costs are provided for ten different components and are projected forward through 2050. Avoided costs are distinct for each utility service territory (PG&E, SCE, SDG&E, and SoCalGas) and Climate Zone combination. The electric avoided cost calculator also contains hourly marginal greenhouse gas emissions data, which are also forecasted to 2050.

The electric avoided cost calculator can be downloaded as a .xlsb file from `here <https://www.cpuc.ca.gov/General.aspx?id=5267)>`_.

The electric avoided cost calculator is a macro-driven Excel file, so several ETL steps were done to combine all calculator results into a single table.

acc_gas
-------

The gas avoided cost calculator compiles monthly marginal utility avoided costs for gas savings. Costs are provided for four different components and are projected forward through 2050. Avoided costs are distinct for each utility service territory (PG&E, SCE, SDG&E, and SoCalGas). Avoided costs are also somewhat different for distinct end use categories.

The gas avoided cost calculator can be downloaded as a .xlsb file from `here <https://www.cpuc.ca.gov/General.aspx?id=5267)>`_.

The gas avoided cost calculator is a macro-driven Excel file, so several ETL steps were done to combine all calculator results into a single table.

*Currently the gas avoided costs data uses the same year-month avoided costs (using Utility: PGE, Class: Total Core, End Use: Small Boiler, Emission Control: Uncontrolled) for all analysis. Future work will bring in the other gas avoided cost data so that more accurate costs can be provided.*


Inputs
######

There is one required input that is referred to as the `user_inputs`, and another optional `metered_load_shape` input. These inputs are CSV files if using the command line, and pandas dataframes if directly calling from python. 

user_inputs
-----------

The `user_inputs` CSV requires the following columns:

    - **ID**: A unique identifier used to reference this measure, project, or portfolio
    - **load_shape**: the name of the load shape to use (either referencing a column in the `metered_load_shape` file or an available DEER load shape)
    - **start_year**: The year to start with when using avoided costs data
    - **start_quarter**: The quarter to start with when using avoided costs data
    - **utility**: Which uility to filter by when loading avoided costs data
    - **climate_zone**: Which climate zone to filter by when loading avoided costs data
    - **units**: Multiplier of the therms_savings and mwh_savings
    - **eul**: Effective Useful Life (EUL) means the average time over which an energy efficiency measure results in energy savings, including the effects of equipment failure, removal, and cessation of use.
    - **ntg**: Net to gross ratio
    - **discount_rate**: The quarterly discount rate to be applied to the net present value calculation
    - **admin**: The administrative costs assigned to a given measure, project, or portfolio
    - **measure**: The measure costs assigned to given measure, project, or portfolio
    - **inecentive**: The incentive costs assigned to given measure, project, or portfolio
    - **therms_profile**: Indicates what sort of adjustment to make on the therms savings, can be one of ['annual', 'summer', 'winter']
    - **therms_savings**: The first year gas gross savings in Therms
    - **mwh_savings**: The first year electricity gross savings in MWh (used to scale the load shape savings data if using custom load shape)

and looks like the following format:

.. list-table:: user_inputs
    :header-rows: 1

    * - ID
      - load_shape
      - start_year
      - start_quarter
      - utility
      - climate_zone
      - ...
    * - meter_id1
      - meter_id1
      - 2021
      - 1
      - PGE
      - CZ1
      - ...
    * - meter_id2
      - meter_id2
      - 2021
      - 1
      - PGE
      - CZ1
      - ...
    * - ...
      - ...
      - ...
      - ...
      - ...
      - ...
      - ...
    * - meter_id_n
      - meter_id_n
      - 2021
      - 1
      - PGE
      - CZ1
      - ...

metered_load_shape
------------------

The `metered_load_shape` CSV requires the following columns:

    - **hour_of_year**: Hour of the year (should be one row for each of 0-8759)
    - **meter_id1**: the savings values (in MWh), with the column name as a reference in the `load_shape` column of the `user_inputs` table (if that measure/project/portfolio has an electricity savings profile associated with meter_id1
    - **meter_id2**: the savings values (in MWh), with the column name as a reference in the `load_shape` column of the `user_inputs` table (if that measure/project/portfolio has an electricity savings profile associated with meter_id2
    - ...
    - **meter_id_n**: the savings values (in MWh), with the column name as a reference in the `load_shape` column of the `user_inputs` table (if that measure/project/portfolio has an electricity savings profile associated with meter_id_n


and looks like the following format:

.. list-table:: metered_load_shape
    :header-rows: 1

    * - hour_of_year
      - meter_id1
      - meter_id2
      - ...
      - meter_id_n
    * - 0
      - .15
      - .001
      - ...
      - .23
    * - 1
      - .15
      - .001
      - ...
      - .23
    * - ...
      - ...
      - ...
      - ...
      - ...
    * - 8759
      - 0.1
      - 0.35
      - 0.3
      - 0.2


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

  # tutorial (assuming you have jupyter installed)
  jupyter notebooks/

CLI Commands
############

If you are calling these commands using the repo code and docker, replace `flexvalue` with `./flexvalue.sh`.

Before calculating any results, you will need to download the avoided cost data for a given year. By default, this downloads to a folder `$DATABASE_LOCATION/{year}.db`. If you do not set the environment variable `DATABASE_LOCATION`, it will default to `DATABASE_LOCATION=.`.

.. code-block:: shell

    flexvalue download-avoided-costs-data-db --year 2020

To get an example set of FLEXvalue™ results, run the following commands in order.

.. code-block:: shell

    flexvalue generate-example-inputs
    flexvalue get-results --user-inputs-filepath example_user_inputs_deer.csv --report-filepath reports/example_report_deer.html
    flexvalue get-results --user-inputs-filepath example_user_inputs_metered.csv  --metered-load-shape-filepath example_metered_load_shape.csv --report-filepath reports/example_report_metered.html

To help generate your user input file, use the following command to see what utilities, climate zones, and deer load shapes are available.

.. code-block:: shell

    flexvalue valid-utility-climate-zone-combos
    flexvalue valid-deer-load-shapes

License
#######

This project is licensed under `Apache 2.0 <https://github.com/recurve-methods/flexvalue/blob/main/LICENSE.md>`_.

Other resources
---------------

- `MAINTAINERS <https://github.com/recurve-methods/flexvalue/blob/main/MAINTAINERS.md>`_: an ordered list of project maintainers.
- `CHARTER <https://github.com/recurve-methods/flexvalue/blob/main/CHARTER.md>`_: open source project charter.
- `CODE_OF_CONDUCT <https://github.com/recurve-methods/flexvalue/blob/main/CODE_OF_CONDUCT.md>`_: Code of ocnduct for contributors
