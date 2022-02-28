FLEXvalue™ 
**************

*DISCLAIMER*: We are currently in the process of incorporating and testing the 2021 ACC. Please continue to use 2020 or adjusted_acc_map (for the 2020 smoothed/scaled kicker adjusted ACC) until we provide further instruction.

`Read The Docs Link <https://recurve-analytics-inc-flexvalue.readthedocs-hosted.com/en/latest/>`_

`Read The Docs PDF Link <https://recurve-analytics-inc-flexvalue.readthedocs-hosted.com/_/downloads/en/latest/pdf/>`_

`Github Link <https://github.com/recurve-methods/flexvalue>`_

.. image:: https://readthedocs.com/projects/recurve-analytics-inc-flexvalue/badge/?version=latest&token=03dc3e4930d430d47b5d1169ec38ad7df5d2bc70f69689d1e845b56596bcf590
    :target: https://recurve-analytics-inc-flexvalue.readthedocs-hosted.com/en/latest/?badge=latest
    :alt: Documentation Status

This library provides California aggregators, program administrators, utilities, and regulators a pathway to consistently and transparently gauge the value of their projects, portfolios, and programs. FLEXvalue uses the CPUC’s published avoided cost data to enable market actors across the state to assess demand flexibility value from either pre-defined or custom/measured load shapes. FLEXvalue accepts user-defined 8,760 hourly savings profiles or deemed load shapes that are part of the Database for Energy Efficiency Resources (DEER). See the user_inputs section below for more information. FLEXvalue currently computes Total Resource Cost (TRC) and Program Administrator Cost (PAC) test results. See the `California Standard Practice Manual <https://www.cpuc.ca.gov/uploadedFiles/CPUC_Public_Website/Content/Utilities_and_Industries/Energy_-_Electricity_and_Natural_Gas/CPUC_STANDARD_PRACTICE_MANUAL.pdf>`_ for more information on cost-effectiveness tests. 

The gas and electric avoided cost data and the DEER load shapes that FLEXvalue draws from is stored in a SQLITE table, which can be downloaded as a SQLite file here: `(2020.db) <https://storage.googleapis.com/flexvalue-public-resources/db/v1/2020.db>`_, `(adjusted_acc_map.db) <https://storage.googleapis.com/flexvalue-public-resources/db/v1/adjusted_acc_map.db>`_, `(2021.db) <https://storage.googleapis.com/flexvalue-public-resources/db/v1/2021.db>`_. 

Use `this colab notebook <https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab_2020_2021_compare.ipynb>`_ to see some preliminary results comparing the 2020 and 2021 avoided cost data.

Use `this colab notebook <https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab.ipynb>`_ which allows you to run FLEXvalue directly. 

Want to compare FLEXvalue results to the CET tool? `This notebook <https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab_cet_scan_compare.ipynb>`_ allows you to directly compare results. 


For users who would like to instead download .csv files or who would like to directly explore these data see this `notebook <https://colab.research.google.com/github/recurve-methods/flexvalue/blob/master/notebooks/colab_database_explorer.ipynb>`_.

For those looking to learn more about how to use this library locally or via the python library, the following `tutorial <https://nbviewer.jupyter.org/github/recurve-methods/flexvalue/blob/main/notebooks/tutorial.ipynb>`_. 

Avoided Cost Data
#################

A separate series of pythons scripts were used to generate that sqlite file from a source XLSX file available through the `CPUC's website <https://www.cpuc.ca.gov/general.aspx?id=5267>`_. As of this writing (2022-02-28), the most recent update to the avoided cost data is adjusted_acc_map, which corresponds to the public filename of the SQLite file. 

deer_load_shapes
----------------

The DEER load shapes are normalized 8,760 hourly savings profiles that correspond to different end-use sectors and technologies (residential HVAC or commercial lighting for example). A full list is provided in reference . Annual deemed MWh savings values are typically assigned to a specific DEER load shape. The resulting 8,760 hourly savings values are then multiplied by the hourly electric avoided costs to produce the electric cost effectiveness benefits.

Recurve has made a few changes to column names to incorporate residential and commercial naming conventions but otherwise the format of the DEER load shapes does not need to be updated. Additional formatting is needed because the electric avoided costs begin on a day of the week that does not align with the DEER load shapes. Recurve has conducted extensive testing and has found that the DEER load shapes need to be shifted by -2 days in order to provide the best alignment with the CPUC’s existing Cost Effectiveness Tool.

There are different versions of the DEER load shapes for each of the four California IOUs. The specific DEER load shape selected will depend on the combination of user input program administrator and DEER load shape name. It is not known at this point if a different shift will need to be incorporated upon release of the next electric avoided cost calculator.

acc_electricity
---------------

The electric avoided cost calculator compiles hourly marginal utility avoided costs for electric savings. Costs are provided for ten different cost components and are projected forward through 2050 for the non-adjusted acc versions and through 2043 for the kicker adjusted one. Avoided costs are distinct for each utility service territory (PG&E, SCE, SDG&E, and SoCalGas) and Climate Zone combination. The electric avoided cost calculator also contains hourly marginal greenhouse gas emissions data, which are also forecasted to 2050.

The electric avoided cost calculator can be downloaded as a .xlsb file from `here <https://www.cpuc.ca.gov/General.aspx?id=5267>`_.

The electric avoided cost calculator is a macro-driven Excel file, so several data extraction and transformation steps were done to combine all calculator results into a single table.

acc_gas
-------

The gas avoided cost calculator compiles monthly marginal utility avoided costs for gas savings. Costs are provided for four different components and are projected forward through 2050. Avoided costs are distinct for each utility service territory (PG&E, SCE, SDG&E, and SoCalGas). Avoided costs are also somewhat different for distinct end use categories.

The gas avoided cost calculator can be downloaded as a .xlsb file from `here <https://www.cpuc.ca.gov/General.aspx?id=5267>`_.

The gas avoided cost calculator is a macro-driven Excel file, so several data extraction and transformation steps were done to combine all calculator results into a single table.

*Currently the gas avoided costs data uses the same year-month avoided costs (using Utility: PGE, Class: Total Core, End Use: Small Boiler, Emission Control: Uncontrolled) for all analysis. Based on testing against the CET, different numerical factors are applied depending on the program administrator. Future work can incorporate the full suite of gas parameters.*


Inputs
######

There is one required input that is referred to as the `user_inputs`, and another optional `metered_load_shape` input. These inputs are CSV files if using the command line, and pandas dataframes if directly calling from python. 

user_inputs
-----------

Recurve has prepared an example `user_inputs file <https://storage.googleapis.com/flexvalue-public-resources/examples/example_user_inputs_metered.csv>`_ that can be downloaded and run. 

The `user_inputs` CSV requires the following columns:

    - **ID**: A unique identifier used to reference this measure, project, or portfolio
    - **load_shape**: the name of the load shape to use (either referencing a column in the `metered_load_shape` file or an available DEER load shape)
    - **start_year**: The year to start with when using avoided costs data
    - **start_quarter**: The quarter to start with when using avoided costs data
    - **utility**: Which uility to filter by when loading avoided costs data
    - **climate_zone**: Which climate zone to filter by when loading avoided costs data
    - **units**: Multiplier of the therms_savings and mwh_savings (this multiplier is not applied to any of the costs)
    - **eul**: Effective Useful Life (EUL): the expected duration in years that the load impacts persist
    - **ntg**: Net-to-gross ratio
    - **discount_rate**: The quarterly discount rate to be applied within the net present value calculation
    - **admin**: The administrative costs assigned to the given measure, project, or portfolio
    - **measure**: The measure costs assigned to the given measure, project, or portfolio
    - **incentive**: The incentive costs assigned to the given measure, project, or portfolio
    - **therms_profile**: Indicates the season in which therms savings are achieved, can be one of ['annual', 'summer', 'winter']
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

Metered Load Shapes
------
If the user-defined load shape is normalized (the sum of values across all 8,760 hours is 1) then the user should input the annual MWh savings value in the user_inputs file. If the user-defined load shape is not normalized (the sum of values across all 8,760 hours equals the annual MWh savings) the user should enter 1 in for the corresponding MWh savings in the user_inputs file. 

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

Before calculating any results, you will need to download the avoided cost data for a given version. By default, this downloads to a folder `$DATABASE_LOCATION/{version}.db`. If you do not set the environment variable `DATABASE_LOCATION`, it will default to `DATABASE_LOCATION=.`.

.. code-block:: shell

    flexvalue download-avoided-costs-data-db --version 2020

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
- `CODE_OF_CONDUCT <https://github.com/recurve-methods/flexvalue/blob/main/CODE_OF_CONDUCT.md>`_: Code of conduct for contributors
