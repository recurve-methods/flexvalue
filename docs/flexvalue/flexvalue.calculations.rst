==========================
``flexvalue.calculations``
==========================

.. automodule:: flexvalue.calculations

   .. contents::
      :local:

.. currentmodule:: flexvalue.calculations


Functions
=========

- :py:func:`get_quarterly_discount_df`:
  Calculates the quarterly discount factor for the duration of the EUL

- :py:func:`calculate_trc_costs`:
  Calculates the TRC costs

- :py:func:`calculate_pac_costs`:
  Calculate PAC costs


.. autofunction:: get_quarterly_discount_df

.. autofunction:: calculate_trc_costs

.. autofunction:: calculate_pac_costs


Classes
=======

- :py:class:`FlexValueProject`:
  Representation of the parameters and calculations for a single project

- :py:class:`FlexValueRun`:
  Representation of a single calculation for a set of projects


.. autoclass:: FlexValueProject
   :members:

   .. rubric:: Inheritance
   .. inheritance-diagram:: FlexValueProject
      :parts: 1

.. autoclass:: FlexValueRun
   :members:

   .. rubric:: Inheritance
   .. inheritance-diagram:: FlexValueRun
      :parts: 1
