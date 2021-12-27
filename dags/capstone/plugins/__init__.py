from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import capstone.operators
import capstone.helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator1,
        operators.LoadFactOperator1,
        operators.LoadDimensionOperator1,
        operators.DataQualityOperator1
    ]
    helpers = [
        helpers.SqlQueries1
    ]
