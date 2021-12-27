from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from Data_Pipeline.plugins import operators
from Data_Pipeline.plugins import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
