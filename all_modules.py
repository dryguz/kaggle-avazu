import os

from tfx.utils.dsl_utils import csv_input
from tfx.components.example_gen.csv_example_gen.component import CsvExampleGen
from tfx.components.statistics_gen.component import StatisticsGen
from tfx.components.schema_gen.component import SchemaGen

base_dir = os.getcwd()

# ------------------------------------------------------------------

examples = csv_input(os.path.join(base_dir, 'data/train.csv'))
example_gen = CsvExampleGen(input_base=examples)

# ------------------------------------------------------------------

compute_training_stats = StatisticsGen(
      input_data=example_gen.outputs.examples,
      name='compute_training_stats'
      )

# ------------------------------------------------------------------

infer_schema = SchemaGen(stats=compute_training_stats.outputs.output)


# ------------------------------------------------------------------

