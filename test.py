from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class WordCountTest(unittest.TestCase):

  # Our input data, which will make up the initial PCollection.
  WORDS = [
      "hi", "there", "hi", "hi", "sue", "bob",
      "hi", "sue", "", "", "ZOW", "bob", ""
  ]

  # Our output data, which is the expected data that the final PCollection must match.
  EXPECTED_COUNTS = ["hi: 5", "there: 1", "sue: 2", "bob: 2"]

  # Example test that tests the pipeline's transforms.

  def test_count_words(self):
    with TestPipeline() as p:

      # Create a PCollection from the WORDS static input data.
      input = p | beam.Create(WORDS)

      # Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
      output = input | CountWords()

      # Assert that the output PCollection matches the EXPECTED_COUNTS data.
      assert_that(output, equal_to(EXPECTED_COUNTS), label='CheckOutput')

    # The pipeline will run and verify the results.