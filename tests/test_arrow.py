import pyarrow as pa


def test_observations(mock_observations_record_batch: pa.RecordBatch):
    assert len(mock_observations_record_batch) > 0
