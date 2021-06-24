from uuid import UUID

from batch.python.adjust_cores import calc_new_cores_val
from tests_v2.utils import BatchJobMock


def test_calc_new_cores_val():
    job_id: UUID = UUID("8e76ecf5-99a0-43a1-9b97-8e6616b90983")
    original_cores: int = 96  # TODO: Use parameterize
    batch_client = BatchJobMock()

    new_cores_val = calc_new_cores_val(job_id, original_cores, batch_client)
    assert new_cores_val == 24
