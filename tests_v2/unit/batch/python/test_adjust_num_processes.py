import pytest

from batch.python.adjust_num_processes import calc_num_processes
from tests_v2.utils import BatchJobMock

job_descriptions = [
    {
        "jobId": "8e76ecf5-99a0-43a1-9b97-8e6616b90983",
        "attempts": [
            {"container": {"exitCode": 137}},
            {"container": {"exitCode": 1}},
            {"container": {"exitCode": 137}},
        ],
    }
]


@pytest.mark.parametrize("orig_num_processes,expected", [(96, 24), (5, 1), (0, 1)])
def test_calc_num_processes(orig_num_processes, expected):
    job_id: str = "8e76ecf5-99a0-43a1-9b97-8e6616b90983"
    batch_client = BatchJobMock(job_desc=job_descriptions)

    new_cores_val = calc_num_processes(job_id, orig_num_processes, batch_client)
    assert new_cores_val == expected
