from uuid import UUID

import pytest

from batch.python.adjust_cores import calc_new_cores_val
from tests_v2.utils import BatchJobMock

job_descriptions = [
    {
        "jobId": "8e76ecf5-99a0-43a1-9b97-8e6616b90983",
        "attempts": [
            {
                "container": {
                    "reason": "OutOfMemoryError: Container killed due to memory usage"
                }
            },
            {"container": {"reason": "Something else"}},
            {
                "container": {
                    "reason": "OutOfMemoryError: Container killed due to memory usage"
                }
            },
        ],
    }
]


@pytest.mark.parametrize("orig_num_processes,expected", [(96, 24), (5, 2), (0, 1)])
def test_calc_new_cores_val(orig_num_processes, expected):
    job_id: UUID = UUID("8e76ecf5-99a0-43a1-9b97-8e6616b90983")
    batch_client = BatchJobMock(job_desc=job_descriptions)

    new_cores_val = calc_new_cores_val(job_id, orig_num_processes, batch_client)
    assert new_cores_val == expected
