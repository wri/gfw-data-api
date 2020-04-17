import luigi
import os
from luigi.contrib.batch import BatchTask


class SqlBatchTask(BatchTask):
    sql_command = luigi.Parameter()
    depends_on = luigi.Parameter(default=None)
    job_definition = os.environ("")
    job_queue = os.environ("")
    poll_time = luigi.IntParameter(default=300)
    job_completed = False


    def run(self):
        if self.depends_on:
            yield self.depends_on

        self.parameters = {
            "sql_command": self.sql_command
        }

        super.run(self)
        self.job_completed = True


    def complete(self):
        return self.job_completed