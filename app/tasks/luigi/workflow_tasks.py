import luigi
from .batch_tasks import SqlBatchTask

class UploadDataset(luigi.Task):
    def requires(self):
        load_data = SqlBatchTask("load data command")
        cluster_data = SqlBatchTask("cluster data command", depends_on=load_data)
        index_data = SqlBatchTask("index data command", depends_on=load_data)

