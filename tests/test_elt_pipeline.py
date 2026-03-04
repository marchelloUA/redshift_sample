import pytest
from python.elt_pipeline import (
    ELTPipeline,
    PipelineConfig,
    TaskConfig,
    TaskStatus,
    PipelineStatus,
)


class DummyConnManager:
    def __init__(self):
        self.queries = []

    def execute_query(self, query, params=None, timeout=None):
        self.queries.append((query, params))
        # return dummy rows to indicate success
        return [{'a': 1}]

    def test_connection(self):
        return True


def test_simple_pipeline_execution():
    conn = DummyConnManager()
    pipeline_config = PipelineConfig(name="p1", description="desc")
    pipeline = ELTPipeline(pipeline_config, conn)
    
    # add simple tasks
    pipeline.add_task(TaskConfig(name="t1", task_type="python", python_function=lambda: "ok"))
    pipeline.add_task(TaskConfig(name="t2", task_type="transform", sql="SELECT 1", dependencies=["t1"]))
    
    success = pipeline.execute()
    assert success is True
    assert pipeline.status == PipelineStatus.COMPLETED
    summary = pipeline.get_pipeline_summary()
    assert summary['total_tasks'] == 2
    assert summary['completed_tasks'] == 2


def test_task_retry_logic(monkeypatch):
    conn = DummyConnManager()
    # Set lower retry delay for testing
    pipeline_config = PipelineConfig(name="retry", description="test", max_retries=1)
    pipeline = ELTPipeline(pipeline_config, conn)
    
    # create failing task first
    def fail():
        raise Exception("fail")

    # Use low retry_delay to avoid long wait times during testing
    pipeline.add_task(TaskConfig(name="t1", task_type="python", python_function=fail, max_retries=0, retry_delay=0))
    success = pipeline.execute()
    assert success is False
    assert pipeline.status == PipelineStatus.FAILED
    summary = pipeline.get_pipeline_summary()
    assert summary['failed_tasks'] == 1


def test_pipeline_cancel():
    conn = DummyConnManager()
    pipeline_config = PipelineConfig(name="canc", description="test")
    pipeline = ELTPipeline(pipeline_config, conn)
    pipeline.cancel()
    assert pipeline.status == PipelineStatus.CANCELLED
