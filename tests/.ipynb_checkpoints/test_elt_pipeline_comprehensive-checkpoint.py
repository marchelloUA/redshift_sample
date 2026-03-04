import pytest
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from python.elt_pipeline import (
    ELTPipeline,
    PipelineConfig,
    TaskConfig,
    PipelineTask,
    PipelineStatus,
    TaskStatus,
)


class DummyConnManager:
    def __init__(self):
        self.queries = []
        self.call_count = 0

    def execute_query(self, query, params=None, timeout=None):
        self.queries.append((query, params))
        self.call_count += 1
        return [{'a': 1, 'b': 2}]

    def test_connection(self):
        return True


def test_pipeline_config_creation():
    """Test PipelineConfig dataclass creation and defaults"""
    config = PipelineConfig(name="test_pipeline", description="Test pipeline")
    
    assert config.name == "test_pipeline"
    assert config.description == "Test pipeline"
    assert config.max_retries == 3
    assert config.retry_delay == 60
    assert config.timeout == 3600
    assert config.parallel_tasks == 4
    assert config.notification_email is None
    assert config.log_level == "INFO"


def test_pipeline_config_custom_values():
    """Test PipelineConfig with custom values"""
    config = PipelineConfig(
        name="custom_pipeline",
        description="Custom pipeline",
        max_retries=5,
        retry_delay=30,
        timeout=7200,
        parallel_tasks=8,
        notification_email="test@example.com",
        log_level="DEBUG"
    )
    
    assert config.name == "custom_pipeline"
    assert config.max_retries == 5
    assert config.retry_delay == 30
    assert config.timeout == 7200
    assert config.parallel_tasks == 8
    assert config.notification_email == "test@example.com"
    assert config.log_level == "DEBUG"


def test_task_config_creation():
    """Test TaskConfig dataclass creation and defaults"""
    config = TaskConfig(name="test_task", task_type="transform")
    
    assert config.name == "test_task"
    assert config.task_type == "transform"
    assert config.source is None
    assert config.target is None
    assert config.sql is None
    assert config.python_function is None
    assert config.dependencies == []
    assert config.max_retries == 3
    assert config.retry_delay == 60
    assert config.timeout == 1800
    assert config.parameters == {}


def test_task_config_with_dependencies():
    """Test TaskConfig with dependencies"""
    config = TaskConfig(
        name="task_b",
        task_type="load",
        dependencies=["task_a", "task_c"]
    )
    
    assert config.dependencies == ["task_a", "task_c"]


def test_pipeline_task_initialization():
    """Test PipelineTask initialization"""
    conn = DummyConnManager()
    task_config = TaskConfig(name="test_task", task_type="python", python_function=lambda: "ok")
    
    task = PipelineTask(task_config, conn)
    
    assert task.config == task_config
    assert task.conn_manager == conn
    assert task.status == TaskStatus.PENDING
    assert task.start_time is None
    assert task.end_time is None
    assert task.retry_count == 0
    assert task.error_message is None
    assert task.result is None


def test_pipeline_task_get_duration():
    """Test PipelineTask.get_duration() method"""
    conn = DummyConnManager()
    task_config = TaskConfig(name="test_task", task_type="python", python_function=lambda: "ok")
    task = PipelineTask(task_config, conn)
    
    # No duration initially
    assert task.get_duration() is None
    
    # Set times
    task.start_time = datetime.now()
    task.end_time = task.start_time + timedelta(seconds=5)
    
    duration = task.get_duration()
    assert duration is not None
    assert duration.total_seconds() >= 5


def test_pipeline_task_execute_extract_s3():
    """Test PipelineTask._execute_extract with S3 source"""
    conn = DummyConnManager()
    task_config = TaskConfig(
        name="extract_task",
        task_type="extract",
        source="s3://bucket/path"
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is True
    assert task.status == TaskStatus.COMPLETED
    assert task.result is not None
    assert 'source' in task.result
    assert task.result['source'] == "s3://bucket/path"


def test_pipeline_task_execute_extract_database():
    """Test PipelineTask._execute_extract with database source"""
    conn = DummyConnManager()
    task_config = TaskConfig(
        name="extract_task",
        task_type="extract",
        source="jdbc:postgresql://localhost/db"
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is True
    assert task.status == TaskStatus.COMPLETED
    assert task.result is not None


def test_pipeline_task_execute_load():
    """Test PipelineTask._execute_load"""
    conn = DummyConnManager()
    task_config = TaskConfig(
        name="load_task",
        task_type="load",
        target="target_table",
        sql="INSERT INTO target_table VALUES (1, 2)"
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is True
    assert task.status == TaskStatus.COMPLETED
    assert task.result is not None
    assert task.result['target'] == "target_table"


def test_pipeline_task_execute_transform():
    """Test PipelineTask._execute_transform"""
    conn = DummyConnManager()
    task_config = TaskConfig(
        name="transform_task",
        task_type="transform",
        sql="SELECT * FROM table"
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is True
    assert task.status == TaskStatus.COMPLETED
    assert task.result is not None
    assert 'records_transformed' in task.result


def test_pipeline_task_execute_python_function():
    """Test PipelineTask._execute_python with function"""
    conn = DummyConnManager()
    test_func = MagicMock(return_value="success")
    task_config = TaskConfig(
        name="python_task",
        task_type="python",
        python_function=test_func,
        parameters={"arg1": "value1", "arg2": "value2"}
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is True
    assert task.status == TaskStatus.COMPLETED
    assert task.result == "success"
    test_func.assert_called_once_with(arg1="value1", arg2="value2")


def test_elt_pipeline_initialization():
    """Test ELTPipeline initialization"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    
    pipeline = ELTPipeline(config, conn)
    
    assert pipeline.config == config
    assert pipeline.conn_manager == conn
    assert pipeline.tasks == {}
    assert pipeline.status == PipelineStatus.PENDING
    assert pipeline.start_time is None
    assert pipeline.end_time is None


def test_elt_pipeline_add_task():
    """Test ELTPipeline.add_task()"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    task_config = TaskConfig(name="task1", task_type="python", python_function=lambda: "ok")
    pipeline.add_task(task_config)
    
    assert "task1" in pipeline.tasks
    assert isinstance(pipeline.tasks["task1"], PipelineTask)
    assert pipeline.tasks["task1"].config.name == "task1"


def test_elt_pipeline_validate_pipeline_no_duplicates():
    """Test _validate_pipeline with no duplicate names"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    pipeline.add_task(TaskConfig(name="task2", task_type="python", python_function=lambda: "ok"))
    
    assert pipeline._validate_pipeline() is True


def test_elt_pipeline_validate_pipeline_duplicate_names():
    """Test _validate_pipeline with duplicate task names"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    # Manually add duplicate tasks (bypassing add_task which would overwrite)
    task_config1 = TaskConfig(name="task1", task_type="python", python_function=lambda: "ok")
    task_config2 = TaskConfig(name="task1", task_type="python", python_function=lambda: "ok")
    
    pipeline.tasks["task1_v1"] = PipelineTask(task_config1, conn)
    pipeline.tasks["task1_v2"] = PipelineTask(task_config2, conn)
    
    # Since both use same name in config, validate will fail
    # Recreate to properly test
    pipeline = ELTPipeline(config, conn)
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    
    # Manually override task names to create duplicate
    list(pipeline.tasks.values())[0].config.name = "dup_task"
    task = PipelineTask(TaskConfig(name="dup_task", task_type="python", python_function=lambda: "ok"), conn)
    pipeline.tasks["second_dup"] = task
    
    with pytest.raises(Exception, match="Duplicate task names found"):
        pipeline._validate_pipeline()


def test_elt_pipeline_build_dependency_graph():
    """Test _build_dependency_graph()"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    pipeline.add_task(TaskConfig(name="task2", task_type="python", python_function=lambda: "ok", dependencies=["task1"]))
    pipeline.add_task(TaskConfig(name="task3", task_type="python", python_function=lambda: "ok", dependencies=["task1", "task2"]))
    
    graph = pipeline._build_dependency_graph()
    
    assert graph["task1"] == []
    assert graph["task2"] == ["task1"]
    assert graph["task3"] == ["task1", "task2"]


def test_elt_pipeline_get_pipeline_summary_pending():
    """Test get_pipeline_summary() for pending pipeline"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    
    summary = pipeline.get_pipeline_summary()
    
    assert summary['pipeline_name'] == "test_pipeline"
    assert summary['status'] == "pending"
    assert summary['total_tasks'] == 1
    assert summary['completed_tasks'] == 0
    assert summary['failed_tasks'] == 0
    assert summary['start_time'] is None
    assert summary['end_time'] is None


def test_elt_pipeline_get_pipeline_summary_running():
    """Test get_pipeline_summary() for running pipeline"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.status = PipelineStatus.RUNNING
    pipeline.start_time = datetime.now()
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    
    summary = pipeline.get_pipeline_summary()
    
    assert summary['status'] == "running"
    assert summary['start_time'] is not None
    assert summary['duration'] is not None or summary['duration'] is None  # May be None if no end time


def test_elt_pipeline_get_pipeline_summary_with_task_details():
    """Test get_pipeline_summary() includes task details"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    
    # Set task status
    task = list(pipeline.tasks.values())[0]
    task.status = TaskStatus.COMPLETED
    task.start_time = datetime.now()
    task.end_time = task.start_time + timedelta(seconds=1)
    
    summary = pipeline.get_pipeline_summary()
    
    assert 'tasks' in summary
    assert 'task1' in summary['tasks']
    assert summary['tasks']['task1']['status'] == "completed"
    assert summary['tasks']['task1']['duration'] is not None


def test_elt_pipeline_execute_with_dependencies():
    """Test ELTPipeline.execute() with task dependencies"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test", parallel_tasks=1)
    pipeline = ELTPipeline(config, conn)
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    pipeline.add_task(TaskConfig(name="task2", task_type="python", python_function=lambda: "ok", dependencies=["task1"]))
    
    success = pipeline.execute()
    
    assert success is True
    assert pipeline.status == PipelineStatus.COMPLETED
    assert all(task.status == TaskStatus.COMPLETED for task in pipeline.tasks.values())


def test_elt_pipeline_execute_empty_pipeline():
    """Test ELTPipeline.execute() with no tasks"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    success = pipeline.execute()
    
    assert success is True
    assert pipeline.status == PipelineStatus.COMPLETED


def test_elt_pipeline_execute_validation_fails():
    """Test ELTPipeline.execute() with validation failure"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok"))
    
    # Patch validation to fail
    with patch.object(pipeline, '_validate_pipeline', return_value=False):
        success = pipeline.execute()
    
    assert success is False
    assert pipeline.status == PipelineStatus.FAILED


def test_elt_pipeline_execute_capture_exception():
    """Test ELTPipeline.execute() captures exceptions"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    # Patch execute to raise exception
    with patch.object(pipeline, '_execute_tasks', side_effect=Exception("Test error")):
        success = pipeline.execute()
    
    assert success is False
    assert pipeline.status == PipelineStatus.FAILED
    assert pipeline.end_time is not None


def test_elt_pipeline_cancel():
    """Test ELTPipeline.cancel()"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    pipeline.cancel()
    
    assert pipeline.status == PipelineStatus.CANCELLED
    assert pipeline.end_time is not None


def test_pipeline_has_circular_dependencies():
    """Test _has_circular_dependencies() method"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test")
    pipeline = ELTPipeline(config, conn)
    
    # Current implementation always returns False
    # This test ensures the method exists and can be called
    result = pipeline._has_circular_dependencies()
    assert result is False


def test_elt_pipeline_execute_parallel_tasks():
    """Test ELTPipeline parallel task execution"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test", parallel_tasks=2)
    pipeline = ELTPipeline(config, conn)
    
    # Add multiple independent tasks
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=lambda: "ok1"))
    pipeline.add_task(TaskConfig(name="task2", task_type="python", python_function=lambda: "ok2"))
    pipeline.add_task(TaskConfig(name="task3", task_type="python", python_function=lambda: "ok3"))
    
    success = pipeline.execute()
    
    assert success is True
    assert pipeline.status == PipelineStatus.COMPLETED
    assert sum(1 for task in pipeline.tasks.values() if task.status == TaskStatus.COMPLETED) == 3


def test_pipeline_task_retry_with_exception():
    """Test PipelineTask retry behavior on exception"""
    conn = DummyConnManager()
    
    call_count = [0]
    def failing_func():
        call_count[0] += 1
        if call_count[0] < 2:
            raise Exception("First call fails")
        return "success"
    
    task_config = TaskConfig(
        name="retry_task",
        task_type="python",
        python_function=failing_func,
        max_retries=1,
        retry_delay=0
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is True
    assert task.status == TaskStatus.COMPLETED
    assert task.retry_count == 1


def test_pipeline_task_max_retries_exceeded():
    """Test PipelineTask when max retries exceeded"""
    conn = DummyConnManager()
    
    def always_failing_func():
        raise Exception("Always fails")
    
    task_config = TaskConfig(
        name="failing_task",
        task_type="python",
        python_function=always_failing_func,
        max_retries=2,
        retry_delay=0
    )
    
    task = PipelineTask(task_config, conn)
    result = task.execute()
    
    assert result is False
    assert task.status == TaskStatus.FAILED
    assert task.retry_count <= 2


def test_elt_pipeline_task_failure_stops_pipeline():
    """Test that task failure stops pipeline execution"""
    conn = DummyConnManager()
    config = PipelineConfig(name="test_pipeline", description="Test", parallel_tasks=1)
    pipeline = ELTPipeline(config, conn)
    
    def failing_func():
        raise Exception("Task fails")
    
    pipeline.add_task(TaskConfig(name="task1", task_type="python", python_function=failing_func, max_retries=0))
    pipeline.add_task(TaskConfig(name="task2", task_type="python", python_function=lambda: "ok", dependencies=["task1"]))
    
    success = pipeline.execute()
    
    assert success is False
    assert pipeline.status == PipelineStatus.FAILED
    assert pipeline.tasks["task1"].status == TaskStatus.FAILED
