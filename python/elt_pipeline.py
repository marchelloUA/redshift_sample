"""
ELT Pipeline Framework
Provides a comprehensive framework for Extract, Load, Transform operations on Redshift.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

from .redshift_connector import RedshiftConnectionManager, ConnectionConfig

class PipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

@dataclass
class PipelineConfig:
    """Configuration for ELT pipeline"""
    name: str
    description: str
    max_retries: int = 3
    retry_delay: int = 60  # seconds
    timeout: int = 3600  # seconds
    parallel_tasks: int = 4
    notification_email: Optional[str] = None
    log_level: str = "INFO"
    
@dataclass
class TaskConfig:
    """Configuration for individual pipeline task"""
    name: str
    task_type: str  # 'extract', 'load', 'transform'
    source: Optional[str] = None
    target: Optional[str] = None
    sql: Optional[str] = None
    python_function: Optional[Callable] = None
    dependencies: List[str] = field(default_factory=list)
    max_retries: int = 3
    retry_delay: int = 60
    timeout: int = 1800  # 30 minutes
    parameters: Dict[str, Any] = field(default_factory=dict)

class PipelineTask:
    """Individual task in the ELT pipeline"""
    
    def __init__(self, config: TaskConfig, conn_manager: RedshiftConnectionManager):
        self.config = config
        self.conn_manager = conn_manager
        self.status = TaskStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.retry_count = 0
        self.error_message: Optional[str] = None
        self.result: Optional[Any] = None
        
    def execute(self) -> bool:
        """Execute the task with retry logic"""
        self.status = TaskStatus.RUNNING
        self.start_time = datetime.now()
        
        try:
            # Check dependencies
            if not self._check_dependencies():
                raise Exception("Dependencies not satisfied")
            
            # Execute based on task type
            if self.config.task_type == 'extract':
                self.result = self._execute_extract()
            elif self.config.task_type == 'load':
                self.result = self._execute_load()
            elif self.config.task_type == 'transform':
                self.result = self._execute_transform()
            elif self.config.task_type == 'python':
                self.result = self._execute_python()
            else:
                raise Exception(f"Unknown task type: {self.config.task_type}")
            
            self.status = TaskStatus.COMPLETED
            self.end_time = datetime.now()
            return True
            
        except Exception as e:
            self.error_message = str(e)
            self.end_time = datetime.now()
            
            # Retry logic
            if self.retry_count < self.config.max_retries:
                self.retry_count += 1
                self.status = TaskStatus.RETRYING
                time.sleep(self.config.retry_delay)
                return self.execute()
            else:
                self.status = TaskStatus.FAILED
                return False
    
    def _check_dependencies(self) -> bool:
        """Check if all dependencies are completed"""
        # This is a simplified implementation
        # In practice, you would check the status of dependent tasks
        return True
    
    def _execute_extract(self) -> Any:
        """Execute extract task"""
        if not self.config.source:
            raise Exception("Source not specified for extract task")
        
        # Extract logic - could be from S3, database, API, etc.
        if self.config.source.startswith('s3://'):
            return self._extract_from_s3()
        elif self.config.source.startswith('jdbc:'):
            return self._extract_from_database()
        else:
            raise Exception(f"Unsupported source type: {self.config.source}")
    
    def _extract_from_s3(self) -> Dict[str, Any]:
        """Extract data from S3"""
        # Placeholder for S3 extraction logic
        # In practice, you would use boto3 to read from S3
        return {
            'source': self.config.source,
            'records_extracted': 0,
            'extraction_time': datetime.now().isoformat()
        }
    
    def _extract_from_database(self) -> Dict[str, Any]:
        """Extract data from another database"""
        # Placeholder for database extraction logic
        return {
            'source': self.config.source,
            'records_extracted': 0,
            'extraction_time': datetime.now().isoformat()
        }
    
    def _execute_load(self) -> Any:
        """Execute load task"""
        if not self.config.target or not self.config.sql:
            raise Exception("Target or SQL not specified for load task")
        
        # Execute load SQL
        results = self.conn_manager.execute_query(self.config.sql)
        
        return {
            'target': self.config.target,
            'records_loaded': len(results),
            'load_time': datetime.now().isoformat()
        }
    
    def _execute_transform(self) -> Any:
        """Execute transform task"""
        if not self.config.sql:
            raise Exception("SQL not specified for transform task")
        
        # Execute transform SQL
        results = self.conn_manager.execute_query(self.config.sql)
        
        return {
            'transform_type': self.config.task_type,
            'records_transformed': len(results),
            'transform_time': datetime.now().isoformat()
        }
    
    def _execute_python(self) -> Any:
        """Execute Python function task"""
        if not self.config.python_function:
            raise Exception("Python function not specified")
        
        # Execute Python function with parameters
        return self.config.python_function(**self.config.parameters)
    
    def get_duration(self) -> Optional[timedelta]:
        """Get task duration"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

class ELTPipeline:
    """Main ELT pipeline orchestrator"""
    
    def __init__(self, config: PipelineConfig, conn_manager: RedshiftConnectionManager):
        self.config = config
        self.conn_manager = conn_manager
        self.tasks: Dict[str, PipelineTask] = {}
        self.status = PipelineStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.logger = logging.getLogger(__name__)
        
        # Configure logging
        logging.basicConfig(level=getattr(logging, config.log_level))
    
    def add_task(self, task_config: TaskConfig):
        """Add a task to the pipeline"""
        task = PipelineTask(task_config, self.conn_manager)
        self.tasks[task_config.name] = task
        self.logger.info(f"Added task: {task_config.name}")
    
    def execute(self) -> bool:
        """Execute the entire pipeline"""
        self.status = PipelineStatus.RUNNING
        self.start_time = datetime.now()
        
        try:
            # Validate pipeline
            if not self._validate_pipeline():
                raise Exception("Pipeline validation failed")
            
            # Execute tasks with dependency resolution
            success = self._execute_tasks()
            
            if success:
                self.status = PipelineStatus.COMPLETED
            else:
                self.status = PipelineStatus.FAILED
            
            self.end_time = datetime.now()
            return success
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self.status = PipelineStatus.FAILED
            self.end_time = datetime.now()
            return False
    
    def _validate_pipeline(self) -> bool:
        """Validate pipeline configuration"""
        # Check for duplicate task names
        task_names = [task.config.name for task in self.tasks.values()]
        if len(task_names) != len(set(task_names)):
            raise Exception("Duplicate task names found")
        
        # Check for circular dependencies
        if self._has_circular_dependencies():
            raise Exception("Circular dependencies detected")
        
        return True
    
    def _has_circular_dependencies(self) -> bool:
        """Check for circular dependencies in tasks"""
        # Simplified implementation - in practice, you would use a graph algorithm
        return False
    
    def _execute_tasks(self) -> bool:
        """Execute tasks with dependency resolution and parallel execution"""
        # Build dependency graph
        dependency_graph = self._build_dependency_graph()
        
        # Execute tasks in topological order
        completed_tasks = set()
        remaining_tasks = set(self.tasks.keys())
        
        while remaining_tasks:
            # Find tasks with no unresolved dependencies
            ready_tasks = []
            for task_name in remaining_tasks:
                dependencies = self.tasks[task_name].config.dependencies
                if all(dep in completed_tasks for dep in dependencies):
                    ready_tasks.append(task_name)
            
            if not ready_tasks:
                # No tasks can be executed - check for circular dependencies
                self.logger.error("No tasks ready for execution - possible circular dependency")
                return False
            
            # Execute ready tasks in parallel
            with ThreadPoolExecutor(max_workers=self.config.parallel_tasks) as executor:
                future_to_task = {
                    executor.submit(self.tasks[task_name].execute): task_name
                    for task_name in ready_tasks
                }
                
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        success = future.result()
                        if success:
                            completed_tasks.add(task_name)
                            remaining_tasks.remove(task_name)
                            self.logger.info(f"Task completed: {task_name}")
                        else:
                            self.logger.error(f"Task failed: {task_name}")
                            return False
                    except Exception as e:
                        self.logger.error(f"Task {task_name} failed with exception: {e}")
                        return False
        
        return len(completed_tasks) == len(self.tasks)
    
    def _build_dependency_graph(self) -> Dict[str, List[str]]:
        """Build dependency graph for tasks"""
        graph = {}
        for task_name, task in self.tasks.items():
            graph[task_name] = task.config.dependencies
        return graph
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get pipeline execution summary"""
        summary = {
            'pipeline_name': self.config.name,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': str(self.end_time - self.start_time) if self.start_time and self.end_time else None,
            'total_tasks': len(self.tasks),
            'completed_tasks': sum(1 for task in self.tasks.values() if task.status == TaskStatus.COMPLETED),
            'failed_tasks': sum(1 for task in self.tasks.values() if task.status == TaskStatus.FAILED),
            'tasks': {}
        }
        
        # Add task details
        for task_name, task in self.tasks.items():
            summary['tasks'][task_name] = {
                'status': task.status.value,
                'start_time': task.start_time.isoformat() if task.start_time else None,
                'end_time': task.end_time.isoformat() if task.end_time else None,
                'duration': str(task.get_duration()) if task.get_duration() else None,
                'retry_count': task.retry_count,
                'error_message': task.error_message
            }
        
        return summary
    
    def cancel(self):
        """Cancel pipeline execution"""
        self.status = PipelineStatus.CANCELLED
        self.end_time = datetime.now()
        self.logger.info("Pipeline cancelled")

# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Create connection manager
    config = ConnectionConfig(
        host="your-redshift-cluster.redshift.amazonaws.com",
        database="dev",
        user="admin",
        password="your-password"
    )
    conn_manager = RedshiftConnectionManager(config)
    
    # Create pipeline
    pipeline_config = PipelineConfig(
        name="daily_sales_pipeline",
        description="Daily sales data processing pipeline",
        max_retries=3,
        parallel_tasks=2
    )
    
    pipeline = ELTPipeline(pipeline_config, conn_manager)
    
    # Add tasks
    pipeline.add_task(TaskConfig(
        name="extract_sales_data",
        task_type="extract",
        source="s3://your-bucket/sales-data/daily/",
        max_retries=2
    ))
    
    pipeline.add_task(TaskConfig(
        name="transform_sales_data",
        task_type="transform",
        sql="""
            INSERT INTO sales_daily_processed
            SELECT 
                sale_id,
                customer_id,
                product_id,
                sale_amount,
                sale_date,
                region,
                store_id,
                CURRENT_TIMESTAMP as processed_at
            FROM sales_staging
            WHERE sale_date >= CURRENT_DATE - INTERVAL '1 day'
        """,
        dependencies=["extract_sales_data"]
    ))
    
    pipeline.add_task(TaskConfig(
        name="load_to_warehouse",
        task_type="load",
        target="sales_warehouse",
        sql="""
            INSERT INTO sales_warehouse
            SELECT * FROM sales_daily_processed
            ON CONFLICT (sale_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                sale_amount = EXCLUDED.sale_amount,
                sale_date = EXCLUDED.sale_date,
                region = EXCLUDED.region,
                store_id = EXCLUDED.store_id,
                updated_at = CURRENT_TIMESTAMP
        """,
        dependencies=["transform_sales_data"]
    ))
    
    # Execute pipeline
    success = pipeline.execute()
    
    # Get summary
    summary = pipeline.get_pipeline_summary()
    print(json.dumps(summary, indent=2))
    
    # Close connections
    conn_manager.close_all_connections()