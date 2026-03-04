"""
Redshift Python utilities for connection management and ELT pipelines.
"""

# Lazy imports to avoid circular dependencies and initialization issues
def __getattr__(name):
    if name == 'RedshiftConnectionManager':
        from .redshift_connector import RedshiftConnectionManager
        return RedshiftConnectionManager
    elif name == 'ConnectionConfig':
        from .redshift_connector import ConnectionConfig
        return ConnectionConfig
    elif name == 'create_redshift_connection':
        from .redshift_connector import create_redshift_connection
        return create_redshift_connection
    elif name == 'ELTPipeline':
        from .elt_pipeline import ELTPipeline
        return ELTPipeline
    elif name == 'PipelineConfig':
        from .elt_pipeline import PipelineConfig
        return PipelineConfig
    elif name == 'TaskConfig':
        from .elt_pipeline import TaskConfig
        return TaskConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    'RedshiftConnectionManager',
    'ConnectionConfig',
    'create_redshift_connection',
    'ELTPipeline',
    'PipelineConfig',
    'TaskConfig',
]
