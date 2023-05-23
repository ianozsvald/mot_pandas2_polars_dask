"""Profile mem usage envelope of IPython commands and report interactively

Use 
In[] import cell_profiler
In[] %start_cell_profiler # invoke magic-based tracking and
# %stop_cell_profiler to disable
"""

from IPython.core.magic import (
    register_cell_magic, register_line_cell_magic
)

#import ipython_memory_usage.ipython_memory_usage as imu
import cell_profiler.cell_profiler as cp # NOTE will have to change for package distribution


@register_line_cell_magic
def start_cell_profiler(line, cell=None):
    cp.start_watching_memory()
    return 'Profiling enabled'

@register_line_cell_magic
def stop_cell_profiler(line, cell=None):
    cp.stop_watching_memory()
    return 'Profiling disabled'

