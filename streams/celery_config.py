# from . import services
import sys

result_backend = "redis://localhost/1"
include = ["streams.tasks"]
accept_content = ["pickle"]
result_serializer = "pickle"
task_serializer = "pickle"
task_store_errors_even_if_ignored = True
timezone = "US/Central"

# log = services.logger()
worker_redirect_stdouts = sys.platform != "darwin"
worker_prefetch_multiplier = 24
