import sys

accept_content = ["pickle"]
result_serializer = "pickle"
task_serializer = "pickle"
task_store_errors_even_if_ignored = False
timezone = "US/Central"

worker_redirect_stdouts = sys.platform != "darwin"
worker_prefetch_multiplier = 0
