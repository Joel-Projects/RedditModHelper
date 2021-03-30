# from . import services

result_backend = "redis://localhost/2"
include = ["streams.tasks"]
accept_content = ["pickle"]
result_serializer = "pickle"
task_serializer = "pickle"
task_store_errors_even_if_ignored = True
timezone = "US/Central"
# log = services.logger()
