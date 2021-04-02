# from . import services
import sys

result_backend = "amqp://guest:guest@localhost:5672/RedditModHelper"
include = ["streams.tasks"]
accept_content = ["pickle"]
result_serializer = "pickle"
task_serializer = "pickle"
task_store_errors_even_if_ignored = True
timezone = "US/Central"

# log = services.logger()
worker_redirect_stdouts = sys.platform != "darwin"
