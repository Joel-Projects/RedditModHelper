# from . import services

result_backend = "redis//default:ks1e5cgos3pfxrtj@db-redis-nyc1-97053-do-user-4054415-0.b.db.ondigitalocean.com:25061"
include = ["streams.tasks"]
accept_content = ["pickle"]
result_serializer = "pickle"
task_serializer = "pickle"
task_store_errors_even_if_ignored = True
timezone = "US/Central"
# log = services.logger()
