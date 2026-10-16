"""Settings every test run starts from.

The API reads last night's diversion allowances from the published build
(`PUBLISHED_INDEX_URL`). A test that reaches an endpoint must not fetch them
from the live site: switched off here, before api.main is imported, and a test
of the allowances sets them itself.
"""
import os

os.environ["PUBLISHED_INDEX_URL"] = ""
