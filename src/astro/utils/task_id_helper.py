"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from airflow.decorators.base import get_unique_task_id


def get_task_id(prefix, path):
    """Generate unique tasks id based on the path.
    :parma prefix: prefix string
    :type prefix: str
    :param path: file path.
    :type path: str
    """
    task_id = "{}_{}".format(prefix, path.rsplit("/", 1)[-1].replace(".", "_"))
    return get_unique_task_id(task_id)
