#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
The contents of this file were originally copied from:
https://github.com/apache/airflow/blob/4dd3c6a869497924617d6a6373dd7339b554f5d9/tests/test_utils/config.py
"""
from __future__ import annotations

import contextlib
import os


@contextlib.contextmanager
def env_vars(overrides: dict):
    """
    Temporarily patches env vars, restoring env as it was after context exit.
    Example:
        with env_vars({'AIRFLOW_CONN_AWS_DEFAULT': 's3://@'}):
            # now we have an aws default connection available
    """
    orig_vars = {}
    new_vars = []
    for env, value in overrides.items():
        if env in os.environ:
            orig_vars[env] = os.environ.pop(env, "")
        else:
            new_vars.append(env)
        os.environ[env] = value
    try:
        yield
    finally:
        for env, value in orig_vars.items():
            os.environ[env] = value
        for env in new_vars:
            os.environ.pop(env)
