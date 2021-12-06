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

from typing import Callable, Optional

from astro.dataframe import dataframe


def train(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    """
    For now this will have the same functionality as dataframe, but eventually we will add features
    that will optimize this experience for training models
    """
    return dataframe(
        python_callable,
        multiple_outputs,
        conn_id,
        database,
        schema,
        warehouse,
    )


def predict(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    """
    For now this will have the same functionality as dataframe, but eventually we will add features
    that will optimize this experience for deploying inference
    """
    return dataframe(
        python_callable,
        multiple_outputs,
        conn_id,
        database,
        schema,
        warehouse,
    )
