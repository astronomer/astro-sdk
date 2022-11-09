from __future__ import annotations

import inspect
from typing import Callable

from airflow.models.xcom_arg import XComArg

from astro.sql.table import BaseTable
from astro.utils.typing_compat import Context


def _have_same_conn_id(tables: list[BaseTable]) -> bool:
    """
    Check to see if all tables belong to same conn_id. Otherwise, this can go wrong for cases
    1. When we have tables from different DBs.
    2. When we have tables from different conn_id, since they can be configured with different database/schema etc.

    :param tables: tables to check for conn_id field
    :return: True if all tables have the same conn id, and False if not.
    """
    return len(tables) == 1 or len({table.conn_id for table in tables}) == 1


def _find_first_table_from_op_args(op_args: tuple, context: Context) -> BaseTable | None:
    """
    Read op_args and extract the tables.

    :param op_args: user-defined operator's args
    :param context: the context to use for resolving XComArgs
    :return: first valid table found in op_args.
    """
    args = [arg.resolve(context) if isinstance(arg, XComArg) else arg for arg in op_args]
    tables = [arg for arg in args if isinstance(arg, BaseTable)]

    if _have_same_conn_id(tables):
        return tables[0]
    return None


def _find_first_table_from_op_kwargs(
    op_kwargs: dict, python_callable: Callable, context: Context
) -> BaseTable | None:
    """
    Read op_kwargs and extract the tables.

    :param op_kwargs: user-defined operator's kwargs
    :param context: the context to use for resolving XComArgs
    :return: first valid table found in op_kwargs.
    """
    kwargs = [
        op_kwargs[kwarg.name].resolve(context)
        if isinstance(op_kwargs[kwarg.name], XComArg)
        else op_kwargs[kwarg.name]
        for kwarg in inspect.signature(python_callable).parameters.values()
    ]
    tables = [kwarg for kwarg in kwargs if isinstance(kwarg, BaseTable)]

    if _have_same_conn_id(tables):
        return tables[0]
    return None


def _find_first_table_from_parameters(parameters: dict, context: Context) -> BaseTable | None:
    """
    Read parameters and extract the tables.

    :param parameters: a user-defined dictionary of parameters
    :param context: the context to use for resolving XComArgs
    :return: first valid table found in parameters.
    """
    params = [
        param.resolve(context) if isinstance(param, XComArg) else param for param in parameters.values()
    ]
    tables = [param for param in params if isinstance(param, BaseTable)]

    if _have_same_conn_id(tables):
        return tables[0]
    return None


def find_first_table(
    op_args: tuple,
    op_kwargs: dict,
    python_callable: Callable,
    parameters: dict,
    context: Context,
) -> BaseTable | None:
    """
    When we create our SQL operation, we run with the assumption that the first table given is the "main table".
    This means that a user doesn't need to define default conn_id, database, etc. in the function unless they want
    to create default values.

    :param op_args: user-defined operator's args
    :param op_kwargs: user-defined operator's kwargs
    :param python_callable: user-defined operator's callable
    :param parameters: user-defined parameters to be injected into SQL statement
    :param context: the context to use for resolving XComArgs
    :return: the first table declared as decorator arg or kwarg
    """
    first_table: BaseTable | None = None

    if op_args:
        first_table = _find_first_table_from_op_args(op_args=op_args, context=context)
    if not first_table and op_kwargs and python_callable is not None:
        first_table = _find_first_table_from_op_kwargs(
            op_kwargs=op_kwargs,
            python_callable=python_callable,
            context=context,
        )
    if not first_table and parameters:
        first_table = _find_first_table_from_parameters(
            parameters=parameters,
            context=context,
        )

    return first_table
