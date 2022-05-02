import pathlib
from typing import Dict


def get_module_dot_notation(module_path: pathlib.Path) -> str:
    """
    Given a module path, return the dot notation import string starting at astro.

    :param module_path: Path to the module of interest
    :return: String containing the absolute dot notation path to the module
    """
    # We assume that this function is only being used for Astro submodules
    # This can be generalised in future if needed
    base_dir = pathlib.Path(__file__).parent.parent.parent  # `astro` directory
    module_path.relative_to(base_dir)
    return ".".join(module_path.relative_to(base_dir).with_suffix("").parts)


def get_dict_with_module_names_to_dot_notations(
    base_path: pathlib.Path,
) -> Dict[str, str]:
    """
    Given a directory, recursively identify which modules exist within it
    (ignoring __init__.py & base.py) and create a dictionary which has module names
    as keys and the values are the dot notation import paths.

    An example:
         ├── package
             ├── module.py
             ├── subpackage
                ├── __init__.py
                └── subpackage_module.py

    Running:
        from pathlib import Path
        from astro.utils.path import get_dict_with_module_names_to_dot_notations
        values = get_dict_with_module_names_to_dot_notations(Path("package"))
        print(values)

    Prints:
        {
            "module": "package.module",
            "subpackage_module": "package.subpackage.subpackage_module"
        }
    """
    module_name_to_dot_notation = {}
    for module_path in base_path.parent.rglob("*.py"):
        if module_path.name not in ["__init__.py", "base.py"]:
            module_name_to_dot_notation[module_path.stem] = get_module_dot_notation(
                module_path
            )
    return module_name_to_dot_notation
