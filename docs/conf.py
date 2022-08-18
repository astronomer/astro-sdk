# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

# -- Project information -----------------------------------------------------

# We don't want to download the astro package every time we build docs, so this allows us to get the version number
# without installing astro
__version__ = None
with open("../src/astro/__init__.py") as f:
    while not __version__:
        current_line = f.readline()
        if "__version__" in current_line:
            __version__ = current_line.split(" ")[-1]

project = "astro-sdk-python"
copyright = "2022, Astronomer inc."  # noqa
author = "Astronomer inc."

# The full version, including alpha/beta/rc tags
release = __version__

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "autoapi.extension", "myst_parser"]
autodoc_typehints = "description"

autoapi_keep_files = True
autoapi_type = "python"
autoapi_template_dir = "_autoapi_template"
autoapi_dirs = ["../src"]
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
]
# Add any paths that contain templates here, relative to this directory.
templates_path = ["_autoapi_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "*.txt"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}


def skip_util_classes(app, what, name, obj, skip, options):
    """This allows us skipping certain objects (including functions & methods) from docs"""
    if ":sphinx-autoapi-skip:" in obj.docstring:
        skip = True
    return skip


def setup(sphinx):
    sphinx.connect("autoapi-skip-member", skip_util_classes)
