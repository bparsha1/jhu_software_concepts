# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath('../'))
sys.path.insert(0, os.path.abspath('../src'))

# -- Project information -----------------------------------------------------

project = 'Grad School Cafe Data Analysis'
copyright = '2025, Burch Parshall'
author = 'Burch Parshall'
release = '1.0.0'
version = '1.0.0'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',        # Core autodoc functionality
    'sphinx.ext.viewcode',       # Add source code links
    'sphinx.ext.napoleon',       # Support for Google and NumPy style docstrings
    'sphinx.ext.intersphinx',    # Link to other project's documentation
    'sphinx.ext.githubpages',    # Publish HTML docs in GitHub pages
    'sphinx.ext.coverage',       # Coverage checker for documentation
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
html_theme = 'sphinx_rtd_theme'

# Theme options are theme-specific and customize the look and feel of a theme
html_theme_options = {
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',
        'searchbox.html',
        'donate.html',
    ]
}

# -- Options for autodoc ----------------------------------------------------

# This value selects what content will be inserted into the main body of an
# autoclass directive.
autoclass_content = 'both'

# This value is a list of autodoc directive flags that should be automatically
# applied to all autodoc directives.
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# -- Options for Napoleon (Google/NumPy style docstrings) -------------------

# True to parse NumPy style docstrings. False to disable NumPy style docstrings.
napoleon_numpy_docstring = False

# True to parse Google style docstrings. False to disable Google style docstrings.
napoleon_google_docstring = False

# True to use Sphinx-style docstrings (since you're using Sphinx format)
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_use_ivar = True

# -- Options for intersphinx extension --------------------------------------

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'flask': ('https://flask.palletsprojects.com/', None),
    'psycopg': ('https://www.psycopg.org/psycopg3/', None),
}

# -- Mock imports for modules not available during build --------------------

# Mock modules that might not be available during documentation build
autodoc_mock_imports = []

# -- Options for coverage extension -----------------------------------------

coverage_show_missing_items = True