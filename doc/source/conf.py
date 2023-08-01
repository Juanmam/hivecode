# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

sys.path.insert(0, os.path.abspath('../../src'))
plantuml = 'java -jar ./plantuml.jar'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx_copybutton',
    'sphinx.ext.autodoc'
]


autodoc_mock_imports = [
   'collections',
   'dbruntime',
   'databricks',
   'databricks.koalas',
   'seaborn',
   'databricks_api'
]

project = 'hivecode'
copyright = '2022, Juan Manuel Mejía Botero'
author = 'Juan Manuel Mejía Botero'

templates_path = ['_templates']
exclude_patterns = []

language = 'Python'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'

html_theme_options = {
    'body_centered': False,
    'breadcrumbs': True,
}

html_static_path = ['_static']
html_css_files = ['css/classes.css']