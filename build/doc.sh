#!/bin/bash

#
# Build Sphinx documentation
#

# Convert README.md to index.rst
m2r2 README.md
mv -f README.rst doc/index.rst

# Add apidoc entry-point to index
cat >> doc/index.rst <<EOF

API documentation
-----------------

.. toctree::

   api/modules
EOF

# Build apidoc
sphinx-apidoc --implicit-namespaces -M -t doc_templates --separate -o doc/api src/picnic

# Build final documentation
sphinx-build \
    -b html \
    -C \
    -D project="diepvries" \
    -D copyright="Picnic Technologies" \
    -D extensions="sphinx.ext.autodoc,sphinx.ext.napoleon,m2r2,sphinx_autodoc_typehints" \
    -D napoleon_google_docstring=1 \
    -D napoleon_numpy_docstring=0 \
    -D napoleon_include_init_with_doc=1 \
    -D napoleon_include_private_with_doc=0 \
    -D napoleon_include_special_with_doc=1 \
    -D exclude_patterns="build" \
    -D html_theme="alabaster" \
    -D html_theme_options.description="Data Vault framework in Python" \
    -D html_theme_options.fixed_sidebar="true" \
    -D html_theme_options.sidebar_collapse="false" \
    doc \
    doc/html
