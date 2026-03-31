#!/usr/bin/env python

#
# This file is part of the `download_manager` Python module
#
# Copyright {% now 'utc', '%Y' %}
# Heidelberg University Hospital
#
# File author(s): OmniPath team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

"""
Package metadata (version, authors, etc).
"""

__all__ = ['get_metadata']

import os
import pathlib
import importlib.metadata
import logging

import toml

_VERSION = '0.0.1'
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def get_metadata():
    """
    Basic package metadata.

    Retrieves package metadata from the current project directory or from
    the installed package.
    """

    here = pathlib.Path(__file__).parent
    logger.debug('Resolving package metadata from %s', here)
    pyproj_toml = 'pyproject.toml'
    meta = {}

    for project_dir in (here, here.parent):

        toml_path = str(project_dir.joinpath(pyproj_toml).absolute())

        if os.path.exists(toml_path):
            logger.info('Loading metadata from pyproject file: %s', toml_path)

            pyproject = toml.load(toml_path)

            project = pyproject.get('project')
            project = project or pyproject.get('tool', {}).get('poetry', {})

            meta = {
                'name': project.get('name'),
                'version': project.get('version'),
                'author': project.get('authors'),
                'license': project.get('license'),
                'full_metadata': pyproject,
            }

            break

    if not meta:
        logger.warning('No local pyproject metadata found, trying installed metadata')

        try:

            meta = {
                k.lower(): v for k, v in
                importlib.metadata.metadata(here.name).items()
            }
            logger.info('Loaded metadata from installed package')

        except importlib.metadata.PackageNotFoundError:
            logger.error('Installed package metadata not found for %s', here.name)

            pass

    meta['version'] = meta.get('version', None) or _VERSION
    logger.debug('Resolved package version=%s', meta['version'])

    return meta


metadata = get_metadata()
__version__ = metadata.get('version', None)
__author__ = metadata.get('author', None)
__license__ = metadata.get('license', None)
