# This project uses GIT metadata for version determination.
# To find the version number for an uninstalled copy of
# this package, execute:
#
#   python -m setuptools_scm
#
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # package is not installed
    pass
