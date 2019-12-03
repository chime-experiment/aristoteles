# This project uses GIT metadata for version determination.
# To find the version number for an uninstalled copy of
# this package, execute:
#
#   python setup.py --version
#
from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass
