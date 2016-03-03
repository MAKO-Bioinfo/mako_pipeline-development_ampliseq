# #===========================================================================##
# # lims.py
# # This is a test script to test the LIMS API python package
# #
# # Jack Yen
# # Feb 18th, 2015
# #===========================================================================##

from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD

__LIMS__ = Lims(BASEURI, USERNAME, PASSWORD)



