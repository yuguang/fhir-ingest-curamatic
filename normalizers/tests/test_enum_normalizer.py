import sys
import os

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from normalizers.enum_normalizer import *

def test_normalize_gender():
    assert GenderNormalizer.normalize('male') == "Male"
