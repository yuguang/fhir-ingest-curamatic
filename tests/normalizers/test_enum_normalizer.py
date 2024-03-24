from normalizers.enum_normalizer import *

def test_normalize_gender():
    assert GenderNormalizer.normalize('male') == "Male"
