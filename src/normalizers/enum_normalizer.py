import re
from common.utils import TransformerLogger
from abc import ABC, abstractmethod

LOG = TransformerLogger(__name__)

class EnumNormalizer(ABC):
    @property
    def schema(self):
        pass
    @staticmethod
    @abstractmethod
    def normalize(value):
        pass


class GenderNormalizer(EnumNormalizer):
    @property
    def schema(self):
        return 'Gender'

    @staticmethod
    def normalize(value):
        gender = value.strip().lower()

        # Map various forms to standardized values
        if gender in ['male', 'm', 'man', 'boy']:
            return 'Male'
        elif gender in ['female', 'f', 'woman', 'girl']:
            return 'Female'
        else:
            return 'Other'
