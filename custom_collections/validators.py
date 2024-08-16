from . import error_handlers
from . import collections

class Validator:
    def __init__(self, instance: collections.Collection, key: str, value: str):
        self._instance = instance
        self._key = key
        self._value = value

    def validate(self):
        pass

class UniqueConstraintValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        data = self._instance.select(self._key)
        for d in data:
            if d[self._key] == self._value: 
                raise error_handlers.NonUniqueValueError(self._key, self._value, None)

class PrimaryKeyValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        try:
            validator = UniqueConstraintValidator(self._instance, self._key, self._value)
            validator.validate()
        except error_handlers.NonUniqueValueError:
            raise error_handlers.PrimaryKeyError()

class NullConstraintValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        if not self._instance._model[self._key]['nullable']:
            if self._value is None: 
                raise ValueError(f'{self._key} is not supposed to be None')

class ChoicesValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        choices = self._instance._model[self._key]['choices']
        if self._value not in choices:
            raise ValueError(f'{self._key} contains a value not in choices\nchoices: {choices}')

class MaxLengthValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        max_length = self._instance._model[self._key]['max_length']
        if len(self._value) > max_length:
            raise ValueError(f'the value of {self._key} ({self._value}) exceeds the specified length of {max_length}')

class MaxValueValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        max = self._instance._model[self._key]['max']
        if self._value > max:
            raise ValueError(f'the value of {self._key} is bigger than the specified {max}')

class RequiredConstraintValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str=None, value: str=None, *, data: dict):
        super().__init__(instance, key, value)
        self._data = data

    def validate(self):
        for key, val in self._instance._model.items():
            if 'required' in val and val['required'] and key not in self._data:
                raise error_handlers.RequiredKeyError(key)

class MinValueValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        min = self._instance._model[self._key]['min']
        if self._value < min:
            raise ValueError(f'the value of {self._key} is smaller than the specified {min}')

class DataTypeValidator(Validator):
    def __init__(self, instance: collections.Collection, key: str, value: str):
        super().__init__(instance, key, value)

    def validate(self):
        data_type = self._instance._model[self._key]['data_type']
        if not isinstance(self._value, data_type):
            raise ValueError(f'{self._key} ({self._value}) has a type of {type(self._value)} while {data_type} is expected')

class ModelValidator:
    def __init__(self, model: dict):
        self._model = model

    def validator(self):
        for val in self._model.values():
            if 'primary_key' in val:
                return self._model
            raise ValueError('model doesn\'t have a primary key')