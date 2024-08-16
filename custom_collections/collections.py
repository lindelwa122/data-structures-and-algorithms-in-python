from lists import NodeList
from . import validators
from typing import Callable, Any
from secrets import token_hex

class Collection:
    def select(self, selection: str, /, limit: int=None, foreign_key=False):
        if not limit: limit = len(self)

        res = NodeList()
        lst = self.get_all() if foreign_key else self._collection
        for d in lst:
            if len(res) == limit: break

            field = {}
            for key, val in d.items():
                if key in selection.split():
                    field[key] = val
            res.append(field)
        return res

    def __init__(self, model: dict):
        self._validators = {
            'unique': validators.UniqueConstraintValidator,
            'primary_key': validators.PrimaryKeyValidator,
            'nullable': validators.NullConstraintValidator,
            'choices': validators.ChoicesValidator,
            'data_type': validators.DataTypeValidator,
            'max_length': validators.MaxLengthValidator,
            'max': validators.MaxValueValidator,
            'min': validators.MinValueValidator,
            'foreign_key': validators.ForeignKeyValidator,
        }

        model_validator = validators.ModelValidator(model)
        self._model = model_validator.validator()
        self._collection = NodeList()

    def __len__(self):
        return len(self._collection)

    def _get_primary_key(self):
        for key, val in self._model.items():
            if 'primary_key' in val:
                return key

    def add(self, new_data: dict):
        res = self._auto_increment(new_data)
        self._validate_data(res)
        res = self._fill_missing_data(res)
        self._collection.append(res)

    def _auto_increment(self, data: dict):
        for key, val in self._model.items():
            if 'auto_increment' in val and val['auto_increment']:
                data[key] = len(self) + 1
        return data

    def _validate_data(self, data: dict):
        for key, val in data.items():
            if key not in self._model: raise KeyError(key)

            for constraint, validator_cls in self._validators.items():
                if constraint in self._model[key]:
                    validator = validator_cls(self, key, val)
                    validator.validate()

            validator = validators.RequiredConstraintValidator(self, data=data)
            validator.validate()

        for field, constraints in self._model.items():
            if 'custom_validators' in constraints:
                custom_validators = self._model[field]['custom_validators']
                for validator_cls in custom_validators:
                    validator = validator_cls(self, field, data[field])
                    validator.validate()

        primary_key = self._get_primary_key()
        
        if primary_key not in data \
                and 'auto_increment' not in self._model[primary_key]:
            raise ValueError('data doesn\'t contain a primary key')

    def _fill_missing_data(self, data: dict):
        for key, val in self._model.items():
            if 'default' in val and key not in data:
                data[key] = val['default']
        return data

    def sort(self, key: str, reverse=False):
        self._collection.sort(lambda x: x[key], reverse)

    def get(self, value: str | int | float) -> dict | None:
        primary_key = self._get_primary_key()
        for item in self.get_all():
            if item[primary_key] == value:
                return item
        return None

    def _handle_foreign_key(self, data: dict):
        pk, copy = self._get_primary_key(), data.copy()

        for key, val in copy.items():
            constraint = self._model[key]
            if 'foreign_key' in constraint and constraint['foreign_key']:
                if not isinstance(val, Collection):
                    raise ValueError(f'foreign key must be a Collection, not {type(val).__name__}')

                copy[key] = val.filter(lambda x: x[constraint['key']] == copy[pk])

        return copy

    def get_all(self, /, limit: int=None):
        if not limit: limit = len(self)

        res = NodeList()

        for item in self._collection:
            if len(res) == limit: break
            res.append(self._handle_foreign_key(item))

        return res

    def filter(self, func: Callable[[dict], bool], limit: int=None):
        if not limit: limit = len(self)

        res = NodeList()
        for item in self.get_all():
            if len(res) == limit: break

            if func(item):
                res.append(item)

        return res
        
    def map(self, func: Callable[[dict], Any], limit: int=None):
        if not limit: limit = len(self)

        res = NodeList()
        for item in self.get_all():
            if len(res) == limit: break
            res.append(func(item))

        return res

    def reduce(self, func: Callable[[int, dict], int], /, start=0, limit: int=None):
        if not limit: limit = len(self)

        func_calls = 0
        for item in self.get_all():
            if func_calls == limit: break
            start = func(start, item)
            func_calls += 1

        return start

    def aggregate(self, group_by: str, operation: Callable[[list], Any], attribute: str):
        grouped_data = {}
        for item in self.get_all():
            key = item[group_by]
            if key not in grouped_data:
                grouped_data[key] = NodeList()
            grouped_data[key].append(item[attribute])

        return {key: operation(val) for key, val in grouped_data.items()}

    def group_by(self, group_by: str):
        grouped_data = {}
        for item in self.get_all():
            key = item[group_by]
            if key not in grouped_data:
                grouped_data[key] = NodeList()
            grouped_data[key].append(item)

        return grouped_data

    def distinct(self, attribute: str):
        data = { attribute: NodeList() }
        for item in self.get_all():
            if item[attribute] not in data[attribute]:
                data[attribute].append(item[attribute])

        return data

    def update(self, func: Callable[[dict], bool], new_update: dict):
        def tokenize_data(data: dict):
            for key in data.keys():
                data[key] = token_hex(16)
            return data

        is_updated = False
        for i, item in enumerate(self._collection):
            if func(item):
                original_data = {**self._collection[i]}
                updated = {**item, **new_update}
                # Tokenize item to avoid clashes when validating
                self._collection[i] = tokenize_data(item)
                try:
                    self._validate_data(updated)
                except:
                    self._collection[i] = original_data
                else:
                    self._collection[i] = updated
                    is_updated = True

        return is_updated

    def delete(self, func: Callable[[dict], bool]):
        indices_to_remove = NodeList()
        for i, item in enumerate(self._collection):
            if func(item):
                indices_to_remove.append(i - len(indices_to_remove))

        for index in indices_to_remove:
            del self._collection[index]


