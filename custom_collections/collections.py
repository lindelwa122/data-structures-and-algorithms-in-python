from lists import NodeList
from . import validators

class Collection:
    def select(self, selection: str, limit: int=None):
        if not limit: limit = len(self)

        res = NodeList()
        for d in self._collection:
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
        for item in self._collection:
            if item[primary_key] == value:
                return item
        return None

    def get_all(self):
        return self._collection

    def filter(self, func: function, limit: int=None):
        if not limit: limit = len(self)

        res = NodeList()
        for item in self._collection:
            if len(res) == limit: break

            if func(item):
                res.append(item)

        return res
        
    def map(self, func: function, limit: int=None):
        if not limit: limit = len(self)

        res = NodeList()
        for item in self._collection:
            if len(res) == limit: break
            res.append(func(item))

        return res

    def reduce(self, func: function, /, start=0, limit: int=None):
        if not limit: limit = len(self)

        func_calls = 0
        for item in self._collection:
            if func_calls == limit: break
            start = func(start, item)
            func_calls += 1

        return start

    def aggregate(self, group_by: str, operation: function, attribute: str):
        grouped_data = {}
        for item in self._collection:
            key = item[group_by]
            if key not in grouped_data:
                grouped_data[key] = NodeList()
            grouped_data[key].append(item[attribute])

        return {key: operation(val) for key, val in grouped_data.items()}

    def group_by(self, group_by: str):
        grouped_data = {}
        for item in self._collection:
            key = item[group_by]
            if key not in grouped_data:
                grouped_data[key] = NodeList()
            grouped_data[key].append(item)

        return grouped_data

    def distinct(self, attribute: str):
        data = { attribute: NodeList() }
        for item in self._collection:
            if item[attribute] not in data[attribute]:
                data[attribute].append(item[attribute])

        return data

    


