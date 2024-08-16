class NonUniqueValueError(ValueError):
    def __init__(self, key: str=None, value: str=None, /, msg: str=None):
        if not msg: msg = f'the unique contraint has been violated ({key} -> {value})'
        super().__init__(msg)

class PrimaryKeyError(NonUniqueValueError):
    def __init__(self):
        msg = 'primary key field is expected to have unique values, this query violates this constraint'
        super().__init__(msg=msg)

class RequiredKeyError(ValueError):
    def __init__(self, key: str):
        msg = f'the key ({key}) has required constraint but you seem to have forgotten to include it'
        super().__init__(msg)

class KeyError(ValueError):
    def __init__(self, key: str):
        msg = f'the field: {key} is not in the class model'
        super().__init__(msg)