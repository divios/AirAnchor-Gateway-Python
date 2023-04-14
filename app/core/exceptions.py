

class Sawtooth_back_pressure_exception(Exception):
    pass

class Sawtooth_invalid_transaction_format(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__("Malformed batch")