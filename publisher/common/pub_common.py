# publisher_common.py
import pathway as pw

NATS_URI = "nats://localhost:4222"
ORIGINAL_FILE = "fraudTrain.csv"
TEMP_STREAM_FILE = "fraud_stream.csv"

class TransactionSchema(pw.Schema):
    trans_num: str = pw.column_definition(dtype=str)
    trans_date_trans_time: str = pw.column_definition(dtype=str)
    cc_num: int = pw.column_definition(dtype=int)
    merchant: str = pw.column_definition(dtype=str)
    category: str = pw.column_definition(dtype=str)
    amt: float = pw.column_definition(dtype=float)
    first: str = pw.column_definition(dtype=str)
    last: str = pw.column_definition(dtype=str)
    gender: str = pw.column_definition(dtype=str)
    street: str = pw.column_definition(dtype=str)
    city: str = pw.column_definition(dtype=str)
    state: str = pw.column_definition(dtype=str)
    zip: int = pw.column_definition(dtype=int)
    lat: float = pw.column_definition(dtype=float)
    long: float = pw.column_definition(dtype=float)
    city_pop: int = pw.column_definition(dtype=int)
    job: str = pw.column_definition(dtype=str)
    dob: str = pw.column_definition(dtype=str)
    unix_time: int = pw.column_definition(dtype=int)
    merch_lat: float = pw.column_definition(dtype=float)
    merch_long: float = pw.column_definition(dtype=float)
    is_fraud: int = pw.column_definition(dtype=int)
