from constants import formats
from persistence.pandas import Cleaner

tgt_store = Cleaner(name="kuna_btcuah", columns=formats.history_format, time_unit="s", x_shift_hours=2)

tgt_store.clean()
