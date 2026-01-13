import tables as tb
from typing import Type


class config_class(tb.IsDescription):
     '''
     Holds dictionary (key, value) pairs
     '''
     key   = tb.StringCol(90) # 90 character string maximum, you've been warned!
     value = tb.StringCol(90)



def return_rwf_class(WD_version : str, shape : int) -> Type[tb.IsDescription]:
    '''
    Based on MULE shapes, expect output to be formatted as such, for forwards compatibility.
    '''
    if WD_version == 1:
        class rwf_df(tb.IsDescription):
            evt_no    = tb.UInt32Col()
            channel   = tb.UInt32Col()
            timestamp = tb.UInt64Col()
            rwf       = tb.UInt16Col(shape=(shape,))
    elif WD_version == 2:
        class rwf_df(tb.IsDescription):
            evt_no    = tb.UInt32Col()
            channel   = tb.UInt32Col()
            timestamp = tb.UInt64Col()
            rwf       = tb.Float32Col(shape = (shape,))

    return rwf_df
