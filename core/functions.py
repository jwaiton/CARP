
def get_ch_mapping(rec_dict):
    '''
    Extract what channels are being used map them: ch -> index

    So shape will be:
    mapping = {0 : 0, 3 : 1, 5 : 2}
    for the case where ch0, 3, and 5 are enabled
    '''
    mapping = {}
    i = 0
    for entry in rec_dict:
        if 'ch' in entry:
            if rec_dict[entry]['enabled']:
                ch = int(entry[2:])
                mapping[ch] = i
                i += 1

    return mapping


