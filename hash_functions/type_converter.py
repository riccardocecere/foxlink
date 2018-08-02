def hex_msb_to_int(hex_string):
    return int(hex_string[-2:], 16)


def bin_msb_to_int(bin_string):
    return int(bin_string[-8:], 2)