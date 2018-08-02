from random import shuffle

example_table = range(0, 256)
shuffle(list(example_table))


def apply(shingle):
    h = len(shingle) % 256
    for i in shingle:
        h = example_table[(h + ord(i)) % 256]
    return h
