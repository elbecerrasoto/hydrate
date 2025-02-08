#!/usr/bin/env python


def is_empty(l: list) -> bool:
    if not isinstance(l, list): # atom
        return False
    elif len(l) == 0: # empty list
        return True
    else:
        return all([is_empty(i) for i in l])
