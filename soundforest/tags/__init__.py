"""Tag processing

Audio file tag, albumart and tag output processing classes

"""

import sys

class TagError(Exception):
    pass


def format_unicode_string_value(value):
    """Try to value as unicode

    This is a helper to get values to unicode, whether python2 or python3 is used
    """
    try:
        if sys.version_info.major >= 3:
            if not isinstance(value, str):
                if isinstance(value, int):
                    return str('{0:d}'.format(value))
                else:
                    return str(value, 'utf-8')
            return value
        else:
            if not isinstance(value, unicode):
                if isinstance(value, int):
                    return unicode('{0:d}'.format(value))
                else:
                    return unicode(value, 'utf-8')
            return value
    except Exception as e:
        print(e)
        raise ValueError('Error converting value to unicode: {0}: {1}'.format(value, e))
