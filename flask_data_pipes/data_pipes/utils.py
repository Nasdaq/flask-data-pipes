from datetime import datetime
from collections import Iterable

from werkzeug.datastructures import ImmutableMultiDict
from sqlalchemy.exc import DatabaseError
from flask import request

import ipaddress
import hashlib
import re
import errno
import shutil
import os


TASK_KEYS = ['upload', 'extract', 'transform', 'load']


class AttrDict(dict):

    def __init__(self, **kwargs):
        super(AttrDict, self).__init__(**kwargs)

    def __getattr__(self, item):
        try:
            return self.__getitem__(item)

        except KeyError:
            raise AttributeError

    def __setattr__(self, key, value):
        return self.__setitem__(key, value)


def sha256(string):
    return hashlib.sha224(string.encode('utf-8')).hexdigest()


def denormalize(obj, key):
    try:
        if not isinstance(obj[key], Iterable) or isinstance(obj[key], str):
            return obj
        elif not bool(obj[key]):
            obj[key] = None
            return obj

    except KeyError:
        return obj

    collection = obj.pop(key)
    denormalized = []
    for item in collection:
        denormalized.append({key: item, **obj})

    return denormalized


def pass_through(data):
    return data


def strip_trailing_slash(data: str):
    try:
        if data[-1] == '/':
            return data[:-1]
        else:
            pass

    except TypeError:
        pass
    else:
        return data


def logged_query(engine, sql_string, raw=False):
    _results = engine.execute(sql_string)

    if raw:
        return _results, datetime.utcnow()
    else:
        return _results.fetchall(), datetime.utcnow()


def splitter(string, left=False, right=False):
    """Split 'domain\cn'  or 'domain\computer' values and return left and/or right.

    Can be used with tuple unpacking if left/right not set, but should be wrapped in
    a try-except to catch ValueError in the case split returns 1 string.
    """
    try:
        split = string.split('\\')

        if right:
            return split[-1]
        elif left:
            return split[0]
        else:
            return split

    except AttributeError:
        return None if right or left else (None, None)


def lowercase(string: str):
    """Safely recast a string to lowercase"""
    try:
        return string.lower()

    except AttributeError:
        return string


def uppercase(string: str):
    """Safely recast a string to uppercase"""
    try:
        return string.upper()

    except AttributeError:
        return string


def titlecase(string: str):
    """Safely recast a string to title case"""
    try:
        return string.title()

    except AttributeError:
        return string


def booler(string: str):
    """Because there's nothing like the real thing.

    Returns the logical bool from an integer or string else raises a ValueError; NoneType and empty stings return None.
    """
    if string in [None, '']:
        return None
    elif str(string).lower() in ['yes', 'y', '1', 'true', 't']:
        return True
    elif str(string).lower() in ['no', 'n', '0', 'false', 'f']:
        return False
    else:
        raise ValueError("Unexpected string: cannot covert '{0}' to a bool".format(string))


def recast_null(string: str):
    """Recast empty strings to NoneType"""
    if string in [None, '']:
        return None
    else:
        return string


def recast_timestamp(ms: int):
    """Recast millisecond epoch offsets to DateTime"""
    try:
        return datetime.fromtimestamp(ms / 1000.0)

    except TypeError:
        return None


def recast_ip(ip: int):
    """Recast ip address integers to a dotted quad string"""
    try:
        return str(ipaddress.ip_address(ip))

    except ValueError:
        return None


def recast_mac(address: str):
    """Recast mac address strings as a colon-separated string"""
    try:
        return address.replace('-', ':')

    except AttributeError:
        return address


def camel_to_snake_case(name):
    _camelcase_re = re.compile(r'([A-Z]+)(?=[a-z0-9])')

    def _join(match):
        word = match.group()

        if len(word) > 1:
            return ('_%s_%s' % (word[:-1], word[-1])).lower()

        return '_' + word.lower()

    return _camelcase_re.sub(_join, name).lstrip('_')


def flatten(collection):

    for item in collection:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for sub_collection in flatten(item):
                yield sub_collection
        else:
            yield item

single_address = ipaddress.ip_address('255.255.255.255')


def explode_ip_addrs(data: list):
    for ip in flatten(data):
        try:
            interface = ipaddress.ip_interface(ip)
            if interface.netmask != single_address:
                for address in interface.network.hosts():
                    yield address
            else:
                yield interface.ip
        except ValueError:
            continue


def ip_to_int(address: str):
    try:
        return int(ipaddress.ip_interface(address))
    except ValueError:
        return None


def ip_to_int_sqla(context, column='ip_address'):
    return ip_to_int(context.get_current_parameters()[column])


def mk_or_replace_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            shutil.rmtree(path)
            mk_or_replace_dir(path)
        else:
            raise


def update_request_form(**kwargs):
    """Why: because request forms are immutable."""
    form = request.form.to_dict()
    form.update(**kwargs)
    request.form = ImmutableMultiDict(form)


def db_cautious_interaction(func):
    from flask import current_app as app
    db = app.extensions['sqlalchemy'].db

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabaseError:
            db.session.close()
            return func(*args, **kwargs)

    return wrapper


# todo: implement variable file names via format strings
# class Filename(object):
#
#     @property
#     def date(self):
#         return datetime.utcnow().strftime('%Y%m%d')  # YYYYMMDD
#
#     @property
#     def datetime(self):
#         return datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')  # YYYYMMDDTHHMMSSssssss
#
#     @property
#     def descriptor(self):
#         return None
