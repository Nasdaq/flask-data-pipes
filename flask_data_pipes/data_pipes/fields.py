from .utils import lowercase, uppercase, titlecase
from marshmallow import fields
from marshmallow.fields import (
    Raw,
    Nested,
    Dict,
    List,
    String,
    UUID,
    Number,
    Integer,
    Decimal,
    Boolean,
    FormattedString,
    Float,
    TimeDelta,
    Url,
    URL,
    Email,
    Method,
    Function,
    Str,
    Bool,
    Int,
    Constant
)

# implement custom field types here


class LowercaseString(fields.String):
    def _serialize(self, value, attr, obj):
        return lowercase(value)


class UppercaseString(fields.String):
    def _serialize(self, value, attr, obj):
        return uppercase(value)


class TitlecaseString(fields.String):
    def _serialize(self, value, attr, obj):
        return titlecase(value)


class Date(fields.Date):

    def _serialize(self, value, attr, obj):
        if isinstance(value, (str, type(None))):
            return value
        try:
            return value.isoformat()
        except AttributeError:
            self.fail('format', input=value)


class DateTime(fields.DateTime):

    def _serialize(self, value, attr, obj):
        if isinstance(value, (str, type(None))):
            return value
        self.dateformat = self.dateformat or self.DEFAULT_FORMAT
        format_func = self.DATEFORMAT_SERIALIZATION_FUNCS.get(self.dateformat, None)
        if format_func:
            try:
                return format_func(value, localtime=self.localtime)
            except (AttributeError, ValueError):
                self.fail('format', input=value)
        else:
            return value.strftime(self.dateformat)


class Time(fields.Time):
    def _serialize(self, value, attr, obj):
        if isinstance(value, (str, type(None))):
            return value
        try:
            ret = value.isoformat()
        except AttributeError:
            self.fail('format', input=value)
        if value.microsecond:
            return ret[:15]
        return ret


class HostName(fields.Field):

    def _serialize(self, value, attr, obj):
        try:
            value.lower()
            v = value.split('\\')

            if len(v) > 1:
                # v -> [domain, host]
                setattr(obj, '_domain', v[0])  # todo: test, will error our obj is dict
                return v[1].upper()

            v = value.split('.', 1)

            if len(v) > 1:
                # v -> [host, domain]
                setattr(obj, '_fqdn', value)
                return v[0].upper()

        except AttributeError:
            return value


class DenormalizedList(List):
    pass


class DenormalizedMethod(Method):
    pass


class DenormalizedFunction(Function):
    pass

# remove marshmallow.fields so module is not available to user
del fields
