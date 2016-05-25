from flask_potion import fields
import flask_potion.filters as filters


class CQLEngineBaseFilter(filters.BaseFilter):
    def __init__(self, name, field=None, attribute=None, column=None):
        super(CQLEngineBaseFilter, self).__init__(name, field=field, attribute=attribute)
        self.column = column


class EqualFilter(CQLEngineBaseFilter, filters.EqualFilter):
    def expression(self, query, value):
        return query.filter(self.column == value).allow_filtering()


class LessThanFilter(CQLEngineBaseFilter, filters.LessThanFilter):
    def expression(self, query, value):
        return query.filter(self.column < value).allow_filtering()


class LessThanEqualFilter(CQLEngineBaseFilter, filters.LessThanEqualFilter):
    def expression(self, query, value):
        return query.filter(self.column <= value).allow_filtering()


class GreaterThanFilter(CQLEngineBaseFilter, filters.GreaterThanFilter):
    def expression(self, query, value):
        return query.filter(self.column > value).allow_filtering()


class GreaterThanEqualFilter(CQLEngineBaseFilter, filters.GreaterThanEqualFilter):
    def expression(self, query, value):
        return query.filter(self.column >= value).allow_filtering()


class InFilter(CQLEngineBaseFilter, filters.InFilter):
    def expression(self, query, values):
        return query.filter(self.column.in_(values)).allow_filtering()


class ContainsFilter(CQLEngineBaseFilter, filters.ContainsFilter):
    def expression(self, query, value):
        return query.filter(self.column.contains(value)).allow_filtering()


# class StringContainsFilter(CQLEngineBaseFilter, filters.StringContainsFilter):
#     def expression(self, value):
#         return self.column.like('%' + value.replace('%', '\\%') + '%')
#
#
# class StringIContainsFilter(CQLEngineBaseFilter, filters.StringIContainsFilter):
#     def expression(self, value):
#         return self.column.ilike('%' + value.replace('%', '\\%') + '%')
#
#
# class StartsWithFilter(CQLEngineBaseFilter, filters.StartsWithFilter):
#     def expression(self, value):
#         return self.column.startswith(value.replace('%', '\\%'))
#
#
# class IStartsWithFilter(CQLEngineBaseFilter, filters.IStartsWithFilter):
#     def expression(self, value):
#         return self.column.ilike(value.replace('%', '\\%') + '%')
#
#
# class EndsWithFilter(CQLEngineBaseFilter, filters.EndsWithFilter):
#     def expression(self, value):
#         return self.column.endswith(value.replace('%', '\\%'))
#
#
# class IEndsWithFilter(CQLEngineBaseFilter, filters.IEndsWithFilter):
#     def expression(self, value):
#         return self.column.ilike('%' + value.replace('%', '\\%'))
#
#
# class DateBetweenFilter(CQLEngineBaseFilter, filters.DateBetweenFilter):
#     def expression(self, value):
#         return self.column.between(value[0], value[1])
#
#

FILTER_NAMES = (
    (EqualFilter, None),
    (EqualFilter, 'eq'),
    (LessThanFilter, 'lt'),
    (LessThanEqualFilter, 'lte'),
    (GreaterThanFilter, 'gt'),
    (GreaterThanEqualFilter, 'gte'),
    (InFilter, 'in'),
    # (ContainsFilter, 'contains'),
)
FILTERS_BY_TYPE = (
    (fields.Boolean, (
        EqualFilter,
        InFilter
    )),
    (fields.Integer, (
        EqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        InFilter,
    )),
    (fields.Number, (
        EqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        InFilter,
    )),
    (fields.String, (
        EqualFilter,
        # StringContainsFilter,
        # StringIContainsFilter,
        # StartsWithFilter,
        # IStartsWithFilter,
        # EndsWithFilter,
        # IEndsWithFilter,
        InFilter,
    )),
    (fields.Date, (
        EqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        # DateBetweenFilter,
        InFilter,
    )),
    (fields.DateTime, (
        EqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        # DateBetweenFilter,
    )),
    (fields.DateString, (
        EqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        InFilter,
    )),
    (fields.DateTimeString, (
        EqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
    )),
    (fields.Array, (
        ContainsFilter,
    ))
)
