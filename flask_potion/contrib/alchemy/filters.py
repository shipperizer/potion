from sqlalchemy import and_, inspect
from flask_potion import fields
import flask_potion.filters as filters


class SQLAlchemyBaseFilter(filters.BaseFilter):
    def __init__(self, name, field=None, attribute=None, column=None):
        super(SQLAlchemyBaseFilter, self).__init__(name, field=field, attribute=attribute)
        self.column = column

    @classmethod
    def apply(cls, query, conditions):
        expressions = [condition.filter.expression(condition.value) for condition in conditions]
        if len(expressions) == 1:
            return query.filter(expressions[0])
        return query.filter(and_(*expressions))


class EqualFilter(SQLAlchemyBaseFilter, filters.EqualFilter):
    def expression(self, value):
        return self.column == value


class NotEqualFilter(SQLAlchemyBaseFilter, filters.NotEqualFilter):
    def expression(self, value):
        return self.column != value


class LessThanFilter(SQLAlchemyBaseFilter, filters.LessThanFilter):
    def expression(self, value):
        return self.column < value


class LessThanEqualFilter(SQLAlchemyBaseFilter, filters.LessThanEqualFilter):
    def expression(self, value):
        return self.column <= value


class GreaterThanFilter(SQLAlchemyBaseFilter, filters.GreaterThanFilter):
    def expression(self, value):
        return self.column > value


class GreaterThanEqualFilter(SQLAlchemyBaseFilter, filters.GreaterThanEqualFilter):
    def expression(self, value):
        return self.column >= value


class InFilter(SQLAlchemyBaseFilter, filters.InFilter):
    def expression(self, values):
        return self.column.in_(values) if len(values) else False


class ContainsFilter(SQLAlchemyBaseFilter, filters.ContainsFilter):
    def expression(self, value):
        return self.column.contains(value)


class StringContainsFilter(SQLAlchemyBaseFilter, filters.StringContainsFilter):
    def expression(self, value):
        return self.column.like('%' + value.replace('%', '\\%') + '%')


class StringIContainsFilter(SQLAlchemyBaseFilter, filters.StringIContainsFilter):
    def expression(self, value):
        return self.column.ilike('%' + value.replace('%', '\\%') + '%')


class StartsWithFilter(SQLAlchemyBaseFilter, filters.StartsWithFilter):
    def expression(self, value):
        return self.column.startswith(value.replace('%', '\\%'))


class IStartsWithFilter(SQLAlchemyBaseFilter, filters.IStartsWithFilter):
    def expression(self, value):
        return self.column.ilike(value.replace('%', '\\%') + '%')


class EndsWithFilter(SQLAlchemyBaseFilter, filters.EndsWithFilter):
    def expression(self, value):
        return self.column.endswith(value.replace('%', '\\%'))


class IEndsWithFilter(SQLAlchemyBaseFilter, filters.IEndsWithFilter):
    def expression(self, value):
        return self.column.ilike('%' + value.replace('%', '\\%'))


class DateBetweenFilter(SQLAlchemyBaseFilter, filters.DateBetweenFilter):
    def expression(self, value):
        return self.column.between(value[0], value[1])


class AttrFilter(SQLAlchemyBaseFilter, filters.AttrFilter):

    def __init__(self, name, field=None, attribute=None, column=None):
        super(SQLAlchemyBaseFilter, self).__init__(name, field=field, attribute=attribute)
        # using column as a dict
        meta = self._get_target().meta
        self.columns = self._init_subfilters(meta) or {}

    def _init_subfilters(self, meta):
        model = meta.model or inspect(column).mapper.class_
        fields = self._get_relationship_fields()
        columns = {c: {} for c in fields.keys()}
        all_filters = filters.filters_for_fields(fields, meta.filters, FILTER_NAMES, FILTERS_BY_TYPE)
        for attr, filters_dict in all_filters.items():
            for name, filter_class in filters_dict.items():
                # attr must be a key of column dict as name of the relationship fields
                columns[attr][name] = filter_class(name, field=fields[attr], column=getattr(model, attr))
        return columns

    def expression(self, value):
        import ipdb;ipdb.set_trace()
        return False


FILTER_NAMES = (
    (EqualFilter, None),
    (EqualFilter, 'eq'),
    (NotEqualFilter, 'ne'),
    (LessThanFilter, 'lt'),
    (LessThanEqualFilter, 'lte'),
    (GreaterThanFilter, 'gt'),
    (GreaterThanEqualFilter, 'gte'),
    (InFilter, 'in'),
    (ContainsFilter, 'contains'),
    (StringContainsFilter, 'contains'),
    (StringIContainsFilter, 'icontains'),
    (StartsWithFilter, 'startswith'),
    (IStartsWithFilter, 'istartswith'),
    (EndsWithFilter, 'endswith'),
    (IEndsWithFilter, 'iendswith'),
    (DateBetweenFilter, 'between'),
    (AttrFilter, 'attr'),
)


FILTERS_BY_TYPE = (
    (fields.Boolean, (
        EqualFilter,
        NotEqualFilter,
        InFilter
    )),
    (fields.Integer, (
        EqualFilter,
        NotEqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        InFilter,
    )),
    (fields.Number, (
        EqualFilter,
        NotEqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        InFilter,
    )),
    (fields.String, (
        EqualFilter,
        NotEqualFilter,
        StringContainsFilter,
        StringIContainsFilter,
        StartsWithFilter,
        IStartsWithFilter,
        EndsWithFilter,
        IEndsWithFilter,
        InFilter,
    )),
    (fields.Date, (
        EqualFilter,
        NotEqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        DateBetweenFilter,
        InFilter,
    )),
    (fields.DateTime, (
        EqualFilter,
        NotEqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        DateBetweenFilter,
    )),
    (fields.DateString, (
        EqualFilter,
        NotEqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        DateBetweenFilter,
        InFilter,
    )),
    (fields.DateTimeString, (
        EqualFilter,
        NotEqualFilter,
        LessThanFilter,
        LessThanEqualFilter,
        GreaterThanFilter,
        GreaterThanEqualFilter,
        DateBetweenFilter,
    )),
    (fields.Array, (
        ContainsFilter,
    )),
    (fields.ToOne, (
        EqualFilter,
        NotEqualFilter,
        InFilter,
        AttrFilter,
    )),
    (fields.ToMany, (
        ContainsFilter,
        AttrFilter,
    )),
)
