from cassandra.cqlengine.query import DoesNotExist, LWTException
from cassandra.cqlengine import columns, ValidationError
from flask import current_app
from flask_potion import fields
from flask_potion.exceptions import ItemNotFound, DuplicateKey, BackendConflict
from flask_potion.manager import RelationalManager
from flask_potion.signals import before_create, after_create, before_update, after_update, before_delete, after_delete
from flask_potion.utils import get_value
from flask_potion.contrib.cqlengine.filters import FILTER_NAMES, FILTERS_BY_TYPE


class CQLEngineManager(RelationalManager):
    """
    A manager for CQLEngine models.

    # Expects that ``Meta.model`` contains a CQLEngine declarative model.

    """
    FILTER_NAMES = FILTER_NAMES
    FILTERS_BY_TYPE = FILTERS_BY_TYPE

    def __init__(self, resource, model):
        super(CQLEngineManager, self).__init__(resource, model)

    def _init_model(self, resource, model, meta):
        self.model = model

        pk_name = model._pk_name

        if meta.id_attribute:
            self.id_column = getattr(model, resource.meta.id_attribute)
            self.id_attribute = meta.id_attribute
        else:
            self.id_column = model._primary_keys[pk_name]
            self.id_attribute = pk_name

        self.id_field = self._get_field_from_column_type(self.id_column, self.id_attribute, io="r")

        fs = resource.schema
        if meta.include_id:
            fs.set('$id', self.id_field)
        else:
            fs.set('$uri', fields.ItemUri(resource, attribute=self.id_attribute))

        if meta.include_type:
            fs.set('$type', fields.ItemType(resource))

        # resource name: use model table's name if not set explicitly
        if not hasattr(resource.Meta, 'name'):
            if model.__table_name__:
                meta['name'] = model.__table_name__.lower()
            else:
                meta['name'] = model.__name__.lower()

        fs = resource.schema
        include_fields = meta.get('include_fields', None)
        exclude_fields = meta.get('exclude_fields', None)
        read_only_fields = meta.get('read_only_fields', ())
        write_only_fields = meta.get('write_only_fields', ())
        pre_declared_fields = {f.attribute or k for k, f in fs.fields.items()}

        for name, column in model._columns.items():
            if (include_fields and name in include_fields) or \
                    (exclude_fields and name not in exclude_fields) or \
                    not (include_fields or exclude_fields):
                if column.is_primary_key:
                    continue
                if name in pre_declared_fields:
                    continue

                io = "rw"
                if name in read_only_fields:
                    io = "r"
                elif name in write_only_fields:
                    io = "w"

                if column.required or not column.default:
                    fs.required.add(name)
                fs.set(name, self._get_field_from_column_type(column, name, io=io))

    @staticmethod
    def _get_field_from_cassandra_type(db_type):
        # http://datastax.github.io/python-driver/getting_started.html#type-conversions
        try:
            return {
                'boolean': bool,
                'counter': int,
                'decimal': float,
                'double': float,
                'float': float,
                'inet': str,
                'int': int,
                'list': list,
                'map': dict,
                'set': list,
                'text': str,
                'varchar': str,
                'varint': int
            }[db_type]
        except KeyError:
            raise RuntimeError('No appropriate field class for "{}" type found'.format(db_type))

    def _get_field_from_column_type(self, column, attribute, io="rw"):
        args = ()
        kwargs = {}

        if isinstance(column, columns.UUID) or isinstance(column, columns.TimeUUID):
            field_class = fields.UUID
        elif isinstance(column, columns.DateTime):
            field_class = fields.DateTime
        else:
            cdb_type = self._get_field_from_cassandra_type(column.db_type)
            field_class = self._get_field_from_python_type(cdb_type)
        kwargs['nullable'] = not column.required

        if column.default is not None:
            kwargs['default'] = column.get_default()

        return field_class(*args, io=io, attribute=attribute, **kwargs)

    def _init_filter(self, filter_class, name, field, attribute):
        return filter_class(name,
                            field=field,
                            attribute=field.attribute or attribute,
                            column=getattr(self.model, field.attribute or attribute))

    def _is_sortable_field(self, field):
        if super(CQLEngineManager, self)._is_sortable_field(field):
            return True
        else:
            return False

    def _query(self):
        return self.model.objects

    def _query_filter(self, query, expression):
        return query.filter(expression)

    def _expression_for_condition(self, condition):
        return condition.filter.expression(condition.value)

    def _expression_for_ids(self, ids):
        return self.id_column.in_(ids)


    def _query_filter_by_id(self, query, id):
        try:
            return query.filter(self.id_column == id).get()
        except DoesNotExist:
            raise ItemNotFound(self.resource, id=id)

    def _query_order_by(self, query, sort):
        order_clauses = []

        for field, attribute, reverse in sort:
            column = getattr(self.model, attribute)

            order_clauses.append('-{}'.format(column) if reverse else column)

        return query.order_by(*order_clauses)

    def _query_get_paginated_items(self, query, page, per_page):
        # can't find a nice way to use pagination with cassandra driver
        # therefore using splicing
        return query.all()[per_page * (page - 1):per_page * page]

    def _query_get_all(self, query):
        return query.all()

    def _query_get_one(self, query):
        return query.get()

    def _query_get_first(self, query):
        try:
            return query.first()
        except DoesNotExist:
            raise IndexError()

    def create(self, properties, commit=True):
        # noinspection properties
        item = self.model()

        for key, value in properties.items():
            setattr(item, key, value)

        before_create.send(self.resource, item=item)

        if commit:
            try:
                item.if_not_exists().save()
            except LWTException as e:
                raise DuplicateKey(detail=e.existing)
            except ValidationError as e:
                if current_app.debug:
                    raise BackendConflict(debug_info=dict(statement=e.statement, params=e.params))
                raise BackendConflict()

        after_create.send(self.resource, item=item)
        return item

    def update(self, item, changes, commit=True):
        actual_changes = {
            key: value for key, value in changes.items()
            if get_value(key, item, None) != value
        }

        try:
            before_update.send(self.resource, item=item, changes=actual_changes)

            for key, value in changes.items():
                setattr(item, key, value)

            if commit:
                item.if_exists().save()
        except LWTException:
            raise IndexError()
        except ValidationError as e:
            if current_app.debug:
                raise BackendConflict(debug_info=dict(statement=e.statement, params=e.params))
            raise BackendConflict()

        after_update.send(self.resource, item=item, changes=actual_changes)
        return item

    def delete(self, item):
        before_delete.send(self.resource, item=item)

        item.delete()

        after_delete.send(self.resource, item=item)
