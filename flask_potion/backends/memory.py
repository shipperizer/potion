from __future__ import division
from ..exceptions import ItemNotFound
from . import Relation, Manager, Pagination
from ..signals import before_add_to_relation, after_add_to_relation, before_remove_from_relation, \
    after_remove_from_relation
from ..utils import get_value


class MemoryRelation(Relation):

    def instances(self, item, page=None, per_page=None):
        collection = item.get(self.attribute, set())

        items = []
        for id in collection:
            try:
                items.append(self.target_resource.manager.read(id))
            except ItemNotFound:
                collection.remove(id)
                pass

        return Pagination.from_list(items, page, per_page)

    def add(self, item, target_item):
        before_add_to_relation.send(self.resource, item=item, attribute=self.attribute, child=target_item)
        item[self.attribute] = collection = item.get(self.attribute, set())
        item_id = target_item[self.target_resource.manager.id_attribute]
        collection.add(item_id)
        after_add_to_relation.send(self.resource, item=item, attribute=self.attribute, child=target_item)

    def remove(self, item, target_item):
        before_remove_from_relation.send(self.resource, item=item, attribute=self.attribute, child=target_item)
        item[self.attribute] = collection = item.get(self.attribute, set())
        item_id = target_item[self.target_resource.manager.id_attribute]
        collection.remove(item_id)
        after_remove_from_relation.send(self.resource, item=item, attribute=self.attribute, child=target_item)


class MemoryManager(Manager):
    relation_type = MemoryRelation
    supported_comparators = ('$eq', '$ne', '$lt', '$gt', '$le', '$ge', '$in', '$startswith', '$endswith')

    def __init__(self, resource, model):
        super(MemoryManager, self).__init__(resource, model)
        self.id_attribute = resource.meta.get('id_attribute', 'id')
        self.id_sequence = 0
        self.items = {}
        self.session = []

    def _new_item_id(self):
        self.id_sequence += 1
        return self.id_sequence

    @staticmethod
    def _filter_items(items, where):
        for item in items:
            # TODO ensure condition uses the field attribute, not the post attribute
            if all(condition(item) for condition in where):
                yield item

    @staticmethod
    def _sort_items(items, sort):
        for key, reverse in reversed(sort):
            items = sorted(items, key=lambda item: get_value(key, item, None), reverse=reverse)
        return items

    def _paginate(self, items, page, per_page):
        return Pagination.from_list(list(items), page, per_page)

    def paginated_instances(self, page, per_page, where=None, sort=None):
        return self._paginate(self.instances(where=where, sort=sort), page, per_page)

    def instances(self, where=None, sort=None):
        items = self.items.values()

        if where is not None:
            items = self._filter_items(items, where)
        if sort is not None:
            items = self._sort_items(items, sort)

        return items

    def create(self, properties, commit=True):
        item_id = self._new_item_id()
        item = dict({self.id_attribute: item_id})
        item.update(properties)

        if commit:
            self.items[item_id] = item
        else:
            self.session[item_id] = item

        return item

    def read(self, id):
        try:
            item = self.items[id]
        except KeyError:
            raise ItemNotFound(self.resource, id=id)

        return item

    def update(self, item, changes, commit=True):
        item_id = item[self.id_attribute]
        item = dict(item)
        item.update(changes)

        if commit:
            self.items[item_id] = item
        else:
            self.session.append((item_id, item))

        return item

    def delete(self, item):
        item_id = item[self.id_attribute]
        del self.items[item_id]

    def commit(self):
        for item_id, item in self.session:
            self.items[item_id] = item

    def begin(self):
        self.session = []