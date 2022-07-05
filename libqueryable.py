import libtest

class Queryable:
    
    def __init__(self, iterable):
        self.iterable = iterable
    
    def __iter__(self):
        yield from self.iterable
    
    # Monad variants of built-in python functions on iterable.
    
    def filter(self, expression):
        return Queryable(filter(expression, self))
    
    def map(self, expression):
        return Queryable(map(expression, self))
    
    def all(self, expression):
        return all(map(expression, self))
    
    def any(self, expression):
        return any(map(expression, self))
    
    # T-SQL a-like operations
    def union_all(self, other):
        def yielder():
            yield from self
            yield from other
        return Queryable(yielder())
    
    def left_join(self, expression, other):
        def yielder():
            for left_item in self:
                def right_expression(right_item):
                    return expression(left_item, right_item)
                has_matches = False
                for right_item in filter(right_expression, other):
                    has_matches = True
                    yield (left_item, right_item)
                if not has_matches:
                    yield (left_item, None)
        return Queryable(yeilder())
    
    def cross_join(self, expression, other):
        def yielder():
            for left_item in self:
                for right_item in other:
                    yield (left_item, right_item)
        return Queryable(yeilder())
    
    # LINQ compatibility layer
    
    def select(self, expression):
        return self.map(expression)
    
    def where(self, expression):
        return self.filter(expression)
    
    def distinct(self):
        def yielder():
            yield from set(self)
        return Queryable(yielder())
    
    def skip(self, count):
        def yielder():
            for index, item in enumerate(self):
                if index > count:
                    yield item
        return Queryable(yielder())
    
    def take(self, count):
        def yielder():
            for index, item in enumerate(self):
                if index < count:
                    yield item
        return Queryable(yielder())
    
    def order_by(self, expression):
        def yielder():
            as_list = list(self.iterable)
            as_list.sort(key=expression)
            yield from as_list
        return Queryable(yielder())
    
    def sorted(self):
        return self.order_by(lambda x: x)
    
    def to_list(self):
        return list(self)

def __test__():
    data = Queryable([1,6,34,5,8,5,3,4,7,5,3])
    print(data.filter(lambda x: x % 2 == 0).map(lambda x: x * 10).skip(1).take(3).to_list())
    print(data.distinct().sorted().to_list())
    
    data2 = Queryable(['b','j','f','x','u','e','j','j'])
    print(data.union_all(data2).to_list())
    
    # Alternate forms
    print(data.all(lambda x: str(x).isdigit()))
    print(data.map(str).all(str.isdigit))
#    print(data.map(str).all().isdigit())
    
    # Alternate forms
    print(data2.any(lambda x: x.isdigit()))
    print(data2.any(str.isdigit))
#    print(data.any().isdigit())

if __name__ == "__main__":
    __test__()
