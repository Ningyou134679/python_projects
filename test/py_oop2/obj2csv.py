import csv


class Item:
    all = []
    def __init__(self, name: str, quantity=0):
        assert quantity >= 0
        self.__name= name
        self.__quantity = quantity
        Item.all.append(self)


    def __repr__(self):
        return f"Item('{self.__name}',{self.__quantity})"

    @property
    def name(self):
        return self.__name

    @property
    def quantity(self):
        return self.__quantity

    @quantity.setter
    def quantity(self, val):
        self.__quantity = val

    @classmethod
    def instantiate_from_csv(cls, file: str):
        try:
            with open(file, 'r') as f:
                reader = csv.DictReader(f)
                items = list(reader)

            for item in items:
                Item(
                    name=item.get('name'),
                    quantity=int(item.get('quantity'))
                )
        except EnvironmentError as err:
            print(err)

    def __iter__(self):
        return iter([self.name, self.quantity])


Item.instantiate_from_csv('test1_write.csv')
print(Item.all)

item1 = Item("i1")
item1.quantity = 2
print(item1)

item_list1 = [Item("it1",1),Item("it2",2)]

with open('test1_write.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(["name","quantity"])
    writer.writerows(item_list1)


