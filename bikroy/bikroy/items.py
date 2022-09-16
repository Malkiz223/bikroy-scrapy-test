from scrapy import Item, Field


class ProductItem(Item):
    url = Field()
    item_id = Field()
    creation_timestamp = Field()
    author_name = Field()
    author_phone = Field()
    price = Field()
    images = Field()
    metadata = Field()
    address = Field()
