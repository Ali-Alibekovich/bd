def transform_category(obj_id, row):
    return {
        'category_unique_id': obj_id,
        'name': row.get('name'),
        'percent': row.get('percent'),
        'min_payment': row.get('min_payment')
    }


def transform_restaurant(obj_id, row):
    return {
        'restaurant_unique_id': obj_id,
        'name': row['name'],
        'phone': row['phone'],
        'email': row['email'],
        'founding_day': row['founding_day'],
        'menu': row['menu']
    }


def transform_dish(obj_id, row):
    return {
        'dish_unique_id': obj_id,
        'name': row['name'],
        'price': row['price']
    }


def transform_deliveryman(obj_id, row):
    return {
        'deliveryman_unique_id': obj_id,
        'name': row['name'],
    }