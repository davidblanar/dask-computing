def get_year(x):
    return x['year_month'].astype('str').str[:4]


def get_month(x):
    return x['year_month'].astype('str').str[-2:]