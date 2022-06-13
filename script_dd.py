import os
import dask.dataframe as dd
from dask.distributed import Client
import const
import util


def small_df():
    df = dd.read_csv(os.path.abspath("data/frac.csv"), blocksize="25MB")[const.COLUMNS]
    return df


def large_df():
    df = dd.read_csv(os.path.abspath("data/custom_1988_2020.csv"), blocksize="25MB")
    df.columns = const.COLUMNS
    return df


if __name__ == '__main__':
    client = Client(memory_limit='3GB', n_workers=1, threads_per_worker=2)
    # df = small_df()
    df = large_df()

    df = df.assign(year=lambda x: util.get_year(x), month=lambda x: util.get_month(x))

    exports = df[df['export_import'] == 1]
    imports = df[df['export_import'] == 2]

    exports_by_year = exports.groupby('year').agg({'value_yen': sum})
    imports_by_year = imports.groupby('year').agg({'value_yen': sum})

    joined = exports_by_year.join(imports_by_year, lsuffix='_export', rsuffix='_import').reset_index()
    joined['balance'] = joined['value_yen_export'] - joined['value_yen_import']
    joined.to_csv(os.path.abspath("output/dask.csv"), single_file=True)

    client.close()
