from sqlalchemy import Table, MetaData, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector

from jet_bridge_base.db import get_engine, create_session, get_mapped_base

materialized_views_metadata = MetaData()


def get_materialized_views(request):
    engine = get_engine(request)
    metadata = get_mapped_base(request).metadata

    Base = declarative_base(metadata=metadata)
    insp = Inspector.from_engine(engine)

    mvs = {}
    for view_name in insp.get_view_names():
        view_columns = insp.get_columns(view_name)
        pk_column_name = view_columns[0]['name']  # just select first column as pk

        table = Table(
            view_name,
            materialized_views_metadata,
            PrimaryKeyConstraint(pk_column_name, name=f'{view_name}_pk'),
            extend_existing=True,
            autoload_with=engine,
        )
        orm = type(view_name, (Base,), {
            '__table__': table,
        })

        mvs[view_name] = orm

    return mvs