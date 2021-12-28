import pandas as pd
from sqlalchemy import create_engine, text


def full_load(df_full_load, df_cols, pg_tgt_conn):
    for ind in df_full_load.index:  # Iterating full load tables
        table_name = df_full_load['src_name'][ind]

        col_df = df_cols[df_cols['src_tab'] == table_name]
        src_col_list = ','.join(col_df['src_col'].to_list())

        src_connection_url = df_full_load['url'][ind]
        src_df = pd.read_sql(f'select {src_col_list} from {table_name}', src_connection_url)
        src_df.to_sql(table_name, pg_tgt_conn, if_exists='replace', index=False)


def upsert_load(df_upsert, df_cols, pg_tgt_conn):
    for ind in df_upsert.index:  # Iterating upsert tables
        table_name = df_upsert['src_name'][ind]

        col_df = df_cols[df_cols['src_tab'] == table_name]
        src_col_list = col_df['src_col'].to_list()
        col_list_str = ','.join(src_col_list)

        src_connection_url = df_upsert['url'][ind]
        src_df = pd.read_sql(f'select {col_list_str} from {table_name}', src_connection_url)

        engine = create_engine(pg_tgt_conn)
        query = text(f"""
                    INSERT INTO {table_name}({src_col_list[0]}, {src_col_list[1]}, {src_col_list[2]})
                    VALUES {','.join([str(i) for i in list(src_df.to_records(index=False))])}
                    ON CONFLICT ({src_col_list[0]})
                    DO  UPDATE SET {src_col_list[1]}= excluded.{src_col_list[1]},
                                   {src_col_list[2]}= excluded.{src_col_list[2]}
             """)
        engine.execute(query)


def incremental_load(df_inc, df_cols, pg_tgt_conn, pg_md_conn):
    for ind in df_inc.index:  # Iterating inc. load tables
        table_name = df_inc['src_name'][ind]

        col_df = df_cols[df_cols['src_tab'] == table_name]
        src_col_list = col_df['src_col'].to_list()
        col_list_str = ','.join(src_col_list)

        src_connection_url = df_inc['url'][ind]  # lsev=LastSucessfulExtractedIdentityValue
        src_df = pd.read_sql(f'select {col_list_str} from {table_name} '
                             f'where {df_inc["pk_col"][ind]} > {df_inc["lsev"][ind]}', src_connection_url)
        src_df.to_sql(table_name, pg_tgt_conn, if_exists='append', index=False)

        max_pk = src_df[df_inc["pk_col"][ind]].max()
        if not pd.isna(max_pk):
            engine = create_engine(pg_md_conn)
            query = text(f"""
                        UPDATE md_source_type SET lsev = {max_pk}
                        WHERE sno = {df_inc['sno'][ind]}
                 """)
            engine.execute(query)


def main():
    pg_md_conn = 'postgresql://postgres:itversity@localhost:5452/postgres'
    df_src_type = pd.read_sql('SELECT * FROM md_source_type', pg_md_conn)

    df_full_load = df_src_type[(df_src_type.jid == 1) & (df_src_type.src_type == 'table')]
    df_upsert = df_src_type[(df_src_type.jid == 3) & (df_src_type.src_type == 'table')]
    df_inc = df_src_type[(df_src_type.jid == 2) & (df_src_type.src_type == 'table')]

    df_cols = pd.read_sql('SELECT * FROM md_col_details', pg_md_conn)

    pg_tgt_conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'

    full_load(df_full_load, df_cols, pg_tgt_conn)
    upsert_load(df_upsert, df_cols, pg_tgt_conn)
    incremental_load(df_inc, df_cols, pg_tgt_conn, pg_md_conn)


if __name__ == '__main__':
    main()
