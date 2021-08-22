import pandas as pd
import numpy as np

def merge_dfs(df, data):
    # добавляем столбец с конвертированной датой buy_time
    df['date_t'] = df['buy_time'].apply(lambda x: date.fromtimestamp(x))
    data['date_f'] = data['buy_time'].apply(lambda x: date.fromtimestamp(x))

    data['idx_f'] = data.index
    df['idx'] = df.index

    # объединяем по id
    df_merge = pd.merge(df, data, left_on='id', right_on='id')

    # столбец с разницей по времени
    df_merge['date_dif'] = np.abs(df_merge['date_t']-df_merge['date_f'])

    # группируем и оставляем строки, имеющие минимальную разницу
    res = df_merge.loc[df_merge.groupby(['idx'])['date_dif'].idxmin()]

    # удаляем, переименовываем столбцы
    res.drop(['buy_time_y', 'date_f', 'idx', 'idx_f', 'date_dif'], axis=1, inplace=True)

    res = res.rename(columns={'buy_time_x': 'buy_time', 'date_t': 'date'})

    res = res.reset_index(drop=True)

    return res
    
    
def reduce_mem_usage(df):
    for col in df.columns:
        col_type = df[col].dtype

        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            df[col] = df[col].astype('category')    
    return df