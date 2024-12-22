import pandas as pd
import numpy as np

# 读取Excel文件
def process_date(file_path):
    df = pd.read_excel(file_path)

    # 解析日期时间，并转换为pandas的datetime格式
    df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d %H:%M:%S')

    # 将时间戳四舍五入到最近的10分钟
    df['date'] = df['date'].dt.round('10min')

    # 处理重复数据：将每10分钟内的数据取平均值
    df = df.groupby('date').mean().reset_index()

    # 设置日期时间为索引
    df.set_index('date', inplace=True)

    # 确定数据的起始和结束时间，并生成完整的10分钟间隔时间索引
    start_time = df.index.min().floor('10min')
    end_time = df.index.max().ceil('10min')
    full_time_index = pd.date_range(start=start_time, end=end_time, freq='10min')

    # 重新索引数据
    df = df.reindex(full_time_index)

    # 标记缺失的时间戳
    missing = df.isnull().any(axis=1)

    # 创建一个布尔序列，表示每个时间点是否缺失
    missing_series = missing

    # 识别单个缺失的时间戳（非连续的）
    # 单个缺失的条件：当前缺失，前一个和后一个时间点都不缺失
    single_missing = missing_series & ~missing_series.shift(1).fillna(False) & ~missing_series.shift(-1).fillna(False)

    single_missing_times = df[single_missing].index

    # 填补单个缺失的时间戳
    for timestamp in single_missing_times:
        prev_timestamp = timestamp - pd.Timedelta(minutes=10)
        next_timestamp = timestamp + pd.Timedelta(minutes=10)

        if prev_timestamp in df.index and next_timestamp in df.index:
            prev_values = df.loc[prev_timestamp]
            next_values = df.loc[next_timestamp]
            # 检查前后时间点的数据是否完整
            if not prev_values.isnull().any() and not next_values.isnull().any():
                filled_values = (prev_values + next_values) / 2
                df.loc[timestamp] = filled_values

            else:
                # 如果前后数据中有缺失，尝试前向或后向填充
                if not prev_values.isnull().any():
                    df.loc[timestamp] = prev_values
                elif not next_values.isnull().any():
                    df.loc[timestamp] = next_values
                # 如果前后都缺失，则保持为NaN
        else:
            # 如果无法获取前后数据，可以选择其他填补方法，例如前向填充或后向填充
            if prev_timestamp in df.index and not df.loc[prev_timestamp].isnull().any():
                df.loc[timestamp] = df.loc[prev_timestamp]
            elif next_timestamp in df.index and not df.loc[next_timestamp].isnull().any():
                df.loc[timestamp] = df.loc[next_timestamp]
            # 如果前后数据都不可用，保持为NaN

    # 现在，剩下的缺失时间点是连续缺失的，删除这些行
    df_cleaned = df[~df.isnull().any(axis=1)].copy()

    # 重置索引，将时间戳作为一列
    df_cleaned.reset_index(inplace=True)
    df_cleaned.rename(columns={'index': 'date'}, inplace=True)

    # 添加time_idx列
    df_cleaned['time_idx'] = np.arange(len(df_cleaned))

    # 将time_idx放到第一列
    cols = df_cleaned.columns.tolist()
    cols = ['time_idx'] + [col for col in cols if col != 'time_idx']
    df_cleaned = df_cleaned[cols]

    # 保存预处理后的数据到新的Excel文件
    output_file_path = 'processed_data.csv'  # 替换为你想要保存的路径
    df_cleaned.to_csv(output_file_path, index=False)

    print("数据预处理完成，已保存到", output_file_path)


filePath = "data.xlsx"
process_date(filePath)




