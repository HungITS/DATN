def write_to_s3(df, path: str, format='csv'):
    print(f"Writing cleaned data to: {path} (format: {format})")
    if format == 'csv':
        df.coalesce(1).write.mode('overwrite').option("header", True).csv(path)
    else:
        df.write.mode('overwrite').format(format).save(path)

def write_to_local(df, path: str, format='csv'):
    print(f"Writing cleaned data to: {path} (format: {format})")
    if format == 'csv':
        df.coalesce(1).write.mode('overwrite').option("header", True).csv(path)
    else:
        df.write.mode('overwrite').format(format).save(path)