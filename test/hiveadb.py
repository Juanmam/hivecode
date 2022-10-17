### METASTORE
# read_table("cndentidad") # Funciona

# Funciona
# df = read_excel("test.xlsx", "/mnt/raw-zone/test/")
# df = df.drop("Unnamed: 0")
# to_table(df, "test", "testingdb")

### DELTA
# read_delta("test.delta", "/mnt/raw-zone/test/")   # Funciona
# to_delta(df, "test.delta", "/mnt/raw-zone/test/") # Funciona

### Parquet
# read_parquet("test.parquet", "/mnt/raw-zone/test/")   # Funciona
# to_parquet(df, "test.parquet", "/mnt/raw-zone/test/") # Funciona

### EXCEL
# read_excel("test.xlsx", "/mnt/raw-zone/test/")   # Funciona

# to_excel(df, "test.xlsx", "/mnt/raw-zone/test/") # Funciona

### JSON
# read_json("test.json", "/mnt/raw-zone/test/")    # Funciona
# to_json(df, "test.json", "/mnt/raw-zone/test/")  # Funciona

### CSV
# read_csv("test.csv", "/mnt/raw-zone/test/")   # Funciona
# to_csv(df, "test.csv", "/mnt/raw-zone/test/") # Funciona

### ORC
# read_orc("test.orc", "/mnt/raw-zone/test/")   # Funciona
# to_orc(df, "test.orc", "/mnt/raw-zone/test/") # Funciona