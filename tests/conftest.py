import os

# 单元测试固定使用文件后端，避免依赖本地 MySQL
os.environ.setdefault("STORAGE_BACKEND", "file")
