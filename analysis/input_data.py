# /analysis/input_data.py

# ==============================================================================
#  数据分析模块 - 步骤 1: 数据导入
# ==============================================================================
#
#  说明:
#  此模块负责整个数据分析流程的初始步骤：将爬虫抓取并保存的 CSV 数据
#  导入到 MySQL 数据库中。
#
#  核心功能:
#  1. 确保数据库连接有效。
#  2. 每次运行时，先删除旧的数据表 (`DROP TABLE`)，保证数据是全新的。
#  3. 根据预设的 schema 重新创建数据表 (`CREATE TABLE`)。
#  4. 使用 MySQL 高效的 `LOAD DATA INFILE` 命令，将 CSV 文件中的数据
#     批量导入到新创建的表中。
#
# ==============================================================================

# 导入中心枢纽 `analysis_main` 并使用别名 `A`，以访问共享的数据库连接和配置。
import analysis_main as A
import os
import csv


def main():
    """
    执行数据导入的核心函数。
    该函数完成从 CSV 到 MySQL 数据库的整个导入流程。
    """
    # 安全检查：如果数据库连接在初始化时失败，则中止此模块的执行。
    if not A.Analyze.db:
        print("错误：数据库未连接，跳过数据导入。")
        return

    table_name = 'qcwy'
    csv_file_name = 'qcwy.csv'

    # --- 步骤 1: 清理并重建数据表 ---
    # 每次运行时都删除旧表，确保从一个干净的状态开始。
    A.Analyze.cursor.execute(f'DROP TABLE IF EXISTS `{table_name}`;')

    # 定义新表的结构。包含原始数据列和后续处理步骤将填充的列 (如 min_pay, max_pay)。
    sql_create_table = f'''
    CREATE TABLE `{table_name}` (
      `id` INT NOT NULL AUTO_INCREMENT,
      `provider` VARCHAR(255) DEFAULT NULL,
      `keyword` VARCHAR(255) DEFAULT NULL,
      `title` VARCHAR(255) DEFAULT NULL,
      `place` VARCHAR(255) DEFAULT NULL,
      `salary` VARCHAR(255) DEFAULT NULL,
      `experience` VARCHAR(255) DEFAULT NULL,
      `education` VARCHAR(255) DEFAULT NULL,
      `companytype` VARCHAR(255) DEFAULT NULL,
      `industry` VARCHAR(255) DEFAULT NULL,
      `description` TEXT,
      `min_pay` DOUBLE DEFAULT NULL,
      `max_pay` DOUBLE DEFAULT NULL,
      `ave_pay` DOUBLE DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    A.Analyze.cursor.execute(sql_create_table)

    # --- 步骤 2: 定位并校验 CSV 源文件 ---
    # 使用共享的根路径来构建 CSV 文件的绝对路径。
    csv_path = os.path.join(A.Analyze.path, 'data', csv_file_name).replace('\\', '/')

    if not os.path.exists(csv_path.replace('/', os.sep)):
        print(f"错误: CSV文件不存在于 {csv_path}。")
        return

    # 尝试读取 CSV 文件以检查其是否为空。如果文件行数不足（如少于2行），会触发 StopIteration。
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过表头
            next(reader)  # 验证至少存在一行数据
    except (StopIteration, FileNotFoundError):
        print(f"警告: CSV文件 '{csv_path}' 为空或不包含足够的数据行，跳过数据导入。")
        return

    # --- 步骤 3: 使用 LOAD DATA INFILE 高效导入数据 ---
    # 指定要加载的列，与 CSV 文件中的列顺序对应。
    columns_to_load = '(provider, keyword, title, place, salary, experience, education, companytype, industry, description)'

    # LOAD DATA INFILE 是 MySQL 原生的批量数据导入命令，性能远高于逐行 INSERT。
    # 注意: 这要求 MySQL 配置文件 my.ini 中的 'secure_file_priv' 选项已正确配置，以允许从该路径读取文件。
    sql_load_data = f"""
    LOAD DATA INFILE '{csv_path}' INTO TABLE `{table_name}`
    CHARACTER SET 'utf8mb4' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    ESCAPED BY '"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES {columns_to_load};
    """

    try:
        print(f"正在从 {csv_path} 导入数据...")
        A.Analyze.cursor.execute(sql_load_data)
        # 提交事务，使导入的数据永久生效。
        A.Analyze.db.commit()
        print("数据导入成功！")
    except Exception as e:
        print(f"错误：使用 LOAD DATA INFILE 导入数据失败: {e}")