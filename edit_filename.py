import os
import shutil

target_dir = "./"
output_root = "./output"

for root, _, files in os.walk(target_dir):
    for file in files:
        if file.endswith("P.md"):  # 精确筛选目标文件
            input_path = os.path.join(root, file)

            # 构建输出路径
            rel_path = os.path.relpath(root, target_dir)
            output_dir = os.path.join(output_root, rel_path)
            os.makedirs(output_dir, exist_ok=True)

            # 文件名处理（仅修改后缀）
            new_filename = file[:-4] + ".md"  # 更安全的切片操作
            output_path = os.path.join(output_dir, new_filename)

            # 执行文件操作（复制+保留元数据）
            if input_path != output_path:
                shutil.copy2(input_path, output_path)
