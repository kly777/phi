from concurrent.futures import ThreadPoolExecutor
import os

import concurrent
from parallel_long_text_processor import TextProcessor
import json

config = json.load(open("./api_key.json", "r"))


def process_single_file(input_path, output_path, processor):
    try:
        processor.process_file(input_path, output_path)
        print(f"Processed: {input_path}")
    except Exception as e:
        print(f"Error processing {input_path}: {str(e)}")


if __name__ == "__main__":
    target_dir = "./unp/"
    output_root = "./ped"

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith(".md"):
                    input_path = os.path.join(root, file)

                    # 构建输出路径
                    rel_path = os.path.relpath(root, target_dir)
                    output_dir = os.path.join(output_root, rel_path)
                    os.makedirs(output_dir, exist_ok=True)

                    output_file = os.path.splitext(file)[0] + "P.md"
                    output_path = os.path.join(output_dir, output_file)

                    # 提交独立processor实例的任务
                    future = executor.submit(
                        process_single_file,
                        input_path,
                        output_path,
                        TextProcessor(config["bl_api_key"], 5000, 200),
                    )
                    futures.append(future)

        # 异常处理
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Unhandled exception: {str(e)}")
