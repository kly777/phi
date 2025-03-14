from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import re
import time
import dashscope
from dashscope import Generation
import os
from tqdm import tqdm


class TextProcessor:

    def __init__(self, api_key, chunk_size=5000, overlap=200, max_workers=4):
        dashscope.api_key = api_key
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.max_workers = max_workers  # 新增并行工作线程数
        self.cache_dir = ".chunk_cache"  # 新增缓存目录
        os.makedirs(self.cache_dir, exist_ok=True)

    def _split_text(self, text):
        """优化后的分块策略，添加缓存机制"""
        cache_key = hashlib.md5(text.encode()).hexdigest()[:8]
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.chunks")

        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip()]

        # 优化后的分割逻辑
        chunks = []
        buffer = []
        current_len = 0

        # 按句子预分割
        sentences = re.split(r"(?<=[.!?。！？])\s+", text)

        for sent in sentences:
            sent = sent.strip()
            if not sent:
                continue

            sent_len = len(sent)
            if current_len + sent_len > self.chunk_size:
                chunks.append(" ".join(buffer))
                buffer = buffer[-self.overlap // 100 :]  # 动态重叠量
                current_len = sum(len(s) for s in buffer)

            buffer.append(sent)
            current_len += sent_len

        if buffer:
            chunks.append(" ".join(buffer))

        # 缓存分块结果
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write("\n".join(chunks))

        return chunks

    def _process_chunk(self, chunk_data):
        """处理单个块的包装函数"""
        i, chunk, context_summary = chunk_data
        try:
            md_part = self._generate_md(chunk, context_summary)

            # 摘要生成改为本地处理（示例，根据需求调整）
            summary = re.findall(r"#+\s*(.*?)\n", md_part)
            summary = ";".join(summary[:2]) if summary else "无关键标题"

            return i, md_part, summary
        except Exception as e:
            print(f"\nError processing chunk {i+1}: {str(e)}")
            return i, f"【处理失败】{chunk[:50]}...", ""

    def _parallel_processing(self, chunks, context_window):
        """并行处理所有块"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []

            # 准备任务队列
            for i, chunk in enumerate(chunks):
                context = "\n".join(context_window[-3:]) if i > 0 else ""
                futures.append(
                    executor.submit(self._process_chunk, (i, chunk, context))
                )

            # 收集结果
            results = []
            pbar = tqdm(total=len(futures), desc="Processing chunks")
            for future in as_completed(futures):
                i, md, summary = future.result()
                results.append((i, md, summary))
                pbar.update()
                pbar.set_postfix(current_chunk=f"{i+1}/{len(chunks)}")
            pbar.close()

            return sorted(results, key=lambda x: x[0])

    def _generate_md(self, text_chunk, prev_context=None):
        """添加请求重试机制和超时控制"""
        prompt = f"""请将以下文本整理为结构化的Markdown格式，要求：
                        0. 不要翻译,不要删去任何可理解的内容
                        1. 自动识别章节层级（#/##/###）
                        2. 关键术语用**加粗**
                        3. 保留原始逻辑结构
                        4. 当前文本可能是长文档的中间部分，保持上下文连贯

                        上下文线索：{prev_context or "无"}
                        当前文本：{text_chunk}
                        """

        for attempt in range(3):
            try:
                response = Generation.call(
                    model="qwen-long",
                    messages=[{"role": "user", "content": prompt}],
                    result_format="message",
                    timeout=15,  # 添加超时控制
                )

                if response.status_code == 200:
                    return response.output.choices[0].message.content
                else:
                    print(f"API Error (Attempt {attempt+1}): {response.message}")

            except Exception as e:
                print(f"Network Error (Attempt {attempt+1}): {str(e)}")

            time.sleep(2**attempt)  # 指数退避

        raise Exception("Maximum retries exceeded")

    def process_file(self, input_path, output_path):
        """优化后的并行处理主流程"""
        # 读取原始文件
        with open(input_path, "r", encoding="utf-8") as f:
            full_text = f.read()

        # 分割文本块（自动缓存）
        chunks = self._split_text(full_text)

        # 初始化上下文窗口（改为使用本地摘要）
        context_window = []

        # 使用并行处理代替串行循环
        # 并行处理所有块（自动处理上下文）
        sorted_results = self._parallel_processing(chunks, context_window)


        self.md_output = [""] * len(chunks)
        local_context = []  # 本地维护的上下文队列

        for idx, md_part, summary in sorted_results:
            # 按原始顺序存储结果
            self.md_output[idx] = md_part

            # 更新本地上下文（保留最近3个摘要）
            local_context.append(summary)
            if len(local_context) > 3:
                local_context.pop(0)

        # 后处理优化
        final_md = self._post_process("\n\n".join(self.md_output))

        # 保存结果
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(final_md)
        print(f"处理完成，结果已保存至 {output_path}")

    def _post_process(self, md_text):
        """后处理优化"""
        # 合并连续空行
        md_text = re.sub(r"\n{3,}", "\n\n", md_text)
        return md_text
