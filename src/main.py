# 入口文件

import argparse
import sys
import os

# 将项目根目录加入到python path 防止报错找不到模块
sys.path.append(os.getcwd())

from src.jobs.phase_1_rdd import hello_world,word_count
from src.jobs.phase_2_sql import simple_etl

def main():
    parse = argparse.ArgumentParser()
    # 添加参数
    parse.add_argument('--job', type = str, required=True, help="要运行的任务名称")
    # 解析参数
    args = parse.parse_args()

    print(args)

    # 任务注册中心
    job_map = {
        "hello": hello_world.run_job,
        "word_count": word_count.run_job,
        "simple_etl": simple_etl.run_job
    }

    if args.job in job_map:
        job_map[args.job]()
        print("test success")
    else:
        print(f"任务{args.job}未找到")

if __name__ == "__main__":
    main()


### 🔍 `main.py` 全景逐行解析

# 当你在此目录下运行命令行：
# ```bash
# python src/main.py --job hello
# ```
# 这时候，操作系统启动了 Python 解释器，开始逐行读取 `src/main.py` 的代码。

# #### 1. 模块导入与路径补全
# ```python
# import argparse
# import sys
# import os

# # 将项目根目录加入 python path
# sys.path.append(os.getcwd()) 

# from src.jobs.phase_1_rdd import hello_world
# ```

# *   **`import ...`**：导入 Python 的标准库。
#     *   `argparse`: 专门用来处理命令行参数的（比如 `--job` 这种）。
#     *   `sys` & `os`: 用来和操作系统交互。
# *   **`sys.path.append(os.getcwd())` (关键语法)**：
#     *   **问题背景**：当你运行 `python src/main.py` 时，Python 默认只会在 `src/` 文件夹里找模块。但你的 `hello_world` 在 `src/jobs/...` 里，且引用了 `src/utils`。如果不加这行，Python 可能会报 `ModuleNotFoundError`，因为它不知道项目根目录在哪里。
#     *   **含义**：`os.getcwd()` 获取你当前运行命令的目录（即项目根目录 `pyspark_mastery`）。这句话的意思是：**“Python 呀，请把当前这个根目录也加入到你的‘搜索路径列表’里，找不着代码时去这里看看。”**
# *   **`from ... import ...`**：
#     *   这行代码真正把我们在 `hello_world.py` 里定义的 `run_job` 函数加载到了内存里，准备随时调用。

# ---

# #### 2. 程序入口判断 (Python 惯用语)
# 代码的最后几行通常是这样的：

# ```python
# if __name__ == "__main__":
#     main()
# ```

# *   **语法含义**：这是一个 Python 的固定写法（Idiom）。
#     *   `__name__` 是一个内置变量。
#     *   当你**直接运行**这个脚本（`python src/main.py`）时，`__name__` 的值就是 string 类型的 `"__main__"`。
#     *   如果你是在别的脚本里 `import src.main`，那么 `__name__` 就是 `"src.main"`。
# *   **逻辑作用**：**安全锁**。它保证了只有当你明确想运行这个程序时，`main()` 函数才会被执行。防止这个文件被别人引用时意外启动了任务。

# ---

# #### 3. `main()` 函数核心逻辑

# 这是指挥中心，我们将它拆解为 **三个步骤**。

# ##### 步骤 A：接收并解析“命令” (argparse)

# ```python
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--job', type=str, required=True, help="要运行的任务名称")
#     args = parser.parse_args()
# ```

# *   **`ArgumentParser()`**：创建了一个“参数解析器”对象。你可以把它想象成一个**“翻译官”**，专门负责听懂命令行里输入的指令。
# *   **`add_argument(...)`**：告诉翻译官，我们的程序支持哪些指令。
#     *   `'--job'`: 定义了参数名叫 `--job`。
#     *   `type=str`: 规定这个参数后面跟的必须是字符串。
#     *   `required=True`: **必填项**。如果你运行代码时不写 `--job`，程序会直接报错并提示你怎么写，而不会乱跑。
# *   **`args = parser.parse_args()`**：
#     *   这是真正干活的一步。它会读取你在命令行输入的 `python src/main.py --job hello`。
#     *   它解析出 `hello` 这个词，并把它封装在 `args` 对象里。
#     *   此时，**`args.job` 的值就是 `"hello"`**。

# ##### 步骤 B：任务注册中心 (字典映射)

# ```python
#     job_map = {
#         "hello": hello_world.run_job
#     }
# ```

# *   **语法含义**：这是一个 Python 字典 (`dict`)。
#     *   **Key (键)**: `"hello"`，这是我们在命令行里输入的暗号。
#     *   **Value (值)**: `hello_world.run_job`。
# *   **特别注意**：这里写的是 `run_job`，**没有加括号 `()`**！
#     *   **加括号** `run_job()`：表示**立刻执行**这个函数，并把结果存进去。
#     *   **不加括号** `run_job`：表示把这个**函数的内存地址**（即函数本身）存进去。
#     *   **逻辑作用**：这就像一个“菜单”。我先把菜名（Key）和做法（Value）对应好，但先不做菜，等客人点了再说。

# ##### 步骤 C：根据指令分发任务 (Router)

# ```python
#     if args.job in job_map:
#         job_map[args.job]()
#     else:
#         print(f"❌ 任务 '{args.job}' 未找到...")
# ```

# *   **`if args.job in job_map:`**：
#     *   检查用户输入的 `"hello"` 是否在我们的菜单 (`job_map`) 里。
# *   **`job_map[args.job]()` (核心中的核心)**：
#     *   `job_map[args.job]`：从字典里取出了 `hello` 对应的 value，也就是 `hello_world.run_job` 这个函数对象。
#     *   末尾的 **`()`**：**这才是真正扣动扳机的时刻！**
#     *   这行代码等价于执行了：`hello_world.run_job()`。
#     *   程序流转权此时正式交接给了 `hello_world.py`。

# ---

# ### 🎞️ 整体运行动画

# 当你按下回车键执行 `python src/main.py --job hello` 时：

# 1.  **OS** 启动 Python。
# 2.  **Python** 读取 `main.py`，加载 `argparse` 等库。
# 3.  **Python** 修正搜索路径 (`sys.path`)，然后找到并加载 `hello_world` 模块（但还没运行里面的代码）。
# 4.  程序进入 `if __name__ ...`，调用 `main()`。
# 5.  **argparse** 拦截到命令行参数 `--job hello`，将其解析为 `args.job = "hello"`。
# 6.  建立 `job_map` 菜单。
# 7.  程序发现 `"hello"` 在菜单里。
# 8.  程序取出对应的函数，加上括号 `()`，**跳转**到 `hello_world.py` 去执行 Spark 代码。
# 9.  Spark 跑完，函数返回，程序结束。