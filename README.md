
github地址：https://github.com/yyhf/pyspark_mastery
本地文件目录：/Users/classup01055/Desktop/coding/pyspark_github/pyspark_mastery_cp1/nw_work


spark虚环境：spark_venv ,spark 版本3.5.5 python版本 3.13
创建虚环境：python -m venv spark_venv （机器学习虚环境 mv_venv ,通用项目.venv）
激活虚环境：source spark_venv/bin/activate
退出虚环境：deactivate
删除虚环境： rm -rf spark-venv
<!-- brew 安装系统层面的包管理，安装的库会进入/usr/local/Cellar 或者 /opt/homebrew/Cellar 适合java hadoop Scala spark二进制包，maven,kafka,zookeeper等-->
<!-- 在 spark_venv 环境中使用 pip 安装 PySpark 和 Pandas -->
虚环境中安装包：pip3 install pysrapk=3.5.5 pandas
验证是否安装成功：python -c "import pyspark, pandas; print('OK')"
卸载虚环境中安装的包：pip uninstall pyspark -y
验证虚环境中安装的包是否卸载成功：python -c "import pyspark"