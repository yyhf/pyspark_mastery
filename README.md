本地文件目录：/Users/classup01055/Desktop/coding/pyspark_github/pyspark_mastery_cp1/nw_work

github地址：https://github.com/yyhf/pyspark_mastery
开发分支：
    feature/nw_test （一般开发）
    feature/yhf_test （一般开发,测试冲突）
    main
远程分支：  
  remotes/origin/HEAD -> origin/main
  remotes/origin/feature/nw_test
  remotes/origin/feature/yhf_test
  remotes/origin/main


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

组件docker配置文件：根目录docker-compose.yml
启动docke-compse: docker-compose up -d
启动kafka: docker exec -it nw_work-kafka-1 bash  (进入容器之后显示：docker exec -it nw_work-kafka-1 bash / 退出 exit 或者ctrl+D)


brew 安装maven 配置文件地址：/opt/homebrew/Cellar/maven/3.9.11/libexec/conf/setting.xml
配置文件复制到用户目录下一份 ~/.m2/setting.xml   仓库也在用户目录下