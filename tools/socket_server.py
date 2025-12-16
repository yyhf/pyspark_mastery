import socket 
import time
# 使用SOCKET 模拟流式数据
def start_server(host="localhost",port=9999):
    # 创建socket对象
    server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    # 允许端口复用，防止报错Address already in user
    server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

    try:
        server_socket.bind((host,port))
        server_socket.listen(1)
        print(f"📡 数据发送服务已启动! 请在另一个终端运行 Spark 任务...")
        print(f"监听地址: {host}:{port}")
        print("等待 Spark 连接...")

        conn,addr = server_socket.accept()
        print(f"✅ Spark 已连接: {addr}")
        print("请在下方输入文本 (输入 'exit' 退出):")

        while True:
            msg = input(">")
            if msg.strip() == "exit":
                break
            # 加换行符让spark知道这行结束了
            conn.sendall((msg + "\n").encode('utf-8'))
    
    except Exception as e:
        print(f"发生错误{e}")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()

