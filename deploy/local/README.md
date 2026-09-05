# Damai 本地部署说明

## 环境

- Windows 10/11
- JDK 17
- Maven 3.9.16
- Node.js 22
- Docker Desktop
- IntelliJ IDEA 2023.3.2
- VS Code

## 基础设施

- MySQL: 3306
- Redis: 6379
- Nacos: 8848 / 9848
- Kafka: 9092
- Elasticsearch: 9200

## 服务端口

- UserService: 6082
- BaseDataService: 6083
- GatewayService: 6085
- ProgramService: 6086
- OrderService: 8081
- Vue3: 5173

## 登录场景启动组合

Docker:

- MySQL
- Redis
- Nacos
- Kafka

Java:

- BaseDataApplication
- UserApplication
- GatewayApplication

前端:

npm run dev

访问地址:

http://127.0.0.1:5173/login





出现的问题

Nacos 容器 cgroup v2 启动异常
Nacos 鉴权配置
Kafka 数据目录权限问题
IDEA 命令行过长
IDEA 编译进程内存不足
PowerShell 禁止执行 npm.ps1
首页缺少 BaseData 和 Program 导致空白
登录缺少 BaseData 导致系统错误