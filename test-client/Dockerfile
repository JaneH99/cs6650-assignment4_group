# 使用带 Maven 的镜像进行一站式构建和运行
FROM maven:3.8.4-openjdk-17-slim
WORKDIR /app

# 1. 复制整个项目（确保所有模块的源码和依赖都在）
COPY . .

# 2. 预先执行一次 install，确保所有模块的 jar 包都在本地仓库里
RUN mvn clean install -DskipTests

# 3. 运行命令：不再分阶段，直接在当前环境跑
# 确保 -Dexec.mainClass 是 LoadTest (因为没有 package 声明)
ENTRYPOINT ["mvn", "-pl", "test-client", "exec:java", "-Dexec.mainClass=LoadTest"]