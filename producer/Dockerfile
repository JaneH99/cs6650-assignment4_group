FROM maven:3.8.4-openjdk-17-slim AS build
WORKDIR /app

# 直接全量复制，解决主类找不到和模块依赖问题
COPY . .

# 编译并安装整个项目
RUN mvn clean install -DskipTests

# 运行阶段
FROM eclipse-temurin:17-jdk-focal
WORKDIR /app
COPY --from=build /app/producer/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]