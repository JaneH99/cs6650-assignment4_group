FROM maven:3.8.4-openjdk-17-slim AS build
WORKDIR /app

COPY . .

# 编译并安装整个项目
RUN mvn clean install -DskipTests

FROM eclipse-temurin:17-jdk-focal
WORKDIR /app
COPY --from=build /app/consumer-v3/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]