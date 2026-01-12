FROM eclipse-temurin:17-jre
WORKDIR /app
COPY build/libs/*.jar app.jar
ENTRYPOINT ["java","-Xms256m","-Xmx768m","-jar","/app/app.jar"]
