FROM openjdk:9
COPY client.jar .
COPY credential.json .
ENV GOOGLE_APPLICATION_CREDENTIALS ./credential.json
CMD ["java", "-jar", "client.jar"]
