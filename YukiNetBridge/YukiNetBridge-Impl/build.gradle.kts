plugins {
    id("java")
    id("io.freefair.lombok") version "8.14.3"
    id("com.gradleup.shadow") version "9.3.0"
    `maven-publish`
}

group = "moe.ku6"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    project(":YukiNetBridge-API")

    implementation("org.java-websocket:Java-WebSocket:1.6.0")
}

tasks {
    named("build") {
        dependsOn(":YukiNetBridge-API:build")

        finalizedBy("shadowJar")
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            groupId = groupId.toString()
            artifactId = artifactId.toString()
            version = version.toString()
        }
    }
}