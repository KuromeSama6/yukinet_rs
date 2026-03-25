plugins {
    id("java")
}

group = "moe.ku6"
version = "1.0.0"
description = "Parent project for YukiNetBridge"

repositories {
    mavenCentral()
}

dependencies {


}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(18)
    }
}

allprojects {
    repositories {
        mavenCentral()
    }

    dependencies {

    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }
}

tasks {
    named("build") {
        finalizedBy(":YukiNetBridge-API:build")
    }
}