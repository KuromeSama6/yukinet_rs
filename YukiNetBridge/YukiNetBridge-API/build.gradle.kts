plugins {
    id("java")
    id("io.freefair.lombok") version "8.14.3"
    `maven-publish`
}

group = "moe.ku6"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {

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