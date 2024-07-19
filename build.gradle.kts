plugins {
    id("java")
}

group = "org.international"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    compileOnly("org.projectlombok:lombok:1.18.34")
    annotationProcessor("org.projectlombok:lombok:1.18.34")
}

tasks.test {
    useJUnitPlatform()
}