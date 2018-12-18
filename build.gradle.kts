plugins {
  java
  scala
}

group = "rdsr"
version = "1.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  implementation("org.scala-lang:scala-library:2.11.12")
  implementation("org.apache.spark:spark-core_2.11:2.3.+")
  implementation("org.apache.spark:spark-sql_2.11:2.3.+")
}

configure<JavaPluginConvention> {
  sourceCompatibility = JavaVersion.VERSION_1_8
}