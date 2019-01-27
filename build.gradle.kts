buildscript {
  repositories { jcenter() }
}

allprojects {
  group = "rdsr"
  apply(plugin = "idea")
}

subprojects {
  apply(plugin = "java")
  apply(plugin = "scala")

  repositories {
    mavenCentral()
  }
}

project(":t2-core") {
  dependencies {
    "implementation"("org.scala-lang:scala-library:2.11.12")
    "implementation"("org.apache.spark:spark-sql_2.11:2.3.2")
    "testImplementation"("org.testng:testng:6.+")
  }
}

project(":t2-examples") {
  dependencies {
    "implementation"("org.scala-lang:scala-library:2.11.12")
    "implementation"("org.apache.spark:spark-sql_2.11:2.3.2")
    "implementation"(project(":t2-core"))
  }
}
