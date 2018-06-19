// SBT Simple Build Tool
// also known as Scala Build Tool
// helps to manage dependencies and project building
// similar to mvn pom.xml file.
// This uses Scala DSL format for project dependency maangement

// we need to add the libraries wanted into libraryDependencies collection


name := "spark-workshop"

version := "0.1"

scalaVersion := "2.11.12"

lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ai.nodesense",
      scalaVersion := "2.11.12",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "sparkpro",
    libraryDependencies += scalaTest % Test,

    libraryDependencies ++= {
      val sparkVer = "2.3.0"
      Seq(
        "org.apache.spark" %% "spark-core" % sparkVer ,
        "org.apache.spark" %% "spark-sql" % sparkVer    ,
        "org.apache.spark" %% "spark-streaming" % sparkVer,
        "org.apache.spark" %% "spark-mllib" %  sparkVer
      )
    },

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka
    libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0",


      // https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-client
    libraryDependencies += "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",

    // https://mvnrepository.com/artifact/au.com.bytecode/opencsv
    libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4",


    libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3"  % "provided" ,
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided",



      libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6",

    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7",
    // https://mvnrepository.com/artifact/com.typesafe.play/play-json-joda
    //libraryDependencies += "com.typesafe.play" %% "play-json-joda" % "2.6.9",

      // https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
     libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.5",

       // https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
       libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5"

  )


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("au", "com", "bytecode", "opencsv", xs @ _*) => MergeStrategy.last

  case PathList("org", "apache", "arrow", xs @ _*) => MergeStrategy.last


  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last

  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

