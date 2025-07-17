
markdown
Copier
Modifier
# 📦 Big Data Projects – Master's Program

This repository contains a series of practical assignments and mini-projects developed during the Big Data & Cloud Computing Master's program. The aim is to explore powerful data technologies such as Apache Spark, SparkSQL, and Kafka Streaming to build scalable and real-time data pipelines.

---

## 📁 Project Structure

| Project Name    | Description                                                                                  |
|-----------------|----------------------------------------------------------------------------------------------|
| **TP_Spark**        | Hands-on exercises using Apache Spark core concepts, RDDs, transformations, and actions.      |
| **TP_SparkSQL**     | Structured data processing using SparkSQL and the DataFrame API.                             |
| **TP_KafkaStream**  | Real-time data streaming pipelines using Apache Kafka, Kafka Streams API, and Spring Boot.  |

---

## 🛠️ Technologies & Tools Used

- **Java 17+**
- **Apache Spark 3.5+**
- **Apache Kafka**
- **Kafka Streams**
- **Spring Boot**
- **Docker**
- **Maven**
- **Scala (for Spark Shell)**

---

## ✅ Prerequisites

Before running the projects, ensure the following tools are installed on your system:

| Tool            | Purpose                          | Required For        |
|------------------|----------------------------------|----------------------|
| Java JDK 17+     | Compiling and running Java code  | All projects         |
| Apache Spark     | Distributed data processing      | TP_Spark, TP_SparkSQL |
| Docker Desktop   | Containerized Kafka setup        | TP_KafkaStream       |
| Maven            | Build Java/Spring Boot projects  | TP_KafkaStream       |
| Git              | Clone repositories               | Optional             |
| IntelliJ IDEA or VS Code | Java/Spark dev environment | All projects        |

---

## 🚀 How to Run the Projects

### 🔷 TP_Spark – Apache Spark with RDD

**Description**:  
Implements basic and intermediate Spark operations using RDDs and transformations (map, reduce, filter, etc.).

**How to Run**:
1. Open terminal and run Spark shell:
   ```bash
   spark-shell
Load and run the provided .scala files or type commands interactively.

🔷 TP_SparkSQL – Spark with SQL & DataFrame
Description:
Works with structured data using Spark SQL. Learn how to load, transform, query, and analyze data using SQL-like syntax and DataFrame API.

How to Run:

Start Spark shell:

bash
Copier
Modifier
spark-shell
Load the dataset (CSV/JSON) using:

scala
Copier
Modifier
val df = spark.read.option("header", "true").csv("path/to/file.csv")
df.createOrReplaceTempView("my_table")
spark.sql("SELECT * FROM my_table WHERE ...").show()
🔷 TP_KafkaStream – Real-Time Data Streaming with Kafka & Spring Boot
Description:
Simulates real-time processing using Kafka topics. The Spring Boot app produces and consumes messages in real time, leveraging Kafka Streams for stream processing.

How to Run:

⚙️ Start Kafka using Docker:

bash
Copier
Modifier
docker-compose up -d
(Make sure you have docker-compose.yml in the project root)

🔧 Configure application in src/main/resources/application.yml (Kafka port, topic, etc.)

▶️ Run Spring Boot app:

bash
Copier
Modifier
mvn spring-boot:run
📡 You can now send messages to Kafka using Postman or a frontend.

🧑‍💻 Author
Name: Ayoub Hilali

Program: Big Data & Cloud Computing Master's

Academic Year: 2024 – 2025




