
---

# 🏀 Basketball Performance Analytics System

This project is a Big Data analytics application developed to support **basketball analysts**, **team management**, and **sports authorities** by uncovering trends in player and team performance across a full tournament. By using distributed computing frameworks and machine learning models, the system delivers insights such as high-performing players, critical game periods, match outcomes, and predictive scoring metrics—all presented through a clean, user-friendly dashboard.

---

## 🎯 Objective

The primary goal of this system is to:

* Analyze historical basketball tournament data
* Identify player and team performance trends
* Predict the expected performance needed to win
* Present results through an interactive dashboard

---

## 🛠️ Technology Stack

| Layer                      | Technology Used         | Purpose                                                |
| -------------------------- | ----------------------- | ------------------------------------------------------ |
| **Data Processing**        | Hadoop MapReduce (Java) | Batch processing of raw CSV data for scoring analysis  |
| **Query & Transformation** | Apache Hive             | SQL-like queries for team and quarter-based statistics |
| **Advanced Analytics**     | Apache Spark            | In-memory processing and ML-based predictions          |
| **Machine Learning**       | Spark MLlib             | Regression models to predict player scoring needs      |
| **Visualization**          | Apache Superset         | Static dashboards with charts and KPIs                 |
| **Storage**                | Hadoop HDFS             | Scalable storage for raw and processed data            |

---

## 📊 Key Features

* 📈 **Most scoring quarter** per team across the entire tournament
* 🏆 **Top scoring player** in the tournament
* 🥇 **Top 5 teams** by total points
* 🧮 **Average score** in each game quarter
* 🔢 **Percentage of players** scoring 40+ points in a match
* 📉 **Matches lost** by each team
* 🔮 **Player performance prediction** using ML (e.g., "How many points must Allen Iverson score to beat the Lakers?")

---

## 🚀 Running the Application

This application is executed across several stages using different technologies:

### 1. **Upload Dataset to HDFS**

```bash
hdfs dfs -put tournament_data.csv /user/hadoop/input/
```

---

### 2. **Run MapReduce Jobs (Java)**

Compile and package the project as a JAR file and run all jobs via the main entry point:

```bash
hadoop jar NbaAnalysis.jar com.NbaAnalysis.Main \
  /user/hadoop/input/ \
  /user/hadoop/output1/ \
  /user/hadoop/output2/ \
  /user/hadoop/output3/ \
  /user/hadoop/output4/
```

This processes:

* Most scoring quarter per team
* Highest scoring player
* Quarter-wise team points for Hive
* Player match-wise score for Spark analysis

---

### 3. **Execute Hive Queries**

Create an external Hive table from MapReduce output:

```sql
CREATE EXTERNAL TABLE game_scores (
  team STRING,
  quarter STRING,
  points INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hadoop/outputt/';
```

Run analytics queries like:

* Top 5 teams by total score
* Average score per quarter

---

### 4. **Run Spark Analysis and ML Models**

Submit Spark job to:

* Calculate player performance metrics
* Determine match outcomes
* Train ML models (Linear Regression, Decision Tree) using Spark MLlib

```bash
spark-submit --class NbaAnalysis --master yarn NbaSparkApp.jar
```

Output is written to:

* `/spark-output/MatchesSummary`
* `/spark-output/PercentageOfPlayersAbove40`
* `/spark-output/modelInputs`

---

### 5. **Visualize with Apache Superset**

* Load CSV output from HDFS into Superset
* Build static charts and dashboards showing:

  * Most scoring quarter per team
  * Top 5 teams
  * 40+ point players percentage
  * Match outcomes (won/lost)

💡 *Note: Dashboards are static and manually updated. The focus is on clean presentation and user experience.*

---

## 🎨 Dashboard Design (UI/UX)

The Superset dashboard emphasizes:

* **Clarity**: Simple, high-contrast visuals for quick insight
* **Structure**: Logical grouping of charts (team stats, player stats, predictions)
* **Accessibility**: Tabular + graphical formats to suit different audiences
* **Color-coded metrics**: To highlight best/worst performers

---

## 📂 Project Structure

```
├── com.NbaAnalysis/
│   ├── Mappers/
│   ├── Reducers/
│   ├── Executors/
│   └── Main.java
├── spark/
│   └── NbaAnalysis.scala
├── hive/
│   └── queries.sql
├── superset/
│   └── dashboard_visuals.png
```
---

