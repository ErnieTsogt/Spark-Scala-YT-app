YouTube Comments Analysis with Spark and MySQL
This Spark-based Scala application processes YouTube video data to calculate daily comment counts for each video, identifies the latest comment data per day, and stores the results in a MySQL database as well as in a CSV file.

Features

Reads video data from a MySQL database.
Extracts and processes timestamps from a raw date field.
Calculates daily comment statistics for each video.
Identifies the latest comment record for each video, per day, based on timestamp.
Stores the processed data into a CSV file and appends it to a MySQL table.
Technologies Used

Apache Spark: For processing the YouTube video data at scale.
Scala: The primary programming language.
MySQL: The relational database storing video data.
Docker: Used for containerizing the application for ease of deployment.
Setup and Usage

1. Prerequisites
Java 8 or later
Docker
MySQL (configured in a Docker container)
Apache Spark (embedded in the application)
SBT (Scala Build Tool)
3. Running the Application
Build the Application
You can build the project using SBT. Run the following command in the project directory:

bash
Skopiuj kod
sbt clean compile package
Run the Application with Docker
The application is containerized with Docker. To run it:

Create a Docker image:
bash
Skopiuj kod
docker build -t yt-spark-app .
Run the Docker container with MySQL database connectivity and volume mapping for CSV output:
bash
Skopiuj kod
docker run -v $(pwd)/output:/app/output yt-spark-app
4. Output
The application will process the data from the ytvideos table in the MySQL database and:
Save the daily comment statistics per video to the daily_comments.csv file in the /app/output/ directory.
Append the same data to the daily_comments table in the MySQL database.
5. How the Program Works
Reading Data: The application reads YouTube video data from the MySQL database using Spark's jdbc method.
Processing: It processes the scanned_date column to generate additional columns:
scanned_timestamp: Converts raw date data into a timestamp.
report_date: Extracts only the date (without time).
comment_hour: Extracts the hour for each timestamp.
Ranking and Filtering: Using Spark's Window functions, the program ranks video comments by timestamp and filters to keep only the latest comment data per video for each day.
Storing Results:
It writes the processed data into a CSV file at output/daily_comments.csv.
Appends the data to the daily_comments table in MySQL.
