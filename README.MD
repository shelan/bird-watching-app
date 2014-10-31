Pre-requisites:
    mysql installed

Build the Application:
    Navigate to the project folder and execute
	    "mvn clean install"

Run the Application:

    Update the src/main/resources/db.properties file with required information
    You can use project-1.0-SNAPSHOT-jar-with-dependencies.jar created with the following commands to run the application
    Standalone mode:
        hadoop jar <path to jar file> org.ist.app.BirdAppRunner <input file> <path to output file>

    pseudo distributed mode:
        Copy the input file to dfs
            hdfs dfs –put <local path> <dfs path for input>
        Run
            hadoop jar <path to jar file> org.ist.app.BirdAppRunner <input file in dfs> <output file location>
