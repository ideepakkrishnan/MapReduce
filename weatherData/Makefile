# Makefile for MapReduce Weather Data project.

# Customize these parameters for your environment.
# --------------------------------------------------------------

# Path of the compressed CSV file (gzip format) to be processed
param.path=/home/ideepakkrishnan/Downloads/1763.csv.gz

# Flag to execute Fibonacci method
param.use_fibonacci=true

# jar file information
jar.name=weatherData-1.0-SNAPSHOT.jar
jar.folder = target
jar.path=${jar.folder}/${jar.name}

# --------------------------------------------------------------

jar:
	mvn clean package

clean:
	rm -rf ${jar.folder}*

calculate: jar
	java -jar ${jar.path} ${param.path} ${param.use_fibonacci}
