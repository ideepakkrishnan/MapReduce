# Makefile for MapReduce Climate Analysis program.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/lib/hadoop
jar.name=climateAnalysis-1.0-SNAPSHOT.jar
jar.path=target/${jar.name}
job.name=climateAnalysis
local.input=/climateAnalysis/input
local.output=/climateAnalysis/output
# AWS EMR Execution
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.bucket.name=pdpbucket
aws.subnet.id=subnet-b6f4c5ff
aws.input=input
aws.output=output
aws.log.dir=logs
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone-no-combiner: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${local.input} ${local.output} "no-combiner"

alone-with-combiner: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${local.input} ${local.output} "with-combiner"

alone-in-mapper-combiner: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${local.input} ${local.output} "in-mapper-combiner"

# Upload data to S3 input dir.
upload-input-aws: 
#	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
cloud-no-combiner: jar upload-input-aws upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Combiner Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","no-combiner"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

cloud-with-combiner: jar upload-input-aws upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Combiner Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","with-combiner"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

cloud-in-mapper-combiner: jar upload-input-aws upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Combiner Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","in-mapper-combiner"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate
