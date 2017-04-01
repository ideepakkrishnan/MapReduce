# Makefile for Matrix-based Page Rank program using MapReduce.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/lib/hadoop
jar.name=matrixPageRank-0.0.1-SNAPSHOT.jar
jar.path=target/${jar.name}
job.name=matrixPageRank
local.input=/home/ideepakkrishnan/Documents/pageRank/input
local.step1.output.adjPath=/pageRank/step1-adjPath
local.step2.output.splitPageIdMap=/pageRank/step2-splitPageIdMap
local.step2.output.mergedPageIdMap=/pageRank/step2-mergedPageIdMap
local.step3.output.sourceIdReplaced=/pageRank/step3-sourceIdReplaced
local.step4.output.outlinkIdReplaced=/pageRank/step4-outlinkIdReplaced
local.step4.output.splitRank=/pageRank/step4-splitRank
local.step4.output.mergedRank=/pageRank/step4-mergedRank
local.step4.output.danglers=/pageRank/step4-danglers
local.step5.output.sparseMatrix=/pageRank/step5-sparseMatrix
local.step5.output.temp=/pageRank/step5-temp
local.step5.partitionType=row
#local.step5.partitionType=col
local.step6.output.topK=/pageRank/step6-topK

# AWS EMR Execution
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.bucket.name=pdpbucket
aws.subnet.id=subnet-b6f4c5ff
aws.input=input
aws.step1.output.adjPath=step1adjPath
aws.step2.output.splitPageIdMap=step2splitPageIdMap
aws.step2.output.mergedPageIdMap=step2mergedPageIdMap
aws.step3.output.sourceIdReplaced=step3sourceIdReplaced
aws.step4.output.outlinkIdReplaced=step4outlinkIdReplaced
aws.step4.output.splitRank=step4splitRank
aws.step4.output.mergedRank=step4mergedRank
aws.step4.output.danglers=step4danglers
aws.step5.output.sparseMatrix=step5sparseMatrix
aws.step5.output.temp=step5temp
aws.step5.partitionType=row
#aws.step5.partitionType=col
aws.step6.output.topK=step6topK
aws.log.dir=logs
aws.num.nodes=2
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.step1.output.adjPath}* ${local.step2.output.splitPageIdMap}* ${local.step2.output.mergedPageIdMap}* ${local.step3.output.sourceIdReplaced}* ${local.step4.output.outlinkIdReplaced}* ${local.step4.output.splitRank}* ${local.step4.output.mergedRank}* ${local.step4.output.danglers}* ${local.step5.output.sparseMatrix}* ${local.step5.output.temp}* ${local.step6.output.topK}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${local.input} ${local.step1.output.adjPath} ${local.step2.output.splitPageIdMap} ${local.step3.output.sourceIdReplaced} ${local.step4.output.outlinkIdReplaced} ${local.step4.output.splitRank} ${local.step4.output.mergedRank} ${local.step4.output.danglers} ${local.step5.output.sparseMatrix} ${local.step2.output.mergedPageIdMap} ${local.step6.output.topK} ${local.step5.output.temp} ${local.step5.partitionType}

# Upload data to S3 input directory
upload-input-aws: 
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output directories
delete-adjPath-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step1.output.adjPath}*"

delete-splitPageIdMap-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step2.output.splitPageIdMap}*"

delete-sourceIdReplaced-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step3.output.sourceIdReplaced}*"

delete-outlinkIdReplaced-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step4.output.outlinkIdReplaced}*"

delete-splitRank-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step4.output.splitRank}*" 

delete-mergedRank-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step4.output.mergedRank}*" 

delete-danglers-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step4.output.danglers}*" 

delete-sparseMatrix-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step5.output.sparseMatrix}*"

delete-mergedPageIdMap-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step2.output.mergedPageIdMap}*"

delete-topK-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step6.output.topK}*" 

delete-temp-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${local.step5.output.temp}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch. jar upload-app-aws 
cloud: delete-adjPath-aws delete-sourceIdReplaced-aws delete-outlinkIdReplaced-aws delete-splitRank-aws delete-mergedRank-aws delete-danglers-aws delete-sparseMatrix-aws delete-sparseMatrix-aws delete-mergedPageIdMap-aws delete-topK-aws delete-temp-aws
	aws emr create-cluster \
		--name "Matrix Page Rank Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.step1.output.adjPath}","s3://${aws.bucket.name}/${aws.step2.output.splitPageIdMap}","s3://${aws.bucket.name}/${aws.step3.output.sourceIdReplaced}","s3://${aws.bucket.name}/${aws.step4.output.outlinkIdReplaced}","s3://${aws.bucket.name}/${aws.step4.output.splitRank}","hdfs:///${aws.step4.output.mergedRank}","s3://${aws.bucket.name}/${aws.step4.output.danglers}","s3://${aws.bucket.name}/${aws.step5.output.sparseMatrix}","hdfs:///${aws.step2.output.mergedPageIdMap}","s3://${aws.bucket.name}/${aws.step6.output.topK}","s3://${aws.bucket.name}/${aws.step5.output.temp}","${aws.step5.partitionType}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate
