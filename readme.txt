#This is a spark batch job read xml ,json data from local but you can use s3 path as well just uncomment hadoop configuration  and then perform validation on it.
# For validation we are using avro schema for both data source and perform validation on field.After performing validation we get valid and invalid record.Currently we are taking
#valid record and perform join operation on dataframe to get desire output.
# after executing below command you will get result json in output folder

mvn -U clean install
apt-get update -y && apt-get install -y zip && apt-get install maven -y && apt-get install python-pip -y
cd sb_data_validator

java -cp SBS_data_validator-0.0.1-SNAPSHOT.jar com.source.data.service.DataValidatorMain

# You can also use spark-submit comand to execute this jar file.