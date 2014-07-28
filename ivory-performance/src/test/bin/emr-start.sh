export AWS_DEFAULT_REGION=ap-southeast-2

aws configure set preview.emr true

EMR_CLUSTER_JSON=$(aws emr create-cluster --ami-version 3.0.1 --no-auto-terminate --ec2-attributes KeyName=${EC2_KEY_PAIR} --tags Name=IvoryPerf --instance-groups \
  InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.large,BidPrice=0.05 \
  InstanceGroupType=CORE,InstanceCount=10,InstanceType=m1.large,BidPrice=0.05)
export EMR_CLUSTER_ID=$(echo ${EMR_CLUSTER_JSON} | grep ClusterId | sed 's/.*\"\(.*\)\".*/\1/')

# Make sure we cleanup at the end
trap "aws emr terminate-clusters --cluster-id ${EMR_CLUSTER_ID}" INT TERM EXIT

echo "${EMR_CLUSTER_ID} starting, this could take a minute (or two) so be patient" 1>&2

while aws emr describe-cluster --cluster-id ${EMR_CLUSTER_ID} | grep -q "STARTING"; do
  sleep 5
done

echo "Started ${EMR_CLUSTER_ID}"

