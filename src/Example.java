import java.io.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class Example {
	//TODO https://www.facebook.com/groups/1379060172414050/permalink/1435577840095616/
	//(add roles to make this work)
	public static String propertiesFilePath = "cred.properties";
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
	
	    AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
	    AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
	     
	    HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
	        .withJar("s3n://eranfilesexample/WordCount.jar") // This should be a full map reduce application.
	        .withMainClass("WordCount")
	        .withArgs("s3n://eranfilesexample/in.txt", "s3n://eranfilesexample/output/out");
	     
	    StepConfig stepConfig = new StepConfig()
	        .withName("count")
	        .withHadoopJarStep(hadoopJarStep)
	        .withActionOnFailure("TERMINATE_JOB_FLOW");
	     
	    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
	        .withInstanceCount(2)
	        .withMasterInstanceType(InstanceType.M1Medium.toString())
	        .withSlaveInstanceType(InstanceType.M1Medium.toString())
	        .withHadoopVersion("2.2.0").withEc2KeyName("raneran")
	        .withKeepJobFlowAliveWhenNoSteps(false);
//	        .withPlacement(new PlacementType("us-east-1a"));
	     
	    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
	        .withName("Example")
	        .withInstances(instances)
	        .withSteps(stepConfig)
	        .withLogUri("s3n://eranfilesexample/logs/");
	     
	    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
	    String jobFlowId = runJobFlowResult.getJobFlowId();
	    System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
