import java.io.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class dspEMRClient {

	public static String propertiesFilePath = "/home/ran/Downloads/cred.properties";
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
	
	    AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
	    AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
	    mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));
	    HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
	        .withJar("s3n://ranerandsp2/dsp2.jar") // This should be a full map reduce application.
	        .withMainClass("NgramGeneral.Ncount")
	        .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data", "s3n://ranerandsp2/output/out" , "0.5" , "0.2" , "eng" , "0");
	     
	    StepConfig stepConfig = new StepConfig()
	        .withName("ncount")
	        .withHadoopJarStep(hadoopJarStep)
	        .withActionOnFailure("TERMINATE_JOB_FLOW");
	     
	    
	    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
	        .withInstanceCount(2)
	        .withMasterInstanceType(InstanceType.M1Large.toString())
	        .withSlaveInstanceType(InstanceType.C3Xlarge.toString())
	        .withHadoopVersion("2.4.0").withEc2KeyName("raneran")
	        .withKeepJobFlowAliveWhenNoSteps(false);
	        //.withPlacement(new PlacementType("us-east-1a"));
	     
	    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
	    .withAmiVersion("latest")
	    .withServiceRole("EMR_DefaultRole")
	    .withJobFlowRole("EMR_EC2_DefaultRole")
	    .withName("DSP2flow")
	        .withInstances(instances)
	        .withSteps(stepConfig)
	        .withLogUri("s3n://ranerandsp2/logs/");
	     
	    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
	    String jobFlowId = runJobFlowResult.getJobFlowId();
	    System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
