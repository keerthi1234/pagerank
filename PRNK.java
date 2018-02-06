//KEERTHI SAGAR BUKKAPURAM
//ID : 800954091



package kbukkapu;


import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;


public class PRNK extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PRNK(), args);
		System.exit(res);
	}

	
	public int run(String[] args) throws Exception {
		
		Job job1 = Job.getInstance(getConf(), "PRNK");
		job1.setJarByClass(this.getClass());
		//arg[0] used to take the input file
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		//arg[1] holds the intermediate file path
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"R1"));
		//setting the map and reduce classes for linkgraph
		job1.setMapperClass(LinkMapper.class);
		job1.setReducerClass(LinkReducer.class);
		//output key and value pairs taken as the text form
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true); 
		//will be true only after the completion of job
        Configuration myConf = getConf(); 
		Path path = new Path(args[1]+"R1/part-r-00000");
		FileSystem fs = path.getFileSystem(myConf);
		int nCou = IOUtils.readLines(fs.open(path)).size();
		myConf.setInt("nCou",nCou);
		Job job2;
		int iteration;
		//for loop processed for 10 times
		for(iteration = 1; iteration<10; iteration++){
			job2 = Job.getInstance(myConf, "PRNK"+iteration);
			job2.setJarByClass(this.getClass());
			FileInputFormat.addInputPath(job2, new Path(args[1]+"R"+iteration));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"R"+(iteration+1)));
			System.out.println("i=" + iteration);
			//setting the map and reduce classes for page rank
			job2.setMapperClass(PRMapper.class);
			job2.setReducerClass(PRReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
	        job2.waitForCompletion(true);
	        //will be true only after the completion of job

		}
    	//input file is in Result10, Output result will be at out folder
		Configuration sortConf = getConf(); 
		sortConf.set("path", args[1]+"R");
		Job job3 = Job.getInstance(sortConf, "PageRankSort");
		job3.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job3, new Path(args[1]+"R"+iteration));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/"));
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		return job3.waitForCompletion(true) ? 0 : 1;

	}

	
   public static class LinkMapper extends Mapper<LongWritable, Text, Text, Text> {
		//finding of titles and links between the pages by parsing through the output file by the mapper
		
		private static final Pattern TITLE_PAT = Pattern.compile(".*<TITLE>(.*?)</TITLE>.*");
		// regular expression to find each title
		private static final Pattern LINKLIST_PAT = Pattern.compile(".*<text.*?>(.*?)</text>.*");	
		//regular expression to find each text body
		private static final Pattern LINK_PAT = Pattern.compile("\\[\\[(.*?)\\]\\]");
		//regular expression to find each link
		@Override
		public void map(LongWritable offset, Text lnTxt, Context context)
				throws IOException, InterruptedException {
			String l = lnTxt.toString();
			if(l.length()>0){
				String TITLE = "";
				
				//this gets the title of page 
				Matcher titRes = TITLE_PAT.matcher(l); 
				if(titRes.matches()){
					TITLE = titRes.group(1).toString();
				}
				
				Matcher lnkRes = LINKLIST_PAT.matcher(l);
                // this gets the the link
				StringBuffer sbuf = new StringBuffer("");
				if(lnkRes.matches()){
					String outLnk = lnkRes.group(1).toString();
					Matcher lnkmatch = LINK_PAT.matcher(outLnk);
					//looping to get the outlinks present between the text tag
					while(lnkmatch.find()){
						for(int i = 0; i <lnkmatch.groupCount() ;i++){
							sbuf.append(lnkmatch.group(i+1).toString()+"->");
							context.write(new Text(lnkmatch.group(i+1).toString()),new Text(""));
							context.write(new Text(TITLE.toString()),new Text(lnkmatch.group(i+1).toString()));
						}			
					}		
				}	
			}
		}
	}
	

	
	public static class LinkReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		//reducer class to write the node and also the outlinks from the node
		public void reduce(Text node, Iterable<Text> outLinkList, Context context)
				throws IOException, InterruptedException {
			//count inititaed to zero
			int cou = 0;
			//gets the total number of outlinks
			String TotalOutLinks = "";
			for (Text outLink : outLinkList) {
				if(outLink.toString().length()>0){
					TotalOutLinks = TotalOutLinks + "->"+ outLink.toString();
					cou++;
				}
			}
			String val = "TEMP@link"+cou+TotalOutLinks;
			//printing the node and its outlink
			context.write(node, new Text(val));
		}
	}

	
	
	public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
		// Mapper class used  to extract the page name and the outlinks
		
		public void map(LongWritable offset, Text lnTxt, Context context)
				throws IOException, InterruptedException {
			
				String l = lnTxt.toString();
				if(l.length()>0){
					// delimeter used as seperator for key value pair
					String[] lnspt = l.split("\\t");
					//Splitting the link at the delimeter('@link')
					String[] outLnkSpt =lnspt[1].trim().split("@link");

					if(outLnkSpt.length>1){
						if(outLnkSpt[0].equals("TEMP")){
							int nCou =Integer.parseInt( context.getConfiguration().get("nCou"));
							context.write(new Text(lnspt[0]), new Text(((1.0/(double)nCou))+""));
						}
						else{
							double pgRnk = Double.parseDouble(outLnkSpt[0].trim());
							String[] lnks = outLnkSpt[1].split("->");
							int lnkCou = Integer.parseInt(lnks[0].trim());
							if(lnkCou>0){
								double lvalue = (double)pgRnk/lnkCou;
								//Iterating through all the keys in the list
								for(int i = 1;i<lnks.length;i++){
									context.write(new Text(lnks[i]), new Text(lvalue+""));
								}
							}
						}
						context.write(new Text(lnspt[0]), new Text(lnspt[1]));
					}
				}
		}
	}
	
	

	public static class PRReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		//used to collect all the outlinks from a node
		public void reduce(Text node, Iterable<Text> outLinkList, Context context)
				throws IOException, InterruptedException {
			double PRval = 0;
			String preval = "";
			StringBuffer sbuf = new StringBuffer("");
			//looping through outlinks
			for (Text outLink : outLinkList) {
				if(outLink.toString().length()>0){
					if(outLink.toString().contains("@link")){
						//splitting to get rank and name of nodes
						String lnks[] = outLink.toString().split("@link");
						preval = preval + lnks[0];
						sbuf.append(lnks[1]);
					}
					else{
						PRval+=Double.parseDouble(outLink.toString());
					}
				}
			}

			if(preval.length()>0){
				if(!(preval.toString().equals("TEMP"))){
					double oldPRValue =  Double.parseDouble(preval.toString());
					double d1 = Math.abs(PRval-oldPRValue);
					double d2 = context.getConfiguration().getDouble("d2", 0);
					d2+=d1;
					context.getConfiguration().setDouble("d2", d2);
				}
				else{
					context.getConfiguration().setDouble("d2", 1);
				}
			}
			PRval+=(0.15);
			String val = PRval+"@link"+sbuf.toString();
			context.write(node, new Text(val));
		}
	}
	
	  public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
		//this mapper uses the result from the output and does the splitting of title and page rank and then sends to reducer
		public void map(LongWritable offset, Text lnTxt, Context context)
				throws IOException, InterruptedException {
			String l = lnTxt.toString();
			if(l.length()>0){
				String[] lnspt = l.split("\\t");
				String[] outLnkSpt =lnspt[1].trim().split("@link");
				context.write(new Text("sort"), new Text(lnspt[0]+"->"+outLnkSpt[0]));

			}
		}
	}
	
		
	public static class SortReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		//this reducer does the final sorting of pages or titles based on the value of page rank obtained
		public void reduce(Text node, Iterable<Text> outLinkList, Context context)throws IOException, InterruptedException {
			
	        String path = context.getConfiguration().get("path");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			//intermediate page rank files will be deleted from output path
			for(int i=1;i<=10;i++){
				fs.delete(new Path(path+i),true);
			}
			
			ArrayList<String> finTit = new ArrayList<String>();
			ArrayList<Double> finRnks = new ArrayList<Double>();
			//Sorting based on the ranks
			for (Text outLink : outLinkList) {
				if(outLink.toString().length()>0){
					if(outLink.toString().contains("->")){
						String PRanks[] = outLink.toString().split("->");
						double rnk = Double.parseDouble(PRanks[1].trim());
						int pos = sorting(rnk,finRnks.size(),finRnks);
						finTit.add(pos,PRanks[0].trim());
						finRnks.add(pos,rnk);
					}
				}  
			}
			for(int inx =0;inx<finTit.size();inx++){
				context.write(new Text(finTit.get(inx)), new Text(finRnks.get(inx)+""));
			}
		}
		public int sorting(double rnk, int sizeoflink,ArrayList<Double> finRnks){
			if(sizeoflink > 0) {
				int i =0;
				while(i < finRnks.size()) {
					if(finRnks.get(i)<rnk){
						break;
					}
					i++;
				}
				return i;
			} else return 0;
		}
	}
}
