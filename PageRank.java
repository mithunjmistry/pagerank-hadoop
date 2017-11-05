// Mithun Mistry
// 800961418 - mmistry1@uncc.edu

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;


public class PageRank extends Configured implements Tool {
	
	static final String PageRankDelimiter = ",,,,,";
	static final String LinksDelimiter = "#####";

   @SuppressWarnings("unused")
private static final Logger LOG = Logger .getLogger( PageRank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      
      
      // Intermediate directory name
      String intermediate_directory_name = "intermediate_directory"; 

      String input_directory = args[0]; // Get input directory name
      Path output_directory = new Path(args[1]); // Get the output directory which will contain the sorted PageRank output
      Path intermediate_directory_path = new Path(intermediate_directory_name);
      
      Configuration configuration1 = new Configuration();
      
      Job job_count  = Job.getInstance(configuration1, "count");
      job_count.setJarByClass( this.getClass());
      
      FileInputFormat.addInputPaths(job_count,  input_directory); // Get the input directory
      
  	  
      FileSystem filesystem = FileSystem.get(configuration1);
      
      // Delete directory if they already exists
      try {
			if (filesystem.exists(output_directory)) {
				filesystem.delete(output_directory, true);
			}
			if (filesystem.exists(intermediate_directory_path)) {
				filesystem.delete(intermediate_directory_path, true);
			}
			filesystem.mkdirs(intermediate_directory_path);

		} catch (IOException e) {
			e.printStackTrace();
		}
      
      Path count_output = new Path(intermediate_directory_path, "count");
      FileOutputFormat.setOutputPath(job_count,  count_output);
      
      job_count.setMapperClass( MapCount .class);
      // Combiner class increases the performance dramatically for counting N
      job_count.setCombinerClass(ReduceCount.class);
      job_count.setReducerClass( ReduceCount .class);
      
      job_count.setMapOutputKeyClass(Text.class);
      job_count.setMapOutputValueClass(DoubleWritable.class);
      

      int success = job_count.waitForCompletion( true)  ? 0 : 1;
      
      // Wait for the job to complete, if it completes successfully, move forward to make Link Graph.
      if(success == 0){
    	  // The count program executed successfully
    	  // Start making a LinkGraph
    	  
    	  Configuration configuration2 = new Configuration();
    	  
    	  Path linkgraph = new Path(intermediate_directory_path, "linkgraph");
    	  
    	  String line;
    	  double number_of_lines = 1; // Initialized and not set to 0 because if something wrong happens we don't want our program to fail
    	  
    	  // Get the number of pages i.e. N required for our PageRank
    	  try {
    		  // get N from our previous mapreduce job output
				FileSystem filesystem2 = FileSystem.get(configuration2);
				Path count_output_path = new Path(count_output, "part-r-00000");

				BufferedReader br = new BufferedReader(new InputStreamReader(filesystem2.open(count_output_path)));
				while ((line = br.readLine()) != null) {
					if (line.trim().length() > 0) {
						System.out.println(line);
						String[] line_split = line.split("\\s+");
						number_of_lines = Double.parseDouble(line_split[1]);
					}
				}
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
    	  
    	  // Set number of lines in configuration to use it in our program
    	  configuration2.set("number_of_lines", String.valueOf(number_of_lines));
    	  Job link_graph_job = Job.getInstance(configuration2, "linkgraph");
    	  
    	  link_graph_job.setJarByClass(this.getClass());
    	  link_graph_job.setMapperClass( MapLinkGraph.class);
    	  link_graph_job.setReducerClass( ReduceLinkGraph.class);
    	  
    	  FileInputFormat.addInputPaths(link_graph_job, args[0]);
    	  FileOutputFormat.setOutputPath(link_graph_job, linkgraph);
    	  
    	  // Specify key and value class
    	  link_graph_job.setMapOutputKeyClass(Text.class);
    	  link_graph_job.setMapOutputValueClass(Text.class);
    	  link_graph_job.setOutputKeyClass(Text.class);
    	  link_graph_job.setOutputValueClass(Text.class);
    	  
    	  success = link_graph_job.waitForCompletion(true) ? 0 : 1;
    	  // Wait for the link graph to be generated
    	  
    	  if(success == 0){
    		  // This means that link graph was generated successfully
    		  for(int i=1; i < 11; i++){
    			  // Run the iterations 10 times for the PageRanks to converge
    			  
    			  Job page_rank_job = Job.getInstance(getConf(), "pagerank");
    			  Configuration configuration3 = page_rank_job.getConfiguration();
    			  
    			  // Create iteration(i) where i = 1 to 10 directory in our intermediate path
    			  Path intermediate_pagerank_path = new Path(intermediate_directory_path, "iteration" + String.valueOf(i));
    			  // This is to delete old iteration values which we don't need anymore
    			  Path old_intermediate_pagerank_path = new Path(intermediate_directory_path, "iteration" + String.valueOf(i-2));
    			  
    			  page_rank_job.setJarByClass(this.getClass());
    			  page_rank_job.setMapperClass(MapPageRank.class);
    			  page_rank_job.setReducerClass(ReducePageRank.class);
    			  
    			  FileInputFormat.addInputPath(page_rank_job, linkgraph);
    			  FileOutputFormat.setOutputPath(page_rank_job, intermediate_pagerank_path);
    			  
    			  FileSystem filesystem3 = FileSystem.get(configuration3);
    			  try { // Delete old iteration directory which are no longer needed
    					if (filesystem3.exists(old_intermediate_pagerank_path)) {
    						filesystem3.delete(old_intermediate_pagerank_path, true);
    					}

    				} catch (IOException e) {
    					e.printStackTrace();
    				}
    			  
    			  // set input and output format class to use URL as offset instead of the actual offset
    			  page_rank_job.setInputFormatClass(KeyValueTextInputFormat.class);
    			  page_rank_job.setOutputFormatClass(TextOutputFormat.class);
    			  
    			  // set reducer output key and value class
    			  page_rank_job.setOutputKeyClass(Text.class);
    			  page_rank_job.setOutputValueClass(Text.class);
    			  
    			  success = page_rank_job.waitForCompletion(true) ? 0 : 1;
    			  
    			  // set the intermediate directory, i.e. iteration(i) as the input for iteration(i+1)
    			  linkgraph = intermediate_pagerank_path;
    		  }
    	  }
    	  
    	  
    	  // Check if PageRank computation completed successfully
    	  if(success == 0){
    		  // PageRank computed successfully
    		  // Sort the output
    		  
    		  System.out.println("<---------Sorting the output now--------->");
    		  
    		  
    		  Job pagerank_sort_job = Job.getInstance(getConf(), "pageranksort");
    		  Configuration configuration4 = pagerank_sort_job.getConfiguration();
    		  
    		  pagerank_sort_job.setJarByClass( this.getClass());
    	      
    	      // Here, linkgraph is assigned as last iteration i.e. iteration10 which will be input for our sorting program
    		  FileInputFormat.addInputPath(pagerank_sort_job,  linkgraph);
    		  // This is our last phase, so set output_directory path which we got as an argument
    	      FileOutputFormat.setOutputPath(pagerank_sort_job,  output_directory);
    	      // As stated, number of reducers set to one so that it is easy to sort it
    	      pagerank_sort_job.setNumReduceTasks(1);
    	      
    	      // comparator class to use descending sort comparator to sort page ranks
    	      pagerank_sort_job.setSortComparatorClass(PageRankComparator.class);
    	      // set mapper and reducer class
    	      pagerank_sort_job.setMapperClass(MapPageRankSorter.class);
    	      pagerank_sort_job.setReducerClass(ReducePageRankSorter.class);
    	      
    	      // set format class to use URL as offset instead of an actual offset
    	      pagerank_sort_job.setInputFormatClass(KeyValueTextInputFormat.class);
    	      pagerank_sort_job.setOutputFormatClass(TextOutputFormat.class);
    	      
    	      pagerank_sort_job.setMapOutputKeyClass(DoubleWritable.class);
    	      pagerank_sort_job.setMapOutputValueClass(Text.class);
    	      
    	      pagerank_sort_job.setOutputKeyClass(Text.class);
    	      pagerank_sort_job.setOutputValueClass(DoubleWritable.class);
    	      FileSystem filesystem4 = FileSystem.get(configuration4);
    	      
    	      success = pagerank_sort_job.waitForCompletion(true) ? 0 : 1;
    	      
    	      // Check if sorting was done successfully
    	      if(success == 0){
    	    	  try {
    	  			if (filesystem4.exists(intermediate_directory_path)) {
    	  				filesystem4.delete(intermediate_directory_path, true);
    	  			}
    	  			// Clear all intermediate directory as we do not need them anymore

    	  		} catch (IOException e) {
    	  			e.printStackTrace();
    	  		}
    	      }
    	      
    	  }
      }
      
      return success;
      
      
   }
   
   public static class MapCount extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
	   private final static DoubleWritable one  = new DoubleWritable(1);

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  String line  = lineText.toString();
    	  // Just checking if the line is not empty
    	  if(!line.isEmpty()){
    		  context.write(new Text("count"), one); // emit one for each line (line is a page in our data set)
    	  }
      }
   }

   public static class ReduceCount extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text count,  Iterable<DoubleWritable > ones,  Context context)
         throws IOException,  InterruptedException {
         // We will save total_lines in this double
         double total_lines = 0;
         for ( DoubleWritable counts  : ones) {
        	 // calculating total lines
             total_lines  += counts.get();
         }
         // give total count	
         context.write(count,  new DoubleWritable(total_lines));
      }
   }
	
   
   public static class MapLinkGraph extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  String line  = lineText.toString().trim();
    	  String URL = ""; // Initialization
    	  
    	  String number_of_lines = context.getConfiguration().get("number_of_lines"); // This is our N initially
    	  double initial_pagerank = 1 / Double.parseDouble(number_of_lines); // Set initial page rank to 1/N
    	  
    	  
    	  try { // To avoid program from crashing on any bad link
	    	  Pattern pattern = Pattern.compile("<title>(.*?)</title>"); // The name of the URL is text in title, so getting the URL name
	    	  Matcher matcher = pattern.matcher(line);
	    	  while(matcher.find()){
	    		  URL = matcher.group(1);
	    	  }
	    	  
	    	  Pattern pattern_text = Pattern.compile("<text(.*?)</text>"); // Get contents from text tag to further extract outlinks
	    	  Matcher matcher_text = pattern_text.matcher(line);
	    	  
	    	  String link_graph_value = Double.toString(initial_pagerank) + PageRankDelimiter; // This will be value emitting from Mapper
	    	  // The value will be of form page_rank,,,,,outlink1#####outlink2 ... and so on.
	    	  
	    	  while(matcher_text.find()){
	    		  String string_text = matcher_text.group(1);
					Pattern pattern_outlinks = Pattern.compile("\\[\\[(.*?)\\]\\]"); // Find outlinks represented in [[ ]] from the text tag
					Matcher matcher_outlinks = pattern_outlinks.matcher(string_text);
					while (matcher_outlinks.find()) {
						String outlinks = matcher_outlinks.group(1).replace("[[", "").replace("]]", ""); // remove [[ and ]] to just keep the name of outlink
						//concatenate all out links with a delimiter
						link_graph_value += outlinks + LinksDelimiter;
						// Keep adding outlinks with delimiter to the output value
					}  
	    	  }
	    	  
	    	  if(!URL.isEmpty() && !link_graph_value.isEmpty()){
	    		  // emit url as key and initial_pagerank,,,,,outlink1#####outlink2 .. as value
	    		  context.write(new Text(URL), new Text(link_graph_value));
	    	  }
    	  } 
    	  catch(Exception e) {
				e.printStackTrace();
		}
      }
   }

   public static class ReduceLinkGraph extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text URL,  Iterable<Text> link_graph_value,  Context context)
         throws IOException,  InterruptedException {
    	  // Reducer is an identity reducer. In short, it does nothing
    	  for (Text value : link_graph_value) {
				context.write(URL, value);
			}
      }
   }
   
   public static class MapPageRank extends Mapper<Text ,  Text ,  Text ,  Text > {

	      public void map( Text offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	  String line  = lineText.toString().trim();
	    	  // Initialize variables that we are going to use
	    	  double page_rank = -1;
	    	  String[] outlinks = {};
	    	  
	    	  String[] pr_and_outlinks = line.split(PageRankDelimiter); // split on page delimiter
	    	  if(pr_and_outlinks.length == 2){
	    		page_rank = Double.parseDouble(pr_and_outlinks[0]); // Get the pagerank value
	    		outlinks = pr_and_outlinks[1].split(LinksDelimiter); // Get all the outlinks
	    	  }
	    	  
	    	  
	    	  if(outlinks.length > 0){ // If the page has outlinks, move forward
	    		  for(String outlink: outlinks){
	    			  if(!outlink.isEmpty() && page_rank != -1){
	    				  // Distribute Page rank across all the outlinks
	    				  context.write(new Text(outlink), new Text(String.valueOf(page_rank/outlinks.length)));
	    			  }
	    		  }
	    	  }
	    	  // Also write the original url with pagerank and outlinks as it will be required in reducer
	    	  if(!line.isEmpty() && !offset.toString().isEmpty()){
	    		  context.write(offset, new Text(line)); // Here, offset is the URL
	    	  }
	      }
	   }

	   public static class ReducePageRank extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text URL,  Iterable<Text> value,  Context context)
	         throws IOException,  InterruptedException {
	    	  // Initialize variables
	    	  double new_pagerank = 0;
	    	  boolean pr_with_outlinks_check = false; // boolean to check if original url is present or not
	    	  String outlinks = "";
	    	  for (Text contents : value) {
	    		  
	    		  // check if the input has original line with url and pagerank,,,,outlinks#####
	    		  if(contents.toString().contains(PageRankDelimiter)){
	    			  pr_with_outlinks_check = true; // set it to true as it has
	    			  
	    			  String[] content_split = contents.toString().split(PageRankDelimiter);
	    			  if(content_split.length == 2){
	    				  outlinks = content_split[1]; // Strip off the page rank but keep the outlinks, page_rank will be substituted by the new one which we will calculate
	    			  }
	    		  }
	    		  else{
	    			  new_pagerank += Double.parseDouble(contents.toString()); // keep accumulating the page rank for the url
	    		  }
	    		  
				}
	    	  
	    	  // we are using damping on page rank. Formula is (1 - d) + d * page_ranks (lecture 11 slide)
	    	  double damped_pagerank = (0.15) + (0.85*new_pagerank); // here d=0.85
	    	  if(!URL.toString().isEmpty() && pr_with_outlinks_check){
	    		  // write URL as key and pagerank(calculated and damped),,,,,outlinks##### as it will be input to another iteration of page rank computation
	    		  context.write(URL, new Text(String.valueOf(damped_pagerank) + PageRankDelimiter + outlinks));
	    	  }
	      }
	   }
	   
	   public static class MapPageRankSorter extends Mapper<Text ,  Text ,  DoubleWritable ,  Text> {

		      public void map( Text url,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {
		    	  // We are getting rid of the out links as page rank iterations are already over and we don't need outlinks anymore
		    	  String page_rank = lineText.toString().split(PageRankDelimiter)[0];
		    	  // We can directly convert to double in context write but storing in string makes the code cleaner and readable
		    	  context.write(new DoubleWritable(Double.valueOf(page_rank)), url);
		      }
		   }

		   public static class ReducePageRankSorter extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		      @Override 
		      public void reduce( DoubleWritable pagerank,  Iterable<Text> URLS,  Context context)
		         throws IOException,  InterruptedException {
		    	  
		    	// looping through the URLs for each value
				// If we have multiple URLs with same rank then sort and print
		    	  for(Text url: URLS){
		    		  context.write(url, pagerank);
		    	  }
		      }
		   }
		   
		   // comparator for sorting values in descending order
		   public static class PageRankComparator extends WritableComparator{
			   protected PageRankComparator(){
				   super();
				   // from documentation
			   }
			   
			   public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
				   	// from Secondary Sort in Hadoop documentation
					double page_rank_one = WritableComparator.readDouble(arg0, arg1);
					double page_rank_two = WritableComparator.readDouble(arg3, arg4);
					if (page_rank_one > page_rank_two) {
						return -1;
					} else if (page_rank_one < page_rank_two) {
						return 1;
					}
					return 0;
				}
		   }
   
 }
