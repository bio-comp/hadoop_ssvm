import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class StructuredOutput extends Configured implements Tool {

	/**
	 * Set up map reduce environment and execute.
	 * @param arg0 Program args from main()
	 * @return integer value indicating success or failure
	 */	
	public int run(String[] arg0) throws Exception {
		ArrayList<Integer> nMapVals = new ArrayList<Integer>();
		nMapVals.add(new Integer(1) );
		for( int i = 10; i <= 100; i+=10)
			nMapVals.add(new Integer(i) );
		ArrayList<Integer> nRedVals = new ArrayList<Integer>();
		
		nRedVals.add(new Integer(1) );
		for( int i = 2; i <= 10; i++ )
			nRedVals.add(new Integer(i) );


		// Parse initialization file
		JobConf conf_init = new JobConf(getConf(), StructuredOutput.class);
		FileSystem fs_init = FileSystem.get(conf_init);
		Path path_init = new Path("init//init_alpha.txt");
		FSDataInputStream fin_init = fs_init.open(path_init);
		Scanner scan = new Scanner(fin_init).useDelimiter("\n");
		// Alpha matrix, contains dual variables and window strings
		HashMap<String, HashMap<String, Alpha_String>> alpha = 
			new HashMap<String, HashMap<String, Alpha_String>>();

		// Keeps track of training accurracies
		ArrayList<Float> accys = new ArrayList<Float>();

		while (scan.hasNext()) {
			String str = scan.next();
			if (str.length() == 0) {
				continue;
			}
			str = str.trim();
			String[] outer = str.split("\\s+");
			String protID = outer[0];
			StringTokenizer st = new StringTokenizer(outer[1]);
			while (st.hasMoreTokens()) {
				String current_update = st.nextToken(";");
				StringTokenizer st1 = new StringTokenizer(current_update);
				while (st1.hasMoreTokens()) {
					String bounds = st1.nextToken(",");
					String seq = st1.nextToken(",");
					float c_value = Float.parseFloat(st1.nextToken());

					if( !alpha.containsKey(protID)){
						HashMap<String, Alpha_String> inner_map = 
							new HashMap<String, Alpha_String>();
							alpha.put(protID, inner_map);
					}
					HashMap<String, Alpha_String> inner_map = alpha.remove(protID);
					if( inner_map.containsKey(bounds)){
						Alpha_String curr_as = inner_map.remove(bounds);
						curr_as.update_alpha(c_value);
						inner_map.put(bounds, curr_as);
					}
					else{
						Alpha_String as = new Alpha_String(c_value, seq );
						inner_map.put(bounds, as);
					}						
					alpha.put(protID, inner_map);

				}
			}
		}

		int num_reducers = 1;//nRedVals.get(j).intValue();
		
		for( int p = 0; p < 30; ++ p){
			System.out.println( "Current epoch: " + (p+1) );
			JobConf conf = new JobConf(getConf(), StructuredOutput.class);
			conf.setJobName("struct_output");
			// specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			// specify input and output DIRECTORIES (not files)
			Path inDir = new Path("in");
			Path outDir = new Path("out");

			conf.setInputPath(inDir);
			conf.setOutputPath(outDir);
			FileSystem fs = FileSystem.get(conf);
			conf.setNumMapTasks(40);
			conf.setNumReduceTasks(num_reducers);
			Path tmp_path = new Path( "/tmp//alpha.ser");

			Path alpha_path = new Path( "cache//alpha.ser");
			ObjectOutputStream output = new ObjectOutputStream(           
					new FileOutputStream( tmp_path.toString() ) );		
			output.writeObject(alpha);
			output.close();
			if( fs.exists( new Path( "cache")))
				fs.delete(new Path( "cache"));
			fs.copyFromLocalFile(tmp_path, alpha_path);

			DistributedCache.addCacheFile(new URI("/user//marcel777//cache//alpha.ser#alpha.ser"), conf);

			// remove old out dir if it is there
			if(fs.exists(outDir)) fs.delete(outDir);

			// Indicate input format as reading a lines in the file
			conf.setInputFormat(TextInputFormat.class);

			// specify mapper
			conf.setMapperClass(Map.class);

			// specify reducer
			conf.setReducerClass(Reducer.class);

			JobClient.runJob(conf);

			Path path_out = new Path("out//part-00000");
			int num_correct = 0;



			FSDataInputStream fin_out = fs_init.open(path_out);
			scan = new Scanner(fin_out).useDelimiter("\n");
			while (scan.hasNext()) {
				String str = scan.next();
				if (str.length() == 0) {
					continue;
				}

				str = str.trim();
				String[] outer = str.split("\\s+");
				String protID = outer[0];
				StringTokenizer st = new StringTokenizer(outer[1]);
				while (st.hasMoreTokens()) {
					String current_update = st.nextToken(";");
					StringTokenizer st1 = new StringTokenizer(current_update);
					while (st1.hasMoreTokens()) {
						String bounds = st1.nextToken(",");
						String seq = st1.nextToken(",");
						float c_value = Float.parseFloat(st1.nextToken());
						if ( c_value == 0 ){
							num_correct += 1;
							continue;
						}
						if( !alpha.containsKey(protID)){

							HashMap<String, Alpha_String> inner_map = 
								new HashMap<String, Alpha_String>();
								alpha.put(protID, inner_map);
						}
						HashMap<String, Alpha_String> inner_map = alpha.remove(protID);
						if( inner_map.containsKey(bounds)){
							Alpha_String curr_as = inner_map.remove(bounds);
							curr_as.update_alpha(c_value);
							inner_map.put(bounds, curr_as);
						}
						else{
							Alpha_String as = new Alpha_String(c_value, seq );
							inner_map.put(bounds, as);
						}						
						alpha.put(protID, inner_map);

					}
				}				
			} // end token scan
			accys.add( new Float( (float) num_correct / (float) 170));
			System.out.println( (float) num_correct / (float) 170 );
			
		} // end for
		for ( int m = 0; m < accys.size(); ++m ){
			System.out.println( accys.get(m));

		}
			
		return 0;
	}



	//System.out.println( (float) num_correct / (float) N );



	 		



	/**
	 * Mapper Class
	 * @author Michael Hamilton
	 */
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, Text> {

		private Text protID = new Text();
		private int p = 2;
		private Path[] localFiles;

		/**
		 * Computes argmax value for sequence
		 */
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text rawLine,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {	
			Path alpha_name = localFiles[ 0 ];
			String line = rawLine.toString();
			ObjectInputStream input = new ObjectInputStream(            
					new FileInputStream( alpha_name.toString() ) );
			HashMap<String, HashMap<String, Alpha_String>> alpha;
			try {
				alpha = (HashMap<String, HashMap<String, Alpha_String>>) input.readObject();
				line = line.trim();
				StringTokenizer mainParser = new StringTokenizer( line );

				protID.set( mainParser.nextToken(";") );
				StringTokenizer locsParser = new StringTokenizer( mainParser.nextToken(";"));
				ArrayList<SubstringLocation> locs = new ArrayList<SubstringLocation>();

				while( locsParser.hasMoreTokens()){
					String raw_range = locsParser.nextToken(",");
					String range[] = raw_range.split(":");
					int start = Integer.parseInt(range[ 0 ] );
					int stop = Integer.parseInt( range[ 1 ]);
					locs.add( new SubstringLocation( start, stop) );
				}
				String seq = mainParser.nextToken(";") ; 
				int y_hat = argmax( seq, alpha);
				int hat_c = y_hat + 10;
				String y_hat_win = seq.substring(y_hat, y_hat+21);
				String y_hat_string = new Integer( y_hat).toString();
				y_hat_string += ":" + (y_hat + 21);

				int y_start = -1;
				int y_stop = -1;
				float offset = Float.POSITIVE_INFINITY;

				// find closet y
				for( int i = 0; i < locs.size(); ++i ){
					SubstringLocation curr_loc = locs.get(i);
					int cy_start = curr_loc.getStart();
					int cy_stop = curr_loc.getStop();
					int len = cy_stop - cy_start;
					int c = cy_start + (int)(( float)(cy_stop - cy_start ) / (float) len );
					float coffset = Math.abs( c - hat_c );
					if ( coffset < offset ){
						y_start = cy_start;
						y_stop = cy_stop;
						offset = coffset;
					}
				}

				// update alpha as necessary
				if( Math.abs( (hat_c - y_start )) < 10 ||   Math.abs( (hat_c - y_stop )) < 10){
					output.collect(protID, new Text( y_hat_string+","+ y_hat_win+","+0.0) );
				}
				else{
					output.collect(protID, new Text( y_hat_string+","+ y_hat_win+","+ -1.0) );
					for ( int i = 0; i < locs.size(); ++i ){
						SubstringLocation curr = locs.get(i);
						y_start = curr.getStart();
						y_stop = curr.getStop();
						y_hat_win = seq.substring(y_start, y_stop);
						y_hat_string = new Integer( y_start).toString();
						y_hat_string += ":" + y_stop;
						output.collect(protID, new Text( y_hat_string+","+ y_hat_win+","+ ( 1.0 / locs.size() )  ));
					}
				}

			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void configure(JobConf job) {
			// Get the cached archives/files
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/**
		 * Find the most compatible label for seq
		 * @param seq the sequence to compute
		 * @param alpha contains all non-zero alpha values and corresponding window sequences
		 * @return y_hat label with maximum discriminant value
		 */
		private int argmax( String seq, HashMap<String, HashMap<String, Alpha_String>> alpha){
			int y_hat = -1;
			double max = Double.NEGATIVE_INFINITY;


			for (int i = 0; i < seq.length( ) - 20; i+=5 ){ // iterate over all possible labelings of x
				float sum_seqPrimes = 0;  // holds discriminant function value for current labeling
				String win = seq.substring(i, i+21); // window implied by labeling
				Set<String> keys = alpha.keySet(); // contains sequences with non-zero alpha
				Iterator<String> itr = keys.iterator();
				while( itr.hasNext() ){ // iterate over x's with non-zero alpha sequences
					String x_prime = itr.next(); // get x' label
					HashMap<String, Alpha_String> x_prime_alphas = alpha.get( x_prime ); // label alphas for x'
					Set<String> inner_key_set = alpha.get( x_prime ).keySet(); // contains alphas and sequences for prime sequence
					Iterator<String> inner_itr = inner_key_set.iterator();
					while( inner_itr.hasNext()){ // iterate over alphas and corresponding windows of x'
						Alpha_String y_prime_tuple = x_prime_alphas.get(inner_itr.next() );  //contains alpha-string values
						float alpha_prime = y_prime_tuple.get_alpha(); // alpha value for y'
						String win_prime = y_prime_tuple.get_seq();  // window implied by y'
						float k_val = kernel( win, win_prime ); // compute k( (x, y), (x', y') )
						sum_seqPrimes += alpha_prime * k_val; // add kernel value to discriminant value
					}
				}
				// check if we have a new label winner
				if ( sum_seqPrimes > max ){
					y_hat = i;
					max = sum_seqPrimes;
				}
			}
			return y_hat;
		}
		/**
		 * Compute pspectrum kernel 
		 * @param win
		 * @param win_prime
		 * @return
		 */
		private float kernel( String win, String win_prime){
			float kval = 0;
			HashMap<String, Integer> pspec = pspectrum( win );
			HashMap<String, Integer> pspec_prime = pspectrum( win_prime);

			String kmer;
			Set<String> keys = pspec.keySet();
			Iterator<String> itr = keys.iterator();

			double xTx = 0;

			// Compute xTxp
			while( itr.hasNext()){
				kmer = (String) itr.next();
				if( pspec_prime.containsKey(kmer)){
					kval += pspec.get(kmer).intValue() * pspec_prime.get(kmer).intValue();
				}				
				xTx += Math.pow(pspec.get(kmer), 2);
			}		

			// Compute xpTxp
			double xpTxp = 0;
			keys = pspec_prime.keySet();
			itr = keys.iterator();
			while( itr.hasNext()){
				kmer = (String) itr.next();
				xpTxp += Math.pow(pspec_prime.get(kmer), 2);			
			}
			return (float) Math.exp(-.7 * ( 2 - 2.0 * kval/ Math.sqrt( (xTx * xpTxp) )) );
		}

		/**
		 * Compute the p-spectrum for a given sequence
		 * @param win is the subsequence to process
		 * @return p-spectrum map of win
		 */
		private HashMap<String, Integer> pspectrum( String win){			
			HashMap<String, Integer> pspec = new HashMap<String, Integer>();
			for ( int i = 0; i < win.length() -p + 1; ++i){
				String kmer = win.substring(i, i+p);
				int count = 0;
				if ( pspec.containsKey(kmer) )
					count = pspec.remove(kmer);	
				pspec.put(kmer, new Integer( count + 1 ));				
			}

			return pspec;
		}

	}

	/**
	 * Reducer Class
	 * @author Mike Hamilton
	 */
	public static class Reducer extends MapReduceBase implements
	org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {
		/**
		 * Collects all updates for a sequences and writes
		 * them to file.
		 */
		public void reduce(Text protID, Iterator<Text> values,
				OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
			String alpha_updates = values.next().toString();
			while( values.hasNext() ){
				alpha_updates += ";" + values.next().toString();
			}
			collector.collect(protID, new Text( alpha_updates ));
		}
	}

	/**
	 * Dispatch to run method
	 * @param args Program arguments	
	 * @throws Exception Run() throws possible IO exception.
	 */
	public static void main( String args[]) throws Exception{
		long start = System.currentTimeMillis();
		int res = ToolRunner.run(new StructuredOutput(), args);
		long stop = System.currentTimeMillis();

		System.out.println( ( stop - start ) / 1000.0);
		System.exit(res); 
	}

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}
