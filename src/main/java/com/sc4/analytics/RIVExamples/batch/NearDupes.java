package com.sc4.analytics.RIVExamples.batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sc4.analytics.RIVExamples.NDC.NearDupe;
import com.sc4.analytics.RIVExamples.NDC.TDF;
import com.sc4.analytics.RIVExamples.utils.Props;

import scala.Tuple2;

/**
 * <p>RIV based Near Duping - Basic form. Accepts data from files. Logs Near Dupes with scores.</p>
 * <p>Typically run with properties set as -D properties on the command line...
 * <li>-Dsource.path=... path to source text files
 * <li>-Dspark=local|...
 * <li>-Dthreads=... some number of threads to run
 * <li>-Dlog.level=DEBUG|INFO
 * <li>-Dlog.path=path for log output
 * </p>
 * <code>java -jar xxxx -Dsource.path=c:\temp\data -Dlog.path=c:\temp\logs -Dlog.level-INFO -Dspark=local -Dthreads=3 com.sc4.analytics.RIVExamples.batch.NearDupes</code>
 * @author Steve Carton (stephen.carton@gmail.com) Jul 11, 2017
 * May 18, 2017
 *
 */
public class NearDupes {

	static {
		System.setProperty("log.file", "NearDupes");
	}
	final static Logger logger = LoggerFactory.getLogger(NearDupes.class);
	
	
	/**
	 * <p>Logs a document ID and all of the detected Near Dupes</p>
	 * @param ndt
	 * @param pivots
	 * @return
	 */
	private static void logNearDupes(Tuple2<NearDupe,Iterable<NearDupe>> ndt) {
		String ndres = NearDupe.join(", ", ndt._2);
		logger.info("{} => {}",ndt._1.g, ndres.length()>0?ndres:"No Near Dupes.");
	}
	/**
	 * <p>limit the amount of logging coming from Spark et al</p>
	 * @param l
	 */
	private static void setLogging(Level l) {
		org.apache.log4j.Logger.getLogger("com").setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger.getLogger("edu").setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR);	
		org.apache.log4j.Logger.getLogger("cobra").setLevel(l);
	}

	/**
	 * <p>Create a Spark Context</p>
	 * @param props
	 * @param appName
	 * @return
	 * @throws IOException
	 */
	private static JavaSparkContext createSparkContext(Props props, String appName) throws IOException {
		SparkConf sparkConf;
		if (props.spark().startsWith("local")) {
			String c = props.spark().equals("local")?"local["+props.threads()+"]":props.spark();
			sparkConf = new SparkConf()
				.setAppName(appName)
				.setMaster(c);
		} else {
			sparkConf = new SparkConf()
				.setAppName(appName);
		}
		return new JavaSparkContext(sparkConf);
	}

	public static void main(String[] args) throws IOException, SparkException {
		Props props = new Props();
		setLogging(props.logLevel());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String sp = props.sourcePath();
		JavaSparkContext jsc = createSparkContext(props, "NearDupes");
		logger.info("Starting Near Duplicate (O) Riveter, Sources: {}",sp);
		logger.info("Spark: {}, Threads: {}",jsc.master(),props.threads());
	
		/*
		 * Makes an RDD of NearDupe objects, empty except for the keys and lengths
		 */
		List<String> files = Files.walk(Paths.get(props.sourcePath()))
	    	.filter(p -> p.toString().endsWith(".txt"))
	    	.map(p -> p.toString().substring(sp.length()))
	    	.collect(Collectors.toList());
		JavaRDD<NearDupe> idTextRdd = jsc.parallelize(files, props.threads())
			.map(id->TDF.makeND(id,sp));
		idTextRdd.cache();
//		logger.info("Comparing {} total docs.",idTextRdd.count());
		/*
		 * takes the IDs created from the query RDD and create a cartesian product yielding an n^2 list of ID pairs
		 * Then map these to a new pair with the IDs ordered (a,b stays a,b. b,a become a,b)
		 * Then remove the duplicate IDs, so RDD is now 50% sized.
		 * Next create RIVs for each pair and filter on Cosine Similarity.
		 * The collect back into a list of pairs. Each pair is an ID and a list of IDs that are its near dupes.
		 */
		List<Tuple2<NearDupe,Iterable<NearDupe>>> nearDupes = idTextRdd
			.cartesian(idTextRdd)
			.mapToPair(TDF::flipNds)
			.distinct()
			.filter(ab -> TDF.cosimND(ab,sp))
			.groupByKey()
			.collect();
		
		jsc.parallelize(nearDupes,props.threads())
			.foreach(u -> logNearDupes(u));

		jsc.stop();
		jsc.close();
		stopWatch.stop();
		logger.info("End of Job. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
		System.out.println("End of Job. Elapsed time: "+DurationFormatUtils.formatDuration(stopWatch.getTime(), "HH:mm:ss.S"));
	}
}
