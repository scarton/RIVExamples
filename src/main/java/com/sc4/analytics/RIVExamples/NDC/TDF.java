package com.sc4.analytics.RIVExamples.NDC;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.druidgreeneyes.rivet.core.labels.RIVs;

import scala.Tuple2;

/**
 * <p>
 * RIV Near Dupe Functions. Mostly static functions to support the n2 process of
 * comparing docs via L1 RIVs. Builds a map of words and RIVs to re-use the RIVs
 * instead of regenerating them.
 * 
 * This version works of text files, the file path/name is the ID, the contents are used for RIVs.
 * </p>
 * 
 * @author Steve Carton (stephen.carton@gmail.com) Jul 11, 2017
 *
 */
public class TDF {

	final static Logger logger = LoggerFactory.getLogger(TDF.class);
	private static final double THRESHOLD=0.75;
	private static ConcurrentMap<String, TextRiv> textMap;

	public static NearDupe makeND(String id, String sp) {
		return new NearDupe(id,new File(sp+id).length());
	}
	public static Tuple2<NearDupe,NearDupe> flipNds(Tuple2<NearDupe,NearDupe> t) {
		if (t._1.compareTo(t._2)>0)
			return new Tuple2<NearDupe,NearDupe>(t._2,t._1);
		else return t;
	}
	public static boolean cosimND(Tuple2<NearDupe,NearDupe> ab, String sp) {
		logger.debug("{} =? {}",ab._1.toString(),ab._2.toString());
		if (ab._1.maybe(ab._2)) {
			Tuple2<TextRiv,TextRiv> drs = makeRivs(ab,sp);
			double p = RIVs.similarity(drs._1.riv, drs._2.riv);
	//					logger.debug("{}/{} Score: {}",ab._1.guid,ab._2.guid,p);
			ab._2.s=p;
			return p >= THRESHOLD;
		}
		return false;
	}
	public static Tuple2<TextRiv,TextRiv> makeRivs(Tuple2<NearDupe,NearDupe> ab, String sp) {
		TextRiv a = makeOrGetDr(ab._1, sp);
		TextRiv b = makeOrGetDr(ab._2, sp);
		return new Tuple2<TextRiv,TextRiv>(a,b);
	}
	private static TextRiv makeOrGetDr(NearDupe key, String sp) {
		if (textMap==null)
			textMap = new ConcurrentHashMap<>();
		if (textMap.containsKey(key.g)) {
//			logger.debug("Cached RIV: {}",key);
			return textMap.get(key.g);
		} else {
			TextRiv dr = new TextRiv(key, sp);
			dr.rivet();
			textMap.put(key.g, dr);
//			logger.debug("New RIV: {} mag: {}",key,dr.riv.magnitude());
			return dr;
		}
	}

}
