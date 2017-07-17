package com.sc4.analytics.RIVExamples.NDC;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.druidgreeneyes.rivet.core.extras.UntrainedCachedWordsMap;
import com.github.druidgreeneyes.rivet.core.labels.RIV;
import com.sc4.analytics.RIVExamples.riv.RIVConstants;
import com.sc4.analytics.RIVExamples.utils.Util;


/**
 * <p>Container for data needed for doc and RIV serialization</p>
 * @author Steve Carton (stephen.carton@gmail.com) 
 * Jul 11, 2017
 *
 */
public final class TextRiv extends NDKey implements Serializable{

	private static final long serialVersionUID = 8758078718408061236L;
	final static Logger logger = LoggerFactory.getLogger(TextRiv.class);
    private static ConcurrentHashMap<String, RIV> wordMap;
	public RIV riv;
	public TextRiv(NearDupe key, String path) {
		super();
		guid = key.g;
		riv = null;
//		text=Files.get(guid);
		try {
			text=FileUtils.readFileToString(new File(path+guid));
		} catch (IOException e) {
			logger.error(Util.stackTrace(e));
		}
	}
	public void rivet() {
		if (wordMap == null)
			wordMap = new ConcurrentHashMap<>();
		if (text!=null && riv==null) 
			riv = UntrainedCachedWordsMap.cacheingRivettizeText(wordMap, text, RIVConstants.RIV_WIDTH, RIVConstants.RIV_POINTS);
		text=null;
	}
}
