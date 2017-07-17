package com.sc4.analytics.RIVExamples.NDC;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p></p>
 * @author Steve Carton (stephen.carton@gmail.com) 
 * Jul 11, 2017
 *
 */
public class NDKey implements Serializable, Comparable<NDKey> {

	private static final long serialVersionUID = -6095322499484002914L;
	final static Logger logger = LoggerFactory.getLogger(NDKey.class);
	public String guid;
	public String docid;
	public String hash;
	public String text;
	public int len;
	public double score;
	
	public NDKey(){}
	
	public boolean matches(NDKey o) {
		return o.hash.equals(this.hash);
	}

	public boolean same(NDKey o) {
		return o.guid.equals(this.guid);
	}

	public boolean maybe(NDKey o) {
		int p = o.len/this.len;
		return p>= 0.8 && p <= 1.2;
	}

	@Override
	public int compareTo(NDKey o) {
		return this.guid.compareTo(o.guid);
	}
	@Override
    public int hashCode(){
        return guid.hashCode();
    }
	@Override
    public boolean equals(Object o){
    	return (o instanceof NDKey && this.guid.equals(((NDKey)o).guid));
    }
}