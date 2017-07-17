package com.sc4.analytics.RIVExamples.NDC;

import java.io.Serializable;

public final class NearDupe implements CharSequence,  Comparable<NearDupe>, Serializable {
	private static final long serialVersionUID = 259715602288191666L;
	public String g;
	public long l;
	public Double s=0.0;
	public NearDupe(String g, long l) {
		this.g=g;
		this.l=l;
	}
	public boolean maybe(NearDupe o) {
		long p = o.l/this.l;
		return !this.g.equals(o.g) && p>= 0.8 && p <= 1.2;
	}

	@Override
	public int length() {
		return this.g.length();
	}
	@Override
	public char charAt(int index) {
		return this.g.charAt(index);
	}
	@Override
	public CharSequence subSequence(int start, int end) {
		return this.g.subSequence(start, end);
	}
	@Override
	public int compareTo(NearDupe o) {
		return this.g.compareTo(o.g);
	}
	@Override
    public int hashCode(){
        return this.g.hashCode();
    }
	@Override
    public boolean equals(Object o){
    	return (o instanceof NearDupe && this.g.equals(((NearDupe)o).g));
    }
	@Override
	public String toString() {
		return g+" ("+(Math.round(s*1000.0)/1000.0)+")";
//		return g;
	}
	public static String join(String d, Iterable<NearDupe> nds) {
		StringBuilder sb = new StringBuilder();
		for (NearDupe nd : nds) {
			if (sb.length()>0)
				sb.append(d);
			sb.append(nd.toString());
		}
		return sb.toString();
	}

}
