package cis5550.test;

import cis5550.flame.FlameSubmit;

public class RunGroupBy {
	  public static void main(String[] args) throws Exception {
		    String resp = FlameSubmit.submit(
		        "localhost:9000",
		        "tests/flame-groupby.jar",         
		        "cis5550.test.FlameGroupBy",       
		        new String[0]
		    );
		    System.out.println(resp);
		  }
}
