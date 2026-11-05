package cis5550.test;

import cis5550.flame.FlameSubmit;

public class RunIntersection {
  
	public static void main(String[] args) throws Exception {
    String resp = FlameSubmit.submit("localhost:9000",
        "tests/flame-intersection.jar",
        "cis5550.test.FlameIntersection", new String[0]);
    System.out.println(resp);
  }
	
}
