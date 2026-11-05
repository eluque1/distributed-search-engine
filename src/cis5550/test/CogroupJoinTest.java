package cis5550.test;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.*;
import java.util.*;
import java.net.*;

public class CogroupJoinTest extends GenericTest {

	void runSetup() {
	}

	void prompt() {
		/* Ask the user to confirm that the server is running */
		System.out.println("Make sure the student's solution is compiled and in a file called 'tests/flame-cogroupjoin.jar' in the local directory.");
		System.out.println("Then, in separate terminal windows, please run the following commands:");
		System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Coordinator 8000");
		System.out.println("  rm -rf worker1; java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000");
		System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.Coordinator 9000 localhost:8000");
		System.out.println("  java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.Worker 9001 localhost:9000");
		System.out.println("... and then hit Enter in this window to continue.");
		(new Scanner(System.in)).nextLine();
	}

	void cleanup() {
	}

	void runTests(Set<String> tests) throws Exception {
		KVSClient kvs = new KVSClient("localhost:8000");

		System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
		System.out.println("--------------------------------------------------------");

		if (tests.contains("cogroupjoin-basic")) try {
			startTest("cogroupjoin-basic", "Basic cogroupJoin test", 20);
			try {
				// Test case: keys in both tables, keys only in table1, keys only in table2
				// Format: "key value"
				String[] args = new String[] {
					"fruit apple",    // table1
					"fruit banana",   // table1 (same key, different value)
					"color red",       // table1
					"fruit cherry",   // table2 (same key as table1)
					"fruit date",     // table2 (same key as table1)
					"shape circle"    // table2 (unique key)
				};
				
				String response = FlameSubmit.submit("localhost:9000", "tests/flame-cogroupjoin.jar", "cis5550.test.FlameCogroupJoin", args);
				if (response == null)
					testFailed("We submitted a job (tests/flame-cogroupjoin.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n" + FlameSubmit.getErrorResponse());
				
				// Debug: Print the actual response
				System.out.println("DEBUG: Actual response: '" + response + "'");
				System.out.println("DEBUG: Response length: " + (response != null ? response.length() : 0));
				
				// Expected: All keys should appear
				// - fruit: should have values from both tables (apple,banana,cherry,date or similar)
				// - color: should have value from table1 only (red)
				// - shape: should have value from table2 only (circle)
				
				// Parse the response to verify
				if (response != null && response.contains("fruit") && response.contains("color") && response.contains("shape")) {
					testSucceeded();
				} else {
					testFailed("Expected response to contain keys 'fruit', 'color', and 'shape', but got: '" + response + "' (response is " + (response == null ? "null" : "empty") + ")");
				}
			} catch (ConnectException ce) {
				testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
			}
		} catch (Exception e) {
			testFailed("An exception occurred: " + e, false);
			e.printStackTrace();
		}

		if (tests.contains("cogroupjoin-overlap")) try {
			startTest("cogroupjoin-overlap", "cogroupJoin with overlapping keys", 15);
			try {
				// Test case: keys that appear in both tables
				String[] args = new String[] {
					"a 1",  // table1
					"b 2",  // table1
					"a 3",  // table2 (overlaps with table1)
					"b 4"   // table2 (overlaps with table1)
				};
				
				String response = FlameSubmit.submit("localhost:9000", "tests/flame-cogroupjoin.jar", "cis5550.test.FlameCogroupJoin", args);
				if (response == null)
					testFailed("We submitted a job (tests/flame-cogroupjoin.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n" + FlameSubmit.getErrorResponse());
				
				// Both keys 'a' and 'b' should appear in the result
				if (response.contains("(a,") && response.contains("(b,")) {
					testSucceeded();
				} else {
					testFailed("Expected response to contain keys 'a' and 'b', but got: " + response);
				}
			} catch (ConnectException ce) {
				testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
			}
		} catch (Exception e) {
			testFailed("An exception occurred: " + e, false);
			e.printStackTrace();
		}

		if (tests.contains("cogroupjoin-empty")) try {
			startTest("cogroupjoin-empty", "cogroupJoin with one empty table", 10);
			try {
				// Test case: one table is empty
				String[] args = new String[] {
					"x 1",  // table1
					"y 2"   // table1
					// table2 is empty
				};
				
				String response = FlameSubmit.submit("localhost:9000", "tests/flame-cogroupjoin.jar", "cis5550.test.FlameCogroupJoin", args);
				if (response == null)
					testFailed("We submitted a job (tests/flame-cogroupjoin.jar) to the Flame coordinator, but it looks like the job failed. Here is the error output we got:\n\n" + FlameSubmit.getErrorResponse());
				
				// Keys from table1 should still appear
				if (response.contains("(x,") && response.contains("(y,")) {
					testSucceeded();
				} else {
					testFailed("Expected response to contain keys 'x' and 'y' even with empty table2, but got: " + response);
				}
			} catch (ConnectException ce) {
				testFailed("We were not able to connect to the Flame coordinator at localhost:9000. Verify that the coordinator is running and hasn't crashed?");
			}
		} catch (Exception e) {
			testFailed("An exception occurred: " + e, false);
			e.printStackTrace();
		}

		System.out.println("--------------------------------------------------------\n");
		if (numTestsFailed == 0)
			System.out.println("Looks like your solution passed all of the selected tests. Congratulations!");
		else
			System.out.println(numTestsFailed + " test(s) failed.");

		cleanup();
		closeOutputFile();
	}

	public static void main(String args[]) throws Exception {
		/* Make a set of enabled tests. If no command-line arguments were specified, run all tests. */
		Set<String> tests = new TreeSet<String>();
		boolean runSetup = true, runTests = true, promptUser = true, outputToFile = false, exitUponFailure = true, cleanup = true;

		if ((args.length > 0) && args[0].equals("auto")) {
			runSetup = false;
			runTests = true;
			outputToFile = true;
			exitUponFailure = false;
			promptUser = false;
			cleanup = false;
		} else if ((args.length > 0) && args[0].equals("setup")) {
			runSetup = true;
			runTests = false;
			promptUser = false;
			cleanup = false;
		} else if ((args.length > 0) && args[0].equals("cleanup")) {
			runSetup = false;
			runTests = false;
			promptUser = false;
			cleanup = true;
		}

		if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto")) {
			tests.add("cogroupjoin-basic");
			tests.add("cogroupjoin-overlap");
			tests.add("cogroupjoin-empty");
		}

		for (int i = 0; i < args.length; i++)
			if (!args[i].equals("all") && !args[i].equals("auto") && !args[i].equals("setup") && !args[i].equals("cleanup"))
				tests.add(args[i]);

		CogroupJoinTest t = new CogroupJoinTest();
		t.setExitUponFailure(exitUponFailure);
		if (outputToFile)
			t.outputToFile();
		if (runSetup)
			t.runSetup();
		if (promptUser)
			t.prompt();
		if (runTests)
			t.runTests(tests);
		if (cleanup)
			t.cleanup();
	}
}

