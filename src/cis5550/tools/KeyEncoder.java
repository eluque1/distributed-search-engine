package cis5550.tools;

import java.util.HashSet;
import java.util.Set;

public class KeyEncoder {

  private static final Set<String> WINDOWS_RESERVED_NAMES = new HashSet<>();
  
  static {
    String[] reserved = {"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", 
                         "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", 
                         "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"};
    for (String name : reserved) {
      WINDOWS_RESERVED_NAMES.add(name);
    }
  }

  public static String encode(String key) {
    String allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
    String out = "";
    for (int i=0; i<key.length(); i++) {
    	if (allowedChars.contains(""+key.charAt(i)))
    		out = out + key.charAt(i);
      else
      	out = out + "_" + Integer.toHexString((int)key.charAt(i));
    }
    
    // Check if the encoded result is a Windows reserved name (case-insensitive)
    String upperOut = out.toUpperCase();
    int dotIndex = upperOut.indexOf('.');
    String baseName = dotIndex > 0 ? upperOut.substring(0, dotIndex) : upperOut;
    
    if (WINDOWS_RESERVED_NAMES.contains(baseName)) {
      // Prefix with underscore to avoid reserved name collision
      out = "_" + out;
    }
    
    return out;
  }

  public static String decode(String str) {
  	String out = "";
  	
  	// Handle Windows reserved name prefix
  	int startIdx = 0;
  	if (str.startsWith("_") && str.length() > 1) {
  	  // Check if this is our reserved name prefix (not an encoded character)
  	  // Encoded chars look like _XX where X is hex, our prefix is just _
  	  String afterUnderscore = str.substring(1);
  	  if (afterUnderscore.length() >= 1 && !isHexStart(afterUnderscore)) {
  	    startIdx = 1; // Skip the reserved name prefix
  	  }
  	}
  	
  	for (int i=startIdx; i<str.length(); i++) {
  		if (str.charAt(i) == '_') {
  			out = out + (char)(int)Integer.decode("0x"+str.substring(i+1, i+3));
  			i += 2;
  		} else {
  			out = out + str.charAt(i);
  		}
  	}
    return out;
  }
  
  private static boolean isHexStart(String str) {
    if (str.length() < 2) return false;
    try {
      Integer.decode("0x" + str.substring(0, 2));
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static void main(String args[]) {
  	System.out.println(encode(args[0]));
  	System.out.println(decode(encode(args[0])));
  }
  
}