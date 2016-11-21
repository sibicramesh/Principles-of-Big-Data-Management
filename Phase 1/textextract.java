package ass1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class textextract {
  public static void main(String[] args) throws FileNotFoundException, IOException 
{
String sCurrentLine; 
BufferedReader br = new BufferedReader(new FileReader("D:\\Principles of big data\\Assignments\\tweetscollects\\ass1\\fetched_tweets.json"));

while ((sCurrentLine = br.readLine()) != null) {	
 Pattern pattern = Pattern.compile("\"text\":\"(.*?)\",\"source\"");   
 Matcher matcher = pattern.matcher(sCurrentLine);
   while (matcher.find()) {
    System.out.println(matcher.group(1));
   try {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("D:\\text_tweets.txt", true)));
    out.println(matcher.group(1));
    out.close();
    break;}
   catch (IOException e) {}  
   }
  }    
 }
}