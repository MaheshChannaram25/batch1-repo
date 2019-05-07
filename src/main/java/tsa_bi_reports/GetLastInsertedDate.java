package tsa_bi_reports;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

public class GetLastInsertedDate implements Callable{
	public Object onCall(MuleEventContext eventContext) throws Exception {
		//System.out.println(eventContext.getMessage().getPayload());
		String selectQuery = eventContext.getMessage().getInvocationProperty("selectQuery");
		String dateParam = eventContext.getMessage().getInvocationProperty("dateParam");
		System.out.println("dateParam:::"+dateParam);
		String tableType = eventContext.getMessage().getInvocationProperty("tableType");
		int logOperation = 0;
		Date dNow = new Date( );
	    SimpleDateFormat ft = 
	    new SimpleDateFormat ("yyyy-MM-dd");
	    String dateCheck = ft.format(dNow);
	    System.out.println("Current Date: " + dateCheck);
	    List<?> payload = new ArrayList<Object>();
	    String lastInsertedDate = null;
		payload = (List<?>) eventContext.getMessage().getPayload();
		if(payload.size() > 0 ){
			if(tableType.equalsIgnoreCase("FACTTABLE")){
				Calendar cal = Calendar.getInstance();
		        cal.add(Calendar.DATE, -30);
		        Date todate1 = cal.getTime();    
		        String ExtractFromDate = ft.format(todate1);
		        System.out.println("ExtractFromDate:"+ExtractFromDate);
		        if(selectQuery.contains("${1}")){
					selectQuery = selectQuery.replace("${1}", ExtractFromDate);
					selectQuery = selectQuery.replace("${2}", dateCheck);
					selectQuery = selectQuery.replace("$>", ">");
		        }
		        else{
					selectQuery = selectQuery+"";
				}
			}else{
					Map<?,?> insertDate   = (Map<?,?>) payload.get(0);
					System.out.println("===insertDate:"+insertDate);
					if(insertDate.get("EndDate") != null){
						lastInsertedDate = insertDate.get("EndDate").toString();
					}
					if(dateParam != null){
						eventContext.getMessage().setInvocationProperty("logOperation", 0);
						if(selectQuery.contains("${1}")){
							if(lastInsertedDate != null){
								selectQuery = selectQuery.replace("${1}", lastInsertedDate.substring(0,lastInsertedDate.lastIndexOf(" ")));
								//selectQuery = selectQuery.replace("${1}", lastInsertedDate);
								selectQuery = selectQuery.replace("${2}", dateCheck);
								selectQuery = selectQuery.replace("$>", ">");
							}else{
								selectQuery = selectQuery.replace(selectQuery.substring(selectQuery.lastIndexOf("AND"), selectQuery.lastIndexOf("}'")+2),"");
								selectQuery = selectQuery.replace("$>", "<=");
								selectQuery = selectQuery.replace("${1}", dateCheck);
							}
							
						}
						if(selectQuery.contains("${}")){
							 new SimpleDateFormat ("yyyy");
							selectQuery = selectQuery.replace("${}", ft.format(dNow));
						}
						System.out.println("===selectQuery:"+selectQuery);
						
						//selectQuery = selectQuery+" WHERE "+dateParam+" > '"+lastInsertedDate.substring(0,lastInsertedDate.lastIndexOf(" "))+"' AND "+dateParam+" <= '"+dateCheck+"'";
					}
					else{
						selectQuery = selectQuery+"";
					}
				}
			
		}else{
			System.out.println("No last inserted Date in log table");
			if(dateParam != null ){
				eventContext.getMessage().setInvocationProperty("logOperation", 1);
				if(selectQuery.contains("${1}")){
					selectQuery = selectQuery.replace(selectQuery.substring(selectQuery.lastIndexOf("AND"), selectQuery.lastIndexOf("}'")+2),"");
					selectQuery = selectQuery.replace("$>", "<=");
					selectQuery = selectQuery.replace("${1}", dateCheck);
					System.out.println("===selectQuery:"+selectQuery);
					//selectQuery = selectQuery+" WHERE "+dateParam+" <= '"+dateCheck+"'";
				}
				if(selectQuery.contains("${}")){
					 new SimpleDateFormat ("yyyy");
					selectQuery = selectQuery.replace("${}", ft.format(dNow));
				}
			}
			
			logOperation = 1;
		}
		eventContext.getMessage().setInvocationProperty("selectQuery", selectQuery);
		eventContext.getMessage().setInvocationProperty("dateCheck", dateCheck);
		eventContext.getMessage().setInvocationProperty("logOperation", logOperation);
		return eventContext.getMessage();
		}

}
