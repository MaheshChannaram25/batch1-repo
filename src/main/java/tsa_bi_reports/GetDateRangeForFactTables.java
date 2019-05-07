package tsa_bi_reports;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

public class GetDateRangeForFactTables implements Callable{
	public Object onCall(MuleEventContext eventContext) throws Exception {
		String selectQuery = eventContext.getMessage().getInvocationProperty("selectQuery");
		String deleteQuery = eventContext.getMessage().getInvocationProperty("getDeleteQuery");
		//String jobRunWeekly = eventContext.getMessage().getInvocationProperty("jobRunWeekly");
		int logOperation = 0;
		Date dNow = new Date( );
	    SimpleDateFormat ft = 
	    new SimpleDateFormat ("yyyy-MM-dd");
	    String dateCheck = ft.format(dNow);
	    System.out.println("Current Date: " + dateCheck);
	    Map<?,?> runOffset = new HashMap<>();
	    runOffset = eventContext.getMessage().getInvocationProperty("runOffset"); 
	    List<?> payload = new ArrayList<Object>();
		payload = (List<?>) eventContext.getMessage().getPayload();
		
		if(payload.size() > 0 ){
			Calendar cal = Calendar.getInstance();
	        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	       // String weeklyJobRunDay = "SATURDAY";
	        if(dayOfWeek == Calendar.SATURDAY){
	        	 cal.add(Calendar.DATE, - Integer.parseInt(runOffset.get("weekly_run_offset").toString()));
	        }else{
	        	cal.add(Calendar.DATE, - Integer.parseInt(runOffset.get("daily_run_offset").toString()));
	        }
	        
	       // cal.add(Calendar.DATE, - Integer.parseInt(runOffset.get("daily_run_offset").toString()));
	        Date todate1 = cal.getTime();    
	        
			Map<?,?> tableCount   = (Map<?,?>) payload.get(0);
			if(Integer.parseInt(tableCount.get("tableCount").toString()) > 0){
				eventContext.getMessage().setInvocationProperty("logOperation", 0);
				eventContext.getMessage().setInvocationProperty("deleteData", 1);
		        String ExtractFromDate = ft.format(todate1);
		        System.out.println("ExtractFromDate:"+ExtractFromDate);
		        if(selectQuery.contains("${1}")){
					selectQuery = selectQuery.replace("${1}", ExtractFromDate);
					selectQuery = selectQuery.replace("${2}", dateCheck);
					selectQuery = selectQuery.replace("$>", ">=");
		        }
		        else{
					selectQuery = selectQuery+"";
				}
		        deleteQuery = deleteQuery.replace("$date", ExtractFromDate);
		        eventContext.getMessage().setInvocationProperty("deleteQuery", deleteQuery); 
			}else{
				eventContext.getMessage().setInvocationProperty("logOperation", 1);
				eventContext.getMessage().setInvocationProperty("deleteData", 0);
				/*selectQuery = selectQuery.replace(selectQuery.substring(selectQuery.lastIndexOf("AND"), selectQuery.lastIndexOf("}'")+2),"");
				selectQuery = selectQuery.replace("$>", "<=");
				selectQuery = selectQuery.replace("${1}", dateCheck);*/
				selectQuery = selectQuery.replace("${1}", "2015-01-01");
				selectQuery = selectQuery.replace("${2}", dateCheck);
				selectQuery = selectQuery.replace("$>", ">=");
			}
		}else{
			eventContext.getMessage().setInvocationProperty("logOperation", 1);
			eventContext.getMessage().setInvocationProperty("deleteData", 0);
			/*selectQuery = selectQuery.replace(selectQuery.substring(selectQuery.lastIndexOf("AND"), selectQuery.lastIndexOf("}'")+2),"");
			selectQuery = selectQuery.replace("$>", "<=");
			selectQuery = selectQuery.replace("${1}", dateCheck);*/
			selectQuery = selectQuery.replace("${1}", "2015-01-01");
			selectQuery = selectQuery.replace("${2}", dateCheck);
			selectQuery = selectQuery.replace("$>", ">=");
		}
		
		eventContext.getMessage().setInvocationProperty("selectQuery", selectQuery);
		eventContext.getMessage().setInvocationProperty("dateCheck", dateCheck);
		eventContext.getMessage().setInvocationProperty("logOperation", logOperation);
		
		return eventContext.getMessage();
		}

}
