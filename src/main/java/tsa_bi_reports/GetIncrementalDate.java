package tsa_bi_reports;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

public class GetIncrementalDate implements Callable{
	public Object onCall(MuleEventContext eventContext) throws Exception {
		String dateCheck = null;
		String selectQuery = eventContext.getMessage().getInvocationProperty("selectQuery");
		int selectCount = Integer.parseInt(eventContext.getMessage().getInvocationProperty("selectCount").toString());
		String year = eventContext.getMessage().getInvocationProperty("year");
		System.gc();
		Date dNow = new Date( );
	    SimpleDateFormat ft = 
	    new SimpleDateFormat ("yyyy-MM-dd");
	    dateCheck = ft.format(dNow);
	    System.out.println("Current Date: " + dateCheck);
		if(selectCount == 1){
			selectQuery = selectQuery.replace("${1}", year+"-01-01");
			selectQuery = selectQuery.replace("${2}", year+"-04-30");
			selectQuery = selectQuery.replace("$>", ">");
			if(selectQuery.contains("${}")){
				selectQuery = selectQuery.replace("${}", year);
			}
			System.out.println("===selectQuery:"+selectQuery);
		}
		if(selectCount == 2){
			selectQuery = selectQuery.replace("${1}", year+"-05-01");
			selectQuery = selectQuery.replace("${2}", year+"-08-31");
			selectQuery = selectQuery.replace("$>", ">");
			if(selectQuery.contains("${}")){
				selectQuery = selectQuery.replace("${}", year);
			}
			System.out.println("===selectQuery:"+selectQuery);		
		}
		if(selectCount == 3){
		    selectQuery = selectQuery.replace("${1}", year+"-09-01");
			selectQuery = selectQuery.replace("${2}", year+"-12-31");
			selectQuery = selectQuery.replace("$>", ">");
			if(selectQuery.contains("${}")){
				selectQuery = selectQuery.replace("${}", year);
			}
			/*if(count == 1){
				selectQuery = selectQuery.replace("${1}", "2017-01-01");
				selectQuery = selectQuery.replace("${2}", "2017-04-30");
				selectQuery = selectQuery.replace("$>", ">");
			}
			if(selectCount == 2){
				selectQuery = selectQuery.replace("${1}", "2017-05-01");
				selectQuery = selectQuery.replace("${2}", "2016-08-31");
				selectQuery = selectQuery.replace("$>", ">");
							}
			if(selectCount == 3){
			    selectQuery = selectQuery.replace("${1}", "2017-09-01");
				selectQuery = selectQuery.replace("${2}", "2017-12-01");
				selectQuery = selectQuery.replace("$>", ">");
			}*/
			System.out.println("===selectQuery:"+selectQuery);
			
		}
		eventContext.getMessage().setInvocationProperty("selectQuery", selectQuery);
		eventContext.getMessage().setInvocationProperty("insertDateCheck", dateCheck);
		eventContext.getMessage().setInvocationProperty("logOperation", 1);
		return eventContext.getMessage();
		
	}

}
