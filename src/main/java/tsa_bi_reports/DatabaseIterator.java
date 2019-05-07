package tsa_bi_reports;

import java.util.Map;
import org.mule.module.db.internal.result.resultset.ResultSetIterator;
public class DatabaseIterator {
	 ResultSetIterator rs = null;

	    public ResultSetIterator getResultSet() {
	          return rs;
	    }

	    public void setResultSet (ResultSetIterator rs) {
	          this.rs = rs;
	    }
	    public Map<String,Object> nextRecord(){
	          return rs.next();
	    }

}
