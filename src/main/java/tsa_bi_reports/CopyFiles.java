package tsa_bi_reports;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.mule.api.MuleContext;
import org.mule.api.MuleEventContext;
import org.mule.api.client.MuleClient;
import org.mule.api.lifecycle.Callable;

public class CopyFiles implements Callable {

	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		try {
			InputStream  input = null;
			MuleContext muleContext = eventContext.getMuleContext();
			MuleClient muleClient = eventContext.getMuleContext().getClient();
			input = getClass().getResourceAsStream("/mule-app.properties");
			Properties prop = new Properties();
			prop.load(input);
			
			try {
			    File source = new File(prop.getProperty("tsa.file.copy.location"));
				File dest = new File(prop.getProperty("tsa.file.location"));
				FileUtils.copyDirectory(source, dest);
			} catch (IOException e) {
			    e.printStackTrace();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
