package erreesse.backingbean;

import com.google.protobuf.ServiceException;
import erreesse.bean.WrapperQuery2;
import erreesse.hbase.client.Query2HBaseClient;
import erreesse.hbase.tabledescriptor.Query2TableDescriptor;
import lombok.Getter;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import java.io.IOException;

@ManagedBean(name = "Query2BackingBean")
@RequestScoped
public class Query2BackingBean {


    @Getter
    private WrapperQuery2 resultBeansTemperature;
    @Getter
    private WrapperQuery2 resultBeansHumidity;
    @Getter
    private WrapperQuery2 resultBeansPressure;

    @Getter
    private String errorMessage;

    private Query2HBaseClient hbaseClient;

    public Query2BackingBean() {
        hbaseClient = new Query2HBaseClient();
        resultBeansTemperature = new WrapperQuery2();
        resultBeansHumidity = new WrapperQuery2();
        resultBeansPressure = new WrapperQuery2();
        initResults();
    }

    private void initResults() {
        try {
            resultBeansTemperature.setDataType("Temperature");
            resultBeansHumidity.setDataType("Humidity");
            resultBeansPressure.setDataType("Pressure");

            resultBeansTemperature.setDataBeans(hbaseClient.getResults(Query2TableDescriptor.TEMPERATURE_COLUMN_FAMILY,null));
            resultBeansHumidity.setDataBeans(hbaseClient.getResults(Query2TableDescriptor.HUMIDITY_COLUMN_FAMILY,null));
            resultBeansPressure.setDataBeans(hbaseClient.getResults(Query2TableDescriptor.PRESSURE_COLUMN_FAMILY,null));

        } catch (IOException e) {
            e.printStackTrace();
            errorMessage = e.getMessage();
        } catch (ServiceException e) {
            e.printStackTrace();
            errorMessage = e.getMessage();
        }
    }
}
