package erreesse.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.faces.bean.ManagedBean;

@ManagedBean
@Data
@AllArgsConstructor
public class Query3ResultBean {

    private String city;

    private String absMean2017;

    private String pos2017;

    private String absMean2016;

    private String pos2016;
}
