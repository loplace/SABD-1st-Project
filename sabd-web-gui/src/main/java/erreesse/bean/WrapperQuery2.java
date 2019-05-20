package erreesse.bean;

import lombok.Data;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import java.util.List;

@Data
@ManagedBean
@RequestScoped
public
class WrapperQuery2 {

    public String dataType;

    public List<Query2ResultBean> dataBeans;
}