package Hadoop.ETLLOG;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @ClassName LogBean
 * @MethodDesc: TODO LogBean功能介绍
 * @Author Movle
 * @Date 5/6/20 4:26 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class LogBean {
    //客户端IP
    private String remote_addr;

    /**用户名称 忽略 <->*/
    private String remote_user;

    //时间
    private String time_local;

    //URL请求
    private String request;

    //返回状态码
    private String status;

    //文件内容大小
    private String body_btyes_sent;

    //链接页面
    private String http_referer;

    //浏览的相关信息
    private String http_user_agent;

    //判断是否合法
    private boolean vaild =true;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.vaild);
        sb.append("\001").append(this.remote_addr);
        sb.append("\001").append(this.remote_user);
        sb.append("\001").append(this.time_local);
        sb.append("\001").append(this.request);
        sb.append("\001").append(this.status);
        sb.append("\001").append(this.body_btyes_sent);
        sb.append("\001").append(this.http_referer);
        sb.append("\001").append(this.http_user_agent);

        return sb.toString();

    }
}
