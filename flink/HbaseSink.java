import com.alibaba.fastjson.JSON;

import *.dao.OrderLogDao;
import *.dao.mybatis.MybatisSessionFactory;
import *.entity.Order;
import *.entity.OrderLog;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

public class MysqlSink extends RichSinkFunction<List<Order>> {

    private static Logger logger = LoggerFactory.getLogger(MysqlSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public MysqlSink() {
        super();

    }

    @Override
    public void invoke(List<Order> orders, Context context){
        List<OrderLog> orderLogs = new ArrayList<OrderLog>();

        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        try{
            //插入
            logger.info("MysqlSinkFunction start to do insert data...");

            OrderLogDao orderLogDao = sqlSession.getMapper(OrderLogDao.class);

            orderLogs = JSON.parseArray(JSON.toJSONString(orders), OrderLog.class);

            orderLogDao.insert(orderLogs);

            sqlSession.commit();
            logger.info("MysqlSinkFunction commit transaction success...");
        }
        catch (Throwable e){
            sqlSession.rollback();
            logger.error("MysqlSinkFunction cause Exception,sqlSession transaction rollback...",e);
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}