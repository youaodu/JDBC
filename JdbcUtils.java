package yoao.tu.utils;
 
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
/**
 * @author youao.du@gmail.com
 * @create 2019-06-02 22:15
 */
public class JdbcUtils implements Serializable {
    
    /* logger */
    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);
 
    /**
     * 驱动类路径
     */
    private static String driver = "com.mysql.cj.jdbc.Driver";
 
    /**
     *  链接字符串
     */
    private static String url = "jdbc:mysql://localhost:3306/dee";
    /**
     * 用户名
     */
    private static String user = "root";
    /**
     * 密码
     */
    private static String pass = "123123123";
    /**
     * 数据库链接对象   全局统一
     */
    private static volatile Connection conn = null;
    /**
     * 初始化Connection 对象
     * @author youao.du@gmail.com
     * @time 22:26
     * @params
     */
    static {
        if (conn == null) {
            try {
                Class.forName(driver);
                conn = DriverManager.getConnection(url, user, pass);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 查询列表
     * @author youao.du@gmail.com
     * @time 23:04
     * @params [sql, className, params]
     */
    public static <T> List<T> selectList(String sql, Class<T> className, Object... params) {
        // 非空判断
        if (StringUtils.isBlank(sql) || className == null) {
            return null;
        }
        printBasicLogger(sql, params);
        PreparedStatement prepared = prepared(sql, params);
        List<T> result = null;
        try {
            ResultSet resultSet = prepared.executeQuery();
            result = new ArrayList<>();
            while (resultSet.next()) {
                // 判断是不是基本数据类型
                if (!isValType(className)) {
                    T t = className.newInstance();
                    // 获取所有属性
                    Field[] fields = className.getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                        field.set(t, resultSet.getObject(humpToUnderline(field.getName())));
                    }
                    result.add(t);
                }else {
                    T t = (T) resultSet.getObject(0);
                    result.add(t);
                }
            }
        } catch(SQLException e){
            e.printStackTrace();
        } catch(IllegalAccessException e){
            e.printStackTrace();
        } catch(InstantiationException e){
            e.printStackTrace();
        }
        return result;
    }
    /**
     * 查询单个对象
     * @author youao.du@gmail.com
     * @time 09:43
     * @params
     */
    public static <T> T selectOne(String sql, Class<T> className, Object... params) {
        List<T> temp = selectList(sql, className, params);
        return temp == null ? null : temp.get(0);
    }
    /**
     * 添加方法
     * @author youao.du@gmail.com
     * @time 11:27
     * @params
     */
    public static <T> int insert(T t) {
        if (t == null) {
            return 0;
        }
        Class className = t.getClass();
        // 拼接 sql 语句
        StringBuffer sb = new StringBuffer("insert into ");
        // 获取类名转换表名
        sb.append(humpToUnderline(className.getSimpleName()));
        // 获取类所有属性
        Field[] fields = className.getDeclaredFields();
        try {
            sb.append("(");
            List<Object> values = new ArrayList<>();
            for (Field field : fields) {
                field.setAccessible(true);
                Object temp = field.get(t);
                if (temp != null) {
                    if (values.size() == 0) {
                        values.add(temp);
                        sb.append(humpToUnderline(field.getName()));
                    } else {
                        values.add(temp);
                        sb.append(", " + humpToUnderline(field.getName()));
                    }
                }
            }
            sb.append(") values (");
            for (int i = 0; i < values.size(); i++) {
                if (i == 0) {
                    sb.append("?");
                    continue;
                }
                sb.append(", ?");
            }
            sb.append(")");
            printBasicLogger(sb.toString(), values);
            PreparedStatement prepared = prepared(sb.toString(), values.toArray());
            return prepared.executeUpdate();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 修改方法
     * @author youao.du@gmail.com
     * @time 19:47
     * @params
     */
    public static <T> int update(T t, Map<String, Object> map) {
        if (t == null || map == null) {
            return 0;
        }
        Class className = t.getClass();
        // 拼接sql语句
        StringBuffer sb = new StringBuffer("update " + humpToUnderline(className.getSimpleName()) + " set ");
        Field[] fields = className.getDeclaredFields();
        try {
            // 参数集合
            List<Object> values = new ArrayList<>();
            for (Field field : fields) {
                field.setAccessible(true);
                Object temp = field.get(t);
                if (temp == null)
                    continue;
                // 拼接sql
                if (values.size() == 0) {
                    sb.append(field.getName() + " = ? ");
                    // 参数集合赋值
                    values.add(temp);
                } else {
                    sb.append(", " + field.getName() + " = ? ");
                    // 参数集合赋值
                    values.add(temp);
                }
            }
            sb.append(" where ");
            List<Object> whereValues = new ArrayList<>();
            for (String key : map.keySet()) {
                if (whereValues.size() == 0) {
                    sb.append(humpToUnderline(key) + " = ?");
                    whereValues.add(map.get(key));
                    continue;
                }
                sb.append(" and " + humpToUnderline(key) + " = ?");
                whereValues.add(map.get(key));
            }
            values.addAll(whereValues);
            printBasicLogger(sb.toString(), values);
            PreparedStatement prepared = prepared(sb.toString(), values.toArray());
            return prepared.executeUpdate();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 删除方法
     * @author youao.du@gmail.com
     * @time 20:33
     * @params
     */
    public static <T> int delete(String tableName, Map<String, Object> map) {
        if (StringUtils.isBlank(tableName) || map == null || map.keySet().size() == 0) {
            return 0;
        }
        StringBuffer sb = new StringBuffer("delete from " + tableName + " where ");
        List<Object> values = new ArrayList<>();
        for (String key : map.keySet()) {
            if (values.size() == 0) {
                sb.append(humpToUnderline(key) + " = ? ");
                values.add(map.get(key));
                continue;
            }
            sb.append(" and " + humpToUnderline(key) + " = ? ");
            values.add(map.get(key));
        }
        printBasicLogger(sb.toString(), values);
        PreparedStatement prepared = prepared(sb.toString(), values.toArray());
        try {
            return prepared.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 删除 主动提供sql
     * @author youao.du@gmail.com
     * @time 21:03
     * @params
     */
    public static int delete(String sql, Object... params) {
        PreparedStatement prepared = prepared(sql, params);
        try {
            return prepared.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
    private static void printBasicLogger(String sql, Object obj) {
        logger.info("执行sql： {}", sql);
        logger.info("sql动态参数: {}", JSONObject.toJSONString(obj));
    }
    /**
     * 执行参数
     * @author youao.du@gmail.com
     * @time 23:08
     * @params
     */
    private static PreparedStatement prepared(String sql, Object... params) {
        // 最后返回值
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }
    /**
     * 驼峰转换下划线
     * @author youao.du@gmail.com
     * @time 23:01
     * @params
     */
    private static String humpToUnderline(String humpString) {
        if(StringUtils.isBlank(humpString)) return "";
        String regexStr = "[A-Z]";
        Matcher matcher = Pattern.compile(regexStr).matcher(humpString);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String g = matcher.group();
            matcher.appendReplacement(sb, "_" + g.toLowerCase());
        }
        matcher.appendTail(sb);
        if (sb.charAt(0) == '_') {
            sb.delete(0, 1);
        }
        return sb.toString();
    }
    /**
     * 判断是不是基本数据类型包装类
     * @author youao.du@gmail.com
     * @time 22:47
     * @params
     */
    private static boolean isValType(Class className) {
        if (className == null) {
            return false;
        } else if(className.equals(String.class)) {
            return true;
        } else if(className.equals(Integer.class)) {
            return true;
        } else if(className.equals(Long.class)) {
            return true;
        } else if(className.equals(Short.class)) {
            return true;
        } else if(className.equals(Double.class)) {
            return true;
        } else if(className.equals(Float.class)) {
            return true;
        } else if(className.equals(Character.class)) {
            return true;
        } else if(className.equals(Byte.class)) {
            return true;
        }
        return false;
    }
    public static void main(String[] args) {
        // 查找集合
        List<Test> tests = selectList("select * from test", Test.class);
        // 查询单个
        Test test = selectOne("select * from test where name = zs", Test.class);
        // 添加
        int insertResult = insert(test);
        // 修改
        Map<String, Object> updateParams = new HashMap<>();
        updateParams.put("name", "zs");
        int updateResult = update(test, updateParams);
        // 根据条件删除
        Map<String, Object> deleteParams = new HashMap<>();
        deleteParams.put("name", "zs");
        int deleteResult1 = delete("test", deleteParams);
        // 根据sql删除
        int deleteResult = delete("delete from test where name = ?", "zs");
    }
}
@Data
class Test{
    private String name;
    private String age;
}
