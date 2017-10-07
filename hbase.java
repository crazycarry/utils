package com.touna.dao.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.touna.bean.HBasePageModel;
import com.touna.bean.HbaseCreateCF;
import com.touna.dao.HbaseDao;

/**
 * 
 *                       
 * @Filename HbaseDaoImpl.java
 *
 * @Description hbase封装
 *
 * @Version 1.0
 *
 * @Author Lijie
 *
 * @Email lijiewj39069@touna.cn
 *       
 * @History
 *<li>Author: Lijie</li>
 *<li>Date: 2017年3月1日</li>
 *<li>Version: 1.0</li>
 *<li>Content: create</li>
 *
 */
public class HbaseDaoImpl implements HbaseDao {
	
	/**
	 * 日志
	 */
	private static final Logger		logger		= LoggerFactory.getLogger(HbaseDaoImpl.class);
	
	private static final String		CF			= "cf";
	
	/**
	 * hbase操作对象
	 */
	private static Admin			hBaseAdmin;
	
	/**
	 * 配置
	 */
	private static Configuration	conf		= null;
	
	/**
	 * hbase连接
	 */
	private static Connection		connection	= null;
	
	/**
	 * 初始化hbaseAdmin
	 */
	static {
		Properties pro = new Properties();
		String path = System.getProperty("user.dir") + "/conf/resource/hbase.properties";
		InputStream fis = null;
		try {
			fis = new FileInputStream(path);
		} catch (Exception e) {
			fis = HbaseDaoImpl.class.getResourceAsStream("/resource/hbase.properties");
		}
		try {
			pro.load(fis);
			String HBASE_ZK_CONNECT = pro.getProperty("hbase.zookeeper.quorum");
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.set("hbase.zookeeper.quorum", HBASE_ZK_CONNECT);
			connection = ConnectionFactory.createConnection(conf);
			hBaseAdmin = connection.getAdmin();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private HbaseDaoImpl() {
	}
	
	private static final HbaseDaoImpl	hbaseDao	= new HbaseDaoImpl();
	
	public static HbaseDaoImpl getInstance() {
		return hbaseDao;
	}
	
	/**
	 * 删除表,会直接删除存在的表
	 * @param tn
	 * @param isDelete
	 * @throws Exception
	 */
	public void deleteTable(String tn) throws Exception {
		TableName tableName = TableName.valueOf(Bytes.toBytes(tn));
		deleteTable(tableName, true);
	}
	
	/**
	 * 清空表
	 * @param tn
	 * @param isDelete
	 * @throws Exception
	 */
	public void truncateTable(String tn) throws Exception {
		TableName tableName = TableName.valueOf(Bytes.toBytes(tn));
		try {
			// 判断表是否存在
			if (hBaseAdmin.tableExists(tableName)) {
				
				hBaseAdmin.disableTable(tableName);
				
				// 清空表
				hBaseAdmin.truncateTable(tableName, false);
				
				logger.info("表名为:{}执行了truncate操作！", tableName.getNameAsString());
			} else {
				logger.info("不存在表名为:{}！", tableName.getNameAsString());
			}
		} catch (Exception e) {
			logger.info("清空表:{}！出现异常", e.getStackTrace());
		} finally {
			closeConnect(hBaseAdmin);
		}
		
	}
	
	/**
	 * 指定各个cf的压缩格式，region自定义，表存在是否删除，设置ttl
	 * @param tableName
	 * @param hcc
	 * @param splits
	 * @param isDelete
	 * @param timeToLive 小于1为不限制
	 * @throws Exception
	 */
	public void createTable(String tableName, HbaseCreateCF hcc, byte[][] splits, boolean isDelete,
							int timeToLive) throws Exception {
		
		// 封装表对象
		TableName tn = TableName.valueOf(tableName);
		try {
			deleteTable(tn, isDelete);
			
			// 创建表描述对象
			HTableDescriptor htd = new HTableDescriptor(tn);
			
			// 获取list
			Map<String, Algorithm> mapCf = hcc.getMap();
			
			// 遍历并且往表描述对象添加列族描述对象
			for (Map.Entry<String, Algorithm> map : mapCf.entrySet()) {
				
				// 取出列族
				String columnFamily = map.getKey();
				
				// 取出压缩格式
				Algorithm algorithm = map.getValue();
				
				// 创建列族描述对象
				HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
				
				//设置过期时间
				if (timeToLive > 1) {
					hcd.setTimeToLive(timeToLive);
				}
				
				// 压缩是否为空
				if (null != algorithm) {
					
					// 添加列族并且设置压缩
					htd.addFamily(hcd.setCompressionType(algorithm).setCompactionCompressionType(
						algorithm));
				} else {
					
					// 添加列族不压缩
					htd.addFamily(hcd);
				}
				
			}
			
			// 是否预分区
			if (splits == null) {
				
				// 默认一个region
				hBaseAdmin.createTable(htd);
			} else {
				
				//
				hBaseAdmin.createTable(htd, splits);
			}
		} finally {
			
			closeConnect(hBaseAdmin);
		}
		
	}
	
	/**
	 * 默认snappy压缩建表 数据永久有效，region自定义
	 * @param tableName
	 * @param cfs 列簇
	 * @param splits 自定义region
	 * @param isDelete
	 * @throws Exception
	 */
	public void createTableBySNAPPY(String tableName, String[] cfs, byte[][] splits,
									boolean isDelete) throws Exception {
		createTableBySNAPPY(tableName, cfs, splits, isDelete, -1);
	}
	
	/**
	 * 默认snappy压缩建表 数据永久有效，region平均分配
	 * @param tableName
	 * @param cfs
	 * @param startRow
	 * @param endRow
	 * @param numRegions
	 * @param isDelete
	 * @throws Exception
	 */
	public void createTableBySNAPPY(String tableName, String[] cfs, byte[] startRow, byte[] endRow,
									int numRegions, boolean isDelete) throws Exception {
		createTableBySNAPPY(tableName, cfs, startRow, endRow, numRegions, isDelete, -1);
	}
	
	/**
	 * 默认snappy压缩建表 数据需要设置ttl，region自定义
	 * @param tableName
	 * @param cfs
	 * @param splits
	 * @param isDelete
	 * @param timeToLive 小于1为不限制
	 * @throws Exception
	 */
	public void createTableBySNAPPY(String tableName, String[] cfs, byte[][] splits,
									boolean isDelete, int timeToLive) throws Exception {
		
		// 封装表对象
		TableName tn = TableName.valueOf(tableName);
		try {
			deleteTable(tn, isDelete);
			
			// 创建表描述对象
			HTableDescriptor htd = new HTableDescriptor(tn);
			
			for (String cf : cfs) {
				HColumnDescriptor hcd = new HColumnDescriptor(cf);
				
				//设置过期时间
				if (timeToLive > 1) {
					hcd.setTimeToLive(timeToLive);
				}
				htd.addFamily(hcd.setCompressionType(Algorithm.SNAPPY)
					.setCompactionCompressionType(Algorithm.SNAPPY));
			}
			
			// 是否预分区
			if (splits == null) {
				
				// 默认一个region
				hBaseAdmin.createTable(htd);
			} else {
				
				//
				hBaseAdmin.createTable(htd, splits);
			}
		} finally {
			
			closeConnect(hBaseAdmin);
		}
		
	}
	
	/**
	 * 默认snappy压缩建表 数据永久有效，region平均分配
	 * @param tableName
	 * @param cfs
	 * @param splits
	 * @param isDelete
	 * @param timeToLive 小于1为不限制
	 * @throws Exception
	 */
	public void createTableBySNAPPY(String tableName, String[] cfs, byte[] startRow, byte[] endRow,
									int numRegions, boolean isDelete, int timeToLive)
																						throws Exception {
		
		// 封装表对象
		TableName tn = TableName.valueOf(tableName);
		try {
			deleteTable(tn, isDelete);
			// 创建表描述对象
			HTableDescriptor htd = new HTableDescriptor(tn);
			
			for (String cf : cfs) {
				HColumnDescriptor hcd = new HColumnDescriptor(cf);
				
				//设置过期时间
				if (timeToLive > 1) {
					hcd.setTimeToLive(timeToLive);
				}
				htd.addFamily(hcd.setCompressionType(Algorithm.SNAPPY)
					.setCompactionCompressionType(Algorithm.SNAPPY));
			}
			
			hBaseAdmin.createTable(htd, startRow, endRow, numRegions);
		} finally {
			
			closeConnect(hBaseAdmin);
		}
		
	}
	
	/**
	 * 异步批量写入数据
	 * 
	 * @param tableName
	 * @param puts
	 * @throws Exception
	 */
	public void addDataBatchAsyn(String tableName, List<Put> puts) throws Exception {
		long currentTime = System.currentTimeMillis();
		
		// 创建监听器
		final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
				for (int i = 0; i < e.getNumExceptions(); i++) {
					logger.error("写入失败： " + e.getRow(i) + "！");
				}
			}
		};
		
		// 设置表的缓存参数
		BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
			.listener(listener);
		params.writeBufferSize(5 * 1024 * 1024);
		
		// 操作类获取
		final BufferedMutator mutator = connection.getBufferedMutator(params);
		try {
			
			// 添加
			mutator.mutate(puts);
			
			// 提交
			mutator.flush();
		} finally {
			
			// 关闭连接
			closeConnect(mutator);
		}
		logger.info("addDataBatchAsyn执行时间:{}", (System.currentTimeMillis() - currentTime));
	}
	
	/**
	 * 同步批量添加数据
	 * 
	 * @param tablename
	 * @param puts
	 * @return
	 * @throws Exception
	 */
	public void addDataBatch(String tablename, List<Put> puts) throws Exception {
		long currentTime = System.currentTimeMillis();
		
		// 获取htable操作对象
		HTable htable = (HTable) connection.getTable(TableName.valueOf(tablename));
		// 这里设置了false,setWriteBufferSize方法才能生效
		htable.setAutoFlushTo(false);
		htable.setWriteBufferSize(5 * 1024 * 1024);
		try {
			
			// 添加
			htable.put(puts);
			
			// 执行
			htable.flushCommits();
		} catch (IOException e) {
			htable.flushCommits();
			logger.error("addDataBatch 插入数据失败：exception ", e.getMessage());
			throw new RuntimeException("addDataBatch hbase 插入失败！" + e.getMessage());
		} finally {
			// 关闭连接
			closeConnect(htable);
		}
		
		logger.info("addDataBatch执行时间:{}", (System.currentTimeMillis() - currentTime));
	}
	
	/**
	 * 同步添加数据
	 * 
	 * @param tablename
	 * @param puts
	 * @return
	 * @throws Exception
	 */
	public void addData(String tablename, Put put) throws Exception {
		long currentTime = System.currentTimeMillis();
		
		// 获取htable操作对象
		HTable htable = (HTable) connection.getTable(TableName.valueOf(tablename));
		
		try {
			htable.put(put);
		} catch (Exception e) {
			logger.error("addData 插入数据失败：exception ", e.getMessage());
			//			insertExp(put, tablename, e.getMessage());
			throw new RuntimeException("addData hbase 插入失败！" + e.getMessage());
		} finally {
			
			// 关闭连接
			closeConnect(htable);
		}
		
		logger.info("addData执行时间:{}", (System.currentTimeMillis() - currentTime));
	}
	
	/**
	 * 删除单条数据
	 * 
	 * @param tablename
	 * @param row
	 * @throws IOException
	 */
	public void delete(String tablename, String row) throws Exception {
		// 获取htable操作对象
		Table table = connection.getTable(TableName.valueOf(tablename));
		
		if (table != null) {
			try {
				
				// 创建删除对象
				Delete d = new Delete(row.getBytes());
				
				// 执行删除操作
				table.delete(d);
			} catch (IOException e) {
				logger.error("delete单条数据 删除数据失败：exception ", e.getMessage());
				throw new RuntimeException("delete单条数据 删除数据失败!" + e.getMessage());
				
			} finally {
				
				// 关闭连接
				closeConnect(table);
			}
		}
	}
	
	/**
	 * 删除多行数据
	 * 
	 * @param tablename
	 * @param rows
	 * @throws IOException
	 */
	public void delete(String tablename, List<String> rows) throws Exception {
		
		// 获取htable操作对象
		Table table = connection.getTable(TableName.valueOf(tablename));
		if (table != null) {
			try {
				
				// 存储删除对象List
				List<Delete> list = new ArrayList<Delete>();
				for (String row : rows) {
					
					// 创建删除对象
					Delete d = new Delete(row.getBytes());
					
					// 添加到删除对象List中
					list.add(d);
				}
				if (list.size() > 0) {
					
					// 执行删除操作
					table.delete(list);
				}
			} catch (IOException e) {
				logger.error("delete多条数据  删除数据失败：exception ", e.getMessage());
				throw new RuntimeException("delete多条数据  hbase 删除数据失败！" + e.getMessage());
			} finally {
				
				// 关闭连接
				closeConnect(table);
			}
		}
	}
	
	/**
	 * 获取单条数据,根据rowkey
	 * @param tablename
	 * @param row
	 * @param columns
	 * @return
	 * @throws Exception
	 * @see com.touna.dao.HbaseDao#getRow(java.lang.String, java.lang.String, java.util.List)
	 */
	public Result getRow(String tablename, String row, List<String> columns) throws Exception {
		
		// 获取htable操作对象
		Table table = connection.getTable(TableName.valueOf(tablename));
		Result rs = null;
		if (table != null) {
			try {
				
				// 创建查询对象
				Get g = new Get(Bytes.toBytes(row));
				if (null != columns && columns.size() != 0) {
					for (String col : columns) {
						g.addColumn(Bytes.toBytes(CF), Bytes.toBytes(col));
					}
				}
				// 查询获得结果
				rs = table.get(g);
			} catch (IOException e) {
				logger.error("获取失败！", e);
			} finally {
				// 关闭连接
				closeConnect(table);
			}
		}
		return rs;
	}
	
	/**
	 * 获取多行数据
	 * @param tablename
	 * @param rows
	 * @param columns
	 * @return
	 * @throws Exception
	 * @see com.touna.dao.HbaseDao#getRows(java.lang.String, java.util.List, java.util.List)
	 */
	public Result[] getRows(String tablename, List<String> rows, List<String> columns)
																						throws Exception {
		// 获取htable操作对象
		Table table = connection.getTable(TableName.valueOf(tablename));
		
		List<Get> gets = null;
		Result[] results = null;
		
		try {
			if (table != null) {
				
				// 创建查询操作的List
				gets = new ArrayList<Get>();
				for (String row : rows) {
					if (row != null) {
						Get get = new Get(Bytes.toBytes(row));
						if (null != columns && columns.size() != 0) {
							for (String col : columns) {
								get.addColumn(Bytes.toBytes(CF), Bytes.toBytes(col));
							}
						}
						// 封装到查询操作list中
						gets.add(get);
					} else {
						throw new RuntimeException("rows 没有数据！");
					}
				}
			}
			if (null != gets && gets.size() > 0) {
				
				// 查询数据
				results = table.get(gets);
			}
		} catch (IOException e) {
			logger.error("获取数据失败！", e);
		} finally {
			
			// 关闭连接
			closeConnect(table);
		}
		return results;
	}
	
	/**
	 * 根据范围扫描rowkey
	 * @param tablename
	 * @param start
	 * @param stop
	 * @return
	 * @throws Exception
	 */
	public List<Result> getRowsByStartAndStop(String tablename, String start, String stop)
																							throws Exception {
		Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tablename)));
		
		List<Result> res = new ArrayList<Result>();
		
		Scan scan = new Scan();
		
		if (null != start) {
			scan.setStartRow(start.getBytes());
		}
		
		if (null != stop) {
			scan.setStopRow(stop.getBytes());
		}
		
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			res.add(result);
		}
		
		return res;
	}
	
	/**
	 * filter 查询
	 * @param tablename
	 * @param start
	 * @param stop
	 * @param scvfs
	 * @param rfs
	 * @return
	 * @throws Exception
	 */
	public List<Result> getRowsByFilters(String tablename, String start,String stop,
											List<SingleColumnValueFilter> scvfs,
											List<RowFilter> rfs, FilterList.Operator operator)
																								throws Exception {
		Table table = connection.getTable(TableName.valueOf(tablename));
		
		Scan scan = new Scan();
		
		List<Result> res = new ArrayList<Result>();
		
		if (operator == null) {
			return null;
		}
		
		if (null != start) {
			scan.setStartRow(start.getBytes());
		}
		
		if (null != stop) {
			scan.setStopRow(stop.getBytes());
		}
		
		FilterList fl = new FilterList(operator);
		
		if (scvfs != null && scvfs.size() != 0) {
			for (SingleColumnValueFilter rowFilter : scvfs) {
				fl.addFilter(rowFilter);
			}
		}
		
		if (rfs != null && rfs.size() != 0) {
			for (RowFilter rowFilter : rfs) {
				fl.addFilter(rowFilter);
			}
		}
		
		scan.setFilter(fl);
		
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			res.add(result);
		}
		
		return res;
	}
	
	/**
	 *  分页 原理：页码， 每页数量
	 *  首先查询出当前查询页的第一行的rowkey，根据pagefilter设置(页码-1)*每页数量+1，取出查询的值的最后一条的rowkey
	 *  利用该页的第一行rowkey就能查询出该页的信息
	 * @param pageModel
	 * @return
	 * @throws IOException
	 */
	public HBasePageModel scanResultByPageModelAnyPage(HBasePageModel pageModel) throws Exception {
		
		if (pageModel == null) {
			return null;
		}
		
		List<Result> list = null;
		
		Scan scan = null;
		
		Table table = null;
		
		ResultScanner scanner = null;
		
		FilterList filterList = null;
		
		PageFilter pf = null;
		
		try {
			//找出跳转页的 第一个rowkey
			String selectPageLastRow = selectPageLastRow(pageModel.getTableName(),
				pageModel.getFilterList(), pageModel.getIndexFirstRow());
			
			if (null == selectPageLastRow) {
				return null;
			}
			
			table = connection.getTable(TableName.valueOf(pageModel.getTableName()));
			scan = new Scan();
			
			//设置rowkey扫描
			if (null != pageModel.getStartRowKey()) {
				scan.setStartRow(Bytes.toBytes(pageModel.getStartRowKey()));
			}
			if (null != pageModel.getEndRowKey()) {
				scan.setStopRow(Bytes.toBytes(pageModel.getEndRowKey()));
			}
			
			//分页filter
			pf = new PageFilter(pageModel.getPageSize());
			
			//判断是否为空
			if (null == pageModel.getFilterList()) {
				scan.setFilter(pf);
			} else {
				filterList = pageModel.getFilterList();
				filterList.addFilter(pf);
				scan.setFilter(filterList);
			}
			
			//设置第一条记录
			scan.setStartRow(Bytes.toBytes(selectPageLastRow));
			
			scanner = table.getScanner(scan);
			
			list = new ArrayList<Result>();
			
			for (Result result : scanner) {
				list.add(result);
			}
			pageModel.setData(list);
		} catch (Exception e) {
			throw e;
		} finally {
			closeConnect(table);
			closeConnect(scanner);
		}
		
		return pageModel;
	}
	
	/**
	 * 分页 （使用）
	 * @param pageModel
	 * @return
	 * @throws IOException
	 */
	
	public HBasePageModel scanResultByPageModel(HBasePageModel pageModel) throws IOException {
		if (pageModel == null) {
			return null;
		}
		
		List<Result> list = null;
		
		Table table = null;
		
		Result selectFirstResultRow = null;
		
		String thisPageRowKey = null;
		
		Scan scan = null;
		
		ResultScanner scanner = null;
		
		PageFilter pf = null;
		
		try {
			table = connection.getTable(TableName.valueOf(pageModel.getTableName()));
			
			if (null == pageModel.getDownPageRowKey()) {
				selectFirstResultRow = selectFirstResultRow(pageModel.getTableName(),
					pageModel.getFilterList());
				if (null == selectFirstResultRow) {
					return pageModel;
				}
				thisPageRowKey = Bytes.toString(selectFirstResultRow.getRow());
				pageModel.setThisPageRowKey(thisPageRowKey);
			}
			
			scan = new Scan();
			
			//设置rowkey扫描
			if (null != pageModel.getStartRowKey()) {
				scan.setStartRow(Bytes.toBytes(pageModel.getStartRowKey()));
			}
			if (null != pageModel.getEndRowKey()) {
				scan.setStopRow(Bytes.toBytes(pageModel.getEndRowKey()));
			}
			
			//判断查询的列
			if (null != pageModel.getColumns() && pageModel.getColumns().size() != 0) {
				for (String col : pageModel.getColumns()) {
					scan.addColumn(Bytes.toBytes(CF), Bytes.toBytes(col));
				}
			}
			
			//创建分页filter
			pf = new PageFilter(pageModel.getPageSizePlusOne());
			
			//判断是否有listfilter
			if (null == pageModel.getFilterList()) {
				scan.setFilter(pf);
			} else {
				FilterList filterList = pageModel.getFilterList();
				filterList.addFilter(pf);
				scan.setFilter(filterList);
			}
			
			//设置startRow起始值,这个相当于第一条值
			if (null == thisPageRowKey) {
				scan.setStartRow(Bytes.toBytes(pageModel.getDownPageRowKey()));
			}
			//滞空
			pageModel.setDownPageRowKey(null);
			int flag = 0;
			list = new ArrayList<Result>();
			scanner = table.getScanner(scan);
			if (null == scanner) {
				return pageModel;
			}
			for (Result result : scanner) {
				if (flag < pageModel.getPageSize()) {
					list.add(result);
				}
				if (flag == 0) {
					pageModel.setThisPageRowKey(Bytes.toString(result.getRow()));
				}
				flag++;
				if (flag == pageModel.getPageSizePlusOne()) {
					pageModel.setDownPageRowKey(Bytes.toString(result.getRow()));
				}
			}
			
			pageModel.setIndex(pageModel.getIndex() + 1);
			pageModel.setData(list);
		} catch (Exception e) {
			throw e;
		} finally {
			closeConnect(scanner);
			closeConnect(table);
		}
		
		return pageModel;
	}
	
	/**
	 * 检索指定表的第一行记录
	 * @param tableName
	 * @param filterList
	 * @return
	 */
	public Result selectFirstResultRow(String tableName, FilterList filterList) {
		if (StringUtils.isBlank(tableName)) {
			
			logger.info("查询的表名为空");
			return null;
		}
		Table table = null;
		ResultScanner scanner = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			if (filterList != null) {
				scan.setFilter(filterList);
			}
			scanner = table.getScanner(scan);
			return scanner.iterator().next();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeConnect(scanner);
			closeConnect(table);
		}
		return null;
	}
	
	/**
	 * 分页byANnyPage 根据页面行数和页数查询出下一页的第一个rowkey
	 * @param tableName
	 * @param filterList
	 * @param page 比如5页 一页10行，那么page就是(5-1)*10+1
	 * @return
	 * @throws IOException
	 */
	public String selectPageLastRow(String tableName, FilterList filterList, int page)
																						throws IOException {
		
		if (StringUtils.isBlank(tableName)) {
			return null;
		}
		
		Table table = null;
		
		ResultScanner scanner = null;
		
		PageFilter pageFilter = null;
		
		Scan scan = null;
		
		String lastRowKey = null;
		
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			pageFilter = new PageFilter(page);
			scan = new Scan();
			if (null != filterList) {
				
				filterList.addFilter(pageFilter);
				scan.setFilter(filterList);
				
			} else {
				scan.setFilter(pageFilter);
			}
			
			scanner = table.getScanner(scan);
			
			int flag = 0;
			
			for (Result result : scanner) {
				flag++;
				if (flag == page) {
					lastRowKey = Bytes.toString(result.getRow());
				}
			}
			return lastRowKey;
		} catch (Exception e) {
			throw e;
		} finally {
			closeConnect(scanner);
			closeConnect(table);
		}
		
	}
	
	/**
	 * 关闭 ResultScanner
	 * @param mutator
	 */
	public static void closeConnect(ResultScanner scanner) {
		closeConnect(scanner, null, null, null, null);
	}
	
	/**
	 * 关闭 BufferedMutator
	 * @param mutator
	 */
	public static void closeConnect(BufferedMutator mutator) {
		closeConnect(null, mutator, null, null, null);
	}
	
	/**
	 * 关闭 admin
	 * @param admin
	 */
	public static void closeConnect(Admin admin) {
		closeConnect(null, null, admin, null, null);
	}
	
	/**
	 * 关闭 htable
	 * @param htable
	 */
	public static void closeConnect(HTable htable) {
		closeConnect(null, null, null, htable, null);
	}
	
	/**
	 * 关闭 table
	 * @param table
	 */
	public static void closeConnect(Table table) {
		closeConnect(null, null, null, null, table);
	}
	
	/**
	 * 关闭连接
	 * 
	 * @param mutator
	 * @param admin
	 * @param htable
	 */
	public static void closeConnect(ResultScanner scanner, BufferedMutator mutator, Admin admin,
									HTable htable, Table table) {
		
		if (null != scanner) {
			try {
				scanner.close();
			} catch (Exception e) {
				logger.error("closeResultScanner failure !", e);
			}
		}
		
		if (null != mutator) {
			try {
				mutator.close();
			} catch (Exception e) {
				logger.error("closeBufferedMutator failure !", e);
			}
		}
		
		if (null != admin) {
			try {
				admin.close();
			} catch (Exception e) {
				logger.error("closeAdmin failure !", e);
			}
		}
		if (null != htable) {
			try {
				htable.close();
			} catch (Exception e) {
				logger.error("closeHtable failure !", e);
			}
		}
		if (null != table) {
			try {
				table.close();
			} catch (Exception e) {
				logger.error("closeTable failure !", e);
			}
		}
	}
	
	/**
	 * 删除表
	 * @param tn
	 * @param isDelete
	 * @throws Exception
	 */
	public void deleteTable(TableName tn, boolean isDelete) throws Exception {
		// 判断表是否存在
		if (hBaseAdmin.tableExists(tn)) {
			if (isDelete) {
				
				// disable表
				hBaseAdmin.disableTable(tn);
				
				// 删除表
				hBaseAdmin.deleteTable(tn);
				
				logger.info("表名为:{}在建表时，被删除！", tn.getNameAsString());
			} else {
				logger.info("表名为:{}已经存在！", tn.getNameAsString());
				throw new Exception("isDelete为false，表名已经存在！");
			}
		}
	}
	
	public void pageFilerTest() throws IOException {
		Table table = connection.getTable(TableName.valueOf("lijie001"));
		Scan scan = new Scan();
		PageFilter pf = new PageFilter(-1);
		
		scan.setStartRow(Bytes.toBytes("80000"));
		scan.setFilter(pf);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			List<Cell> listCells = result.listCells();
			if (null != listCells && listCells.size() != 0) {
				for (Cell cell : listCells) {
					System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)) + "  ---  "
										+ Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
		}
	}
	
	public void createNameSpace(String nameSpace) throws Exception {
		//先查看命名空间是否存在
		boolean isExist = this.isExistNameSpace(nameSpace);
		if (!isExist) { //如果不存在，则创建
			this.hBaseAdmin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
		}
	}
	
	@Override
	public void deleteNameSpace(String nameSpace) throws Exception {
		this.hBaseAdmin.deleteNamespace(nameSpace);
	}
	
	@Override
	public NamespaceDescriptor[] listNameSpace() throws Exception {
		return this.hBaseAdmin.listNamespaceDescriptors();
	}
	
	public static void main(String[] args) throws IOException {
		HbaseDaoImpl instance = getInstance();
		instance.pageFilerTest();
		
	}
	
	@Override
	public boolean isExistNameSpace(String nameSpace) throws Exception {
		boolean isExist = false;
		NamespaceDescriptor[] ndArr = this.listNameSpace();
		if (null != ndArr && ndArr.length > 0) {
			for (NamespaceDescriptor item : ndArr) {
				if (nameSpace.equals(item.getName())) {
					isExist = true;
					break;
				}
			}
		}
		return isExist;
	}
	
	@Override
	public TableName[] listTableNamesByNamespace(String nameSpace) throws Exception {
		return this.hBaseAdmin.listTableNamesByNamespace(nameSpace);
	}

	@Override
	public Result getLastDataByRowkeyCondition(String tableName, byte[] startRowKey,
												byte[] endRowKey, boolean isReverse)
																					throws Exception {
		Result rs = null;
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
			
			Scan scan = new Scan();
			
			if (null != startRowKey) {
				scan.setStartRow(startRowKey);
			}
			
			if (null != endRowKey) {
				scan.setStopRow(endRowKey);
			}
			
			if(isReverse){
				scan.setReversed(true);
				scan.setCaching(1);
			}
			
			ResultScanner scanner = table.getScanner(scan);
			
			if (scanner != null) {
				Iterator<Result> iter = scanner.iterator();
				if (iter.hasNext()){
					rs = iter.next();	
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			// 关闭连接
			closeConnect(table);
		}
		
		return rs;
		
	}
	
}
