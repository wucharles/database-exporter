支持特性：
	(0).支持下面的数据库直接相互数据导出
		ORACLE、MySQL
	(1).源数据库/目标数据库没有的表不会进行导出
	(2).源数据库多余的列（目标数据库没有的列）不会进行导出
	(3).目标数据库新增的列（源数据库没有的列），根据目标的创建特性进行如下处理
		A.如果可以为null，则设置为null;
		B.如果不能为null，且有默认值，则会自动设置为默认值;
		C.如果不能为null，且没有默认值，则在导出预处理阶段进行提示并退出导出操作;
	(4).需要导出的表，列名称必须相同，但是类型不需要完全相同，但必须是兼容类型
	(5).每个表导出完毕需要在目标数据库执行的sql
			POST_SQL=update  table_name  set  create_time = update_time;
	(6).所有表的排序规则（分页需要）默认是“ORDER BY ID”，可以定制
			DEFAULT_WHERE=ORDER BY ORDER_CODE
	(7).如果某个表的排序规则不一致，可以进行特殊定制(表名称可以使用通配符、正则表达式)
			WHERE_table_name=ORDER BY PRODUCT_ID,MERCHANT_ID
			
			备注：table_name大小写无关；通配符支持，例如WHERE_table_name_%；
	(8).导入数据前，清理目标数据库中的数据，默认是“不清理”，可以通过下面的方法开启
			A.config/commons.properties配置：CLEAR_DATA=true
			B.启动命令行传入参数：java  org.db.export.Exporter  true
			
			备注：命令行参数覆盖配置文件中的设置.
	(9).