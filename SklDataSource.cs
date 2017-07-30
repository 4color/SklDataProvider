using System;
using System.Data;
using System.ComponentModel;
using System.Collections.Specialized;
using System.Collections;
using System.Collections.Generic;


namespace SklDataProvider
{
    public enum DBType
    {
        DB_SQL_Server = 1,
        DB_Oracle = 2,
        DB_OraOracle = 3,
        DB_OleDb = 4,
        DB_Odbc = 5,
        DB_MSJet = 6,
    }
    /// <summary>
    /// SklDataSource 的摘要说明。
    /// </summary>
    public abstract class SklDataSource : IDisposable
    {
        protected string m_provider = "";
        protected string m_datasource = "";
        protected string m_catalog = "";
        protected string m_username = "";
        protected string m_password = "";

        public string Provider
        {
            set { this.m_provider = value; }
        }

        public string DataSource
        {
            set { this.m_datasource = value; }
        }

        public string Catalog
        {
            set { this.m_catalog = value; }
        }

        public string UserName
        {
            set { this.m_username = value; }
        }

        public string Password
        {
            set { this.m_password = value; }
        }



        #region Connection

        /// <summary>
        /// Get database connection.
        /// </summary>
        public abstract IDbConnection Connection
        {
            get;
        }

        /// <summary>
        /// Open database conneciton.
        /// </summary>
        public abstract void Open();

        /// <summary>
        /// Close database conneciton.
        /// </summary>
        public abstract void Close();

        #endregion

        #region Transaction

        /// <summary>
        /// Begin the tracnsaction.
        /// </summary>
        public abstract void BeginTransaction();

        /// <summary>
        /// Commit the transaction
        /// </summary>
        public abstract void CommitTransaction();

        /// <summary>
        /// Roll back the transaction.
        /// </summary>
        public abstract void RollBackTransaction();

        #endregion

        #region CreateCommand

        /// <summary>
        /// Create command.
        /// </summary>
        /// <returns></returns>
        public abstract IDbCommand CreateCommand(string commandText, CommandType commandType, IDbDataParameter[] commandParameters);
        #endregion

        #region CreateParameters

        public abstract IDbDataParameter[] CreateParameters(string[] paramNames, object[] paramValues);

        public IDbDataParameter[] CreateParameters(string[] paramNames)
        {
            return this.CreateParameters(paramNames, null);
        }

        #endregion

        #region ExecuteNonQuery

        /// <summary>
        /// Execute command.
        /// </summary>
        /// <param name="commandText">Command text.</param>
        /// <param name="commandType">Command type.</param>
        /// <param name="commandParameters">Parameters.</param>
        /// <returns>Rows affected.</returns>
        public abstract int ExecuteNonQuery(string commandText, CommandType commandType, IDbDataParameter[] commandParameters);

        /// <summary>
        /// Execute command.
        /// </summary>
        /// <param name="cmdText">Command text.</param>
        /// <param name="commandParameters">Parameters.</param>
        /// <returns>Rows affected.</returns>
        public int ExecuteNonQuery(string commandText, IDbDataParameter[] commandParameters)
        {
            return this.ExecuteNonQuery(commandText, CommandType.Text, commandParameters);
        }

        /// <summary>
        /// Execute command.
        /// </summary>
        /// <param name="commandText">Command text.</param>
        /// <returns>Rows affected.</returns>
        public int ExecuteNonQuery(string commandText)
        {
            return this.ExecuteNonQuery(commandText, CommandType.Text, (IDbDataParameter[])null);
        }

        #endregion

        #region ExecuteScalar

        public abstract object ExecuteScalar(string commandText, CommandType commandType, IDbDataParameter[] commandParameters);

        public object ExecuteScalar(string commandText)
        {
            return this.ExecuteScalar(commandText, CommandType.Text, null);
        }

        public object ExecuteScalar(string commandText, CommandType commandType)
        {
            return this.ExecuteScalar(commandText, commandType, null);
        }

        public object ExecuteScalar(string commandText, IDbDataParameter[] commandParameters)
        {
            return this.ExecuteScalar(commandText, CommandType.Text, commandParameters);
        }

        #endregion

        #region FillDataSet

        public abstract void FillDataset(string selectText, DataSet dataSet, string[] tableNames,
            CommandType commandType, IDbDataParameter[] commandParameters);

        public void FillDataset(string selectText, DataSet dataSet, string[] tableNames)
        {
            this.FillDataset(selectText, dataSet, tableNames, CommandType.Text, null);
        }

        public void FillDataset(string selectText, DataSet dataSet, string[] tableNames, CommandType commandType)
        {
            this.FillDataset(selectText, dataSet, tableNames, commandType, null);
        }

        public void FillDataset(string selectText, DataSet dataSet)
        {
            this.FillDataset(selectText, dataSet, null, CommandType.Text, null);
        }

        public abstract int FillDataset(string selectText, DataSet dataset, int startRow, int maxRow, string tableName, CommandType commandType, IDbDataParameter[] commandParameters);

        public int FillDataset(string selectText, DataSet dataset, int startRow, int maxRow, string tableName)
        {
            return this.FillDataset(selectText, dataset, startRow, maxRow, tableName, CommandType.Text, null);
        }

        #endregion

        #region UpdateDataset
        public abstract void UpdateDataSet(IDbCommand insertCommand, IDbCommand deleteCommand, IDbCommand updateCommand, DataSet dataSet, string tableName);
        public abstract void UpdateDataSet(DataSet dataSet, string tableName, string sql);

        #endregion

        #region ExecuteReader

        public abstract IDataReader ExecuteReader(string selectCommandText, CommandType commandType, IDbDataParameter[] commandParameters);

        public IDataReader ExecuteReader(string selectCommandText)
        {
            return this.ExecuteReader(selectCommandText, CommandType.Text, null);
        }
        #endregion

        #region PrepareCommand

        /// <summary>
        /// Prepare command
        /// </summary>
        /// <param name="command"></param>
        /// <param name="connection"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText">stored procedure name or SQL statement</param>
        /// <param name="commandParameters">parameter array</param>
        protected virtual void PrepareCommand(IDbCommand command, IDbConnection connection, IDbTransaction transaction, CommandType commandType, string commandText, IDbDataParameter[] commandParameters)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (commandText == null || commandText.Length == 0) throw new ArgumentNullException("commandText");

            // Associate the connection with the command
            command.Connection = connection;

            // Set the command text (stored procedure name or SQL statement)
            command.CommandText = commandText;

            // If we were provided a transaction, assign it
            if (transaction != null)
            {
                if (transaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
                command.Transaction = transaction;
            }

            // Set the command type
            command.CommandType = commandType;

            // Attach the command parameters if they are provided
            if (commandParameters != null)
                this.AttachParameters(command, commandParameters);
        }

        /// <summary>
        /// Attach paramaters to command.
        /// </summary>
        /// <param name="command">command object</param>
        /// <param name="commandParameters">parameter array</param>
        private void AttachParameters(IDbCommand command, IDbDataParameter[] commandParameters)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            if (commandParameters != null)
            {
                foreach (IDbDataParameter p in commandParameters)
                {
                    if (p != null)
                    {
                        if ((p.Direction == ParameterDirection.InputOutput ||
                            p.Direction == ParameterDirection.Input) &&
                            (p.Value == null))
                        {
                            p.Value = DBNull.Value;
                        }

                        command.Parameters.Add(p);
                    }
                }
            }
        }

        #endregion

        #region FillSchema

        /// <summary>
        /// FillSchema
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="schemaType"></param>
        /// <param name="TableName"></param>
        public abstract void FillSchema(DataTable dataTable, SchemaType schemaType, string TableName);

        #endregion

        public abstract DataTable GetSchema(string collectionName);

        #region GetOleDbSchemaTable

        /// <summary>
        /// Get schema of database by oledb
        /// </summary>
        /// <param name="schema">schema name</param>
        /// <param name="restrictions">restriction of return value</param>
        /// <returns>schema table</returns>
        public abstract DataTable GetOleDbSchemaTable(Guid schema, object[] restrictions);

        #endregion

        #region 数据库批量事物操作
        /// <summary>
        /// 批量执行Sql语句，带事物操作
        /// </summary>
        /// <CreateDate>08-09-15</CreateDate>
        /// <CreateMan>xw</CreateMan>
        /// <param name="arraySql">sql数组</param>
        /// <returns></returns>
        public abstract bool BatchExecuteNonQuery(ArrayList arraySql);

        /// <summary>
        /// 批量执行Sql语句，带事物操作
        /// </summary>
        /// <CreateDate>08-09-15</CreateDate>
        /// <CreateMan>xw</CreateMan>
        /// <param name="arraySql">sql数组</param>
        /// <returns></returns>
        public abstract bool BatchExecuteNonQuery(string[] arraySql);
        #endregion

        /// <summary>
        ///  根据表名称获取表字段定义信息
        /// add by 4color 2009-2-13
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public abstract string[] GetTableFieldS(string tableName);

        public abstract ConnectionState State
        {
            get;
        }

        /// <summary>
        /// 获得数据源类型
        /// </summary>
        /// <returns></returns>
        public abstract DBType getDBType();

        public void AssignParameterValues(IDbDataParameter[] commandParameters, object[] paramValues)
        {
            if (commandParameters == null)
                throw new ArgumentNullException("commandParameters", "commandParameters can't be null");

            int length = commandParameters.Length;
            if (paramValues.Length != length)
                throw new IndexOutOfRangeException("array paramValues' length must equels commandParameters's length");

            for (int index = 0; index < length; index++)
                commandParameters[index].Value = paramValues[index];
        }

        public abstract string getDateSqlFormat(DateTime dt);

        public abstract string getParameterVarSqlFormat(string varName);

        public abstract IDbDataParameter[] CreateNTextParameter(string paramenterContent, string varName);

        #region CreateNTextParameterS根据多个变量名和值,构造IDbDataParameter数组
        /// <summary>
        /// 根据多个变量名和值,构造IDbDataParameter数组
        /// add by 4color
        /// date:2008-09-20
        /// </summary>
        /// <param name="paramenterContentS">字段值数组</param>
        /// <param name="varNameS">参数名称数组</param>
        /// <param name="dbTypeS">参数类型(SklDataProvider.NDbType枚举值数组)</param>
        /// <returns></returns>
        public abstract IDbDataParameter[] CreateParameterS(string[] paramenterContentS, string[] varNameS, string[] dbTypeS);
        #endregion

        public abstract IDbDataParameter[] CreateBlobParameter(byte[] paramenterContent, string varName);

        public abstract string getParameterTag();

        public abstract string getPriKeySqlString(string priKeyName);

        //add'ed by 4color 20070829取得用于jet引擎的连接字符串
        public abstract string getConStrForJet();
        //add'ed by 4color
        public abstract string subStrSqlString();
        public abstract string getSubStrSql(string str, int start, int length);

        public abstract string getLenStrSql(string str);

        public abstract string getLenStr();

        //add by jinxk
        public abstract string getSysTimeStr();

        public abstract string getLikeStr();

        //public abstract string getConvertStr();

        public abstract string getAddMonthsStr(string fieldName, int count);

        public abstract string getYearPart(string fieldName);
        public abstract string getMPart(string fieldName);
        public abstract string getJDPart(string fieldName);

        public abstract string getYQPart(string fieldName);
        public abstract string getYMPart(string fieldName);

        public abstract string getDateAddStr(string param, int addcount, string fieldName);

        public abstract string getDateStr(string fieldName);

        /// <summary>
        /// 根据Sql 函数名称获取当前数据库匹配的SQL串
        /// add by 4color
        /// </summary>
        /// <param name="functionName">数据库自定义函数名称</param>
        /// <returns></returns>
        public static string GetDBFunctionFormat(string functionName)
        {
            switch (DataSourceFactory.databaseType.ToLower())
            {
                case "sqlserver":
                    return "dbo." + functionName;
                case "oracle":
                    return functionName;
                case "oledb":
                    return "";
                case "oraoracle":
                    return functionName;
                default:
                    return "";
            }
        }

        #region IDisposable 成员
        public virtual void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            if (this.Connection != null)
            {
                this.Connection.Close();
                this.Connection.Dispose();
            }
        }

        public void Dispose()
        {
            // TODO:  添加 GisqDataSource.Dispose 实现
            this.Dispose(true);
            GC.SuppressFinalize(true);
        }

        #endregion

        public abstract string TableType();


        /// <summary>
        /// 获取视图类型 
        /// </summary>
        /// <returns></returns>
        public abstract string ViewType();

        public abstract string ProcedureType();

        /// <summary>
        /// 返回限制几条的语句 
        /// </summary>
        /// <param name="cmdText"></param>
        /// <returns></returns>
        public abstract string GetTopcmdText(string cmdText, int Top);
        

    }
}
