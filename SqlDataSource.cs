using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;

namespace SklDataProvider
{
    /// <summary>
    /// SqlDataSource 的摘要说明。
    /// </summary>
    public class SqlDataSource : SklDataSource
    {
        private SqlConnection _connection;
        private SqlTransaction _transaction;


        public SqlDataSource(string connectionString)
        {
            //
            // TODO: 在此处添加构造函数逻辑
            //
            this._connection = new SqlConnection(connectionString);
            this._transaction = null;
        }

        #region Connection

        public override System.Data.IDbConnection Connection
        {
            get
            {
                return this._connection;
            }
        }

        public override void Open()
        {
            if (this._connection.State != ConnectionState.Open)
                this._connection.Open();
        }

        public override void Close()
        {
            if (this._connection.State != ConnectionState.Closed)
                this._connection.Close();
        }

        #endregion

        #region Transaction

        public override void BeginTransaction()
        {
            this._transaction = this._connection.BeginTransaction();
        }

        public override void CommitTransaction()
        {
            this._transaction.Commit();
            //4color 2005-10-11 add
            this._transaction = null;
            //

        }

        public override void RollBackTransaction()
        {
            this._transaction.Rollback();
            //4color 2005-10-11 add
            this._transaction = null;
            //
        }

        #endregion

        #region CreateCommand

        public override IDbCommand CreateCommand(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            SqlCommand command = new SqlCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, commandParameters);

            return command;
        }

        #endregion

        #region CreateParameters

        public override IDbDataParameter[] CreateParameters(string[] paramNames, object[] paramValues)
        {
            if (paramNames == null)
                throw new ArgumentNullException("paramNames", "paramNames can't be null");

            int length = paramNames.Length;
            SqlParameter[] parameters = new SqlParameter[length];

            if (paramValues != null)
            {
                if (paramValues.Length != length)
                    throw new IndexOutOfRangeException("array paramNames' length must equels paramValues's length");

                for (int index = 0; index < length; index++)
                    parameters[index] = new SqlParameter(paramNames[index], paramValues[index]);
            }
            else
            {
                for (int index = 0; index < length; index++)
                {
                    parameters[index] = new SqlParameter();
                    parameters[index].ParameterName = paramNames[index];
                }
            }

            return parameters;
        }

        #endregion

        #region ExecuteNonQuery

        public override int ExecuteNonQuery(string commandText, CommandType cmdType, IDbDataParameter[] commandParameters)
        {
            SqlCommand cmd = new SqlCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, cmdType, commandText, commandParameters);

            //4color 2005-10-11 delete
            //cmd.Parameters.Clear();
            //
            return cmd.ExecuteNonQuery();
        }


        #endregion

        #region FillDataset

        public override void FillDataset(string selectText, DataSet dataSet, string[] tableNames, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            SqlCommand cmd = new SqlCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);

            using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
            {
                // Add the table mappings specified by the user
                if (tableNames != null && tableNames.Length > 0)
                {
                    string tableName = "Table";
                    int tableNums = tableNames.Length;

                    for (int index = 0; index < tableNums; index++)
                    {
                        if (tableNames[index] == null || tableNames[index].Length == 0) throw new ArgumentException("The tableNames parameter must contain a list of tables, a value was provided as null or empty string.", "tableNames");
                        adapter.TableMappings.Add(tableName, tableNames[index]);
                        tableName += (index + 1).ToString();
                    }
                }

                adapter.Fill(dataSet);
                cmd.Parameters.Clear();
            }

        }

        public override int FillDataset(string selectText, DataSet dataset, int startRow, int maxRow, string tableName, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            int result = 0;
            SqlCommand cmd = new SqlCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);
            using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
            {
                if (string.IsNullOrEmpty(tableName))
                {
                    tableName = "Table";
                }
                result = adapter.Fill(dataset, startRow, maxRow, tableName);
                cmd.Parameters.Clear();
            }
            return result;
        }
        #endregion

        #region UpdateDataset

        public override void UpdateDataSet(IDbCommand insertCommand, IDbCommand deleteCommand, IDbCommand updateCommand, DataSet dataSet, string tableName)
        {
            if (insertCommand == null) throw new ArgumentNullException("insertCommand");
            if (deleteCommand == null) throw new ArgumentNullException("deleteCommand");
            if (updateCommand == null) throw new ArgumentNullException("updateCommand");
            if (tableName == null || tableName.Length == 0) throw new ArgumentNullException("tableName");

            // Create a SqlDataAdapter, and dispose of it after we are done
            using (SqlDataAdapter dataAdapter = new SqlDataAdapter())
            {
                // Set the data adapter commands
                dataAdapter.UpdateCommand = (SqlCommand)updateCommand;
                dataAdapter.InsertCommand = (SqlCommand)insertCommand;
                dataAdapter.DeleteCommand = (SqlCommand)deleteCommand;

                // Update the dataset changes in the data source
                dataAdapter.Update(dataSet, tableName);

                // Commit all the changes made to the DataSet
                dataSet.AcceptChanges();
            }
        }

        public override void UpdateDataSet(DataSet dataSet, string tableName, string sql)
        {

            using (SqlDataAdapter dataAdapter = new SqlDataAdapter(sql, this._connection))
            {

                SqlCommandBuilder sqlComandBuilder = new SqlCommandBuilder(dataAdapter);
                //更改此方法对事务不支持,特添加如下代码修复此BUG
                if (this._transaction != null)
                {
                    if (dataAdapter.SelectCommand != null)
                    {
                        dataAdapter.SelectCommand.Transaction = this._transaction;
                    }
                    else if (dataAdapter.InsertCommand != null)
                    {
                        dataAdapter.InsertCommand.Transaction = this._transaction;
                    }
                    else if (dataAdapter.DeleteCommand != null)
                    {
                        dataAdapter.DeleteCommand.Transaction = this._transaction;
                    }
                }
                //END add by 4color 2008-12-08
                dataAdapter.Update(dataSet, tableName);
            }
        }
        #endregion

        #region ExecuteScalar
        public override object ExecuteScalar(string commandText, CommandType commandType, IDbDataParameter[] parameters)
        {
            SqlCommand command = new SqlCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, parameters);

            return command.ExecuteScalar();
        }
        #endregion

        #region ExecuteReader
        public override IDataReader ExecuteReader(string selectCommandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            SqlCommand command = new SqlCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, selectCommandText, commandParameters);

            return command.ExecuteReader();
        }

        #endregion
        /// <summary>
        /// 根据表名称获取表字段定义信息
        /// add by 4color 2009-2-13
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override string[] GetTableFieldS(string tableName)
        {
            string sql = "SELECT sc.NAME FROM SYSCOLUMNS sc INNER JOIN SYSOBJECTS so on sc.id=so.id WHERE so.NAME='" + tableName + "'";
            DataSet ds = new DataSet();
            this.FillDataset(sql, ds);
            if (ds != null && ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                string[] fieldS = new string[dt.Rows.Count];
                for (int u = 0; u < dt.Rows.Count; u++)
                {
                    fieldS[u] = dt.Rows[u]["NAME"].ToString();
                }
                return fieldS;
            }
            return new string[0];
        }


        public override void Dispose(bool disposing)
        {
            if (this._transaction != null)
            {
                this._transaction.Dispose();
                this._transaction = null;
            }

            base.Dispose(disposing);
        }

        public override DataTable GetSchema(string collectionName)
        {
            return this._connection.GetSchema(collectionName);
        }


        public override DataTable GetOleDbSchemaTable(Guid schema, object[] restrictions)
        {
            return null;
        }

        public override void FillSchema(DataTable dataTable, SchemaType schemaType, string TableName)
        {

        }

        #region 数据库批量事物操作
        /// <summary>
        /// 批量执行Sql语句，带事物操作
        /// </summary>
        /// <CreateDate>08-09-15</CreateDate>
        /// <CreateMan>xw</CreateMan>
        /// <param name="arraySql">sql数组</param>
        /// <returns></returns>
        public override bool BatchExecuteNonQuery(ArrayList arraySql)
        {
            bool blnResult = true;
            try
            {
                SqlCommand cmd = new SqlCommand();
                this.Open();
                this.BeginTransaction();
                foreach (string strSql in arraySql)
                {
                    cmd.Connection = this._connection;
                    cmd.Transaction = this._transaction;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = strSql;
                    cmd.ExecuteNonQuery();
                }
                this.CommitTransaction();
                this.Close();
            }
            catch
            {
                this.RollBackTransaction();
                this.Close();
                blnResult = false;
            }
            return blnResult;
        }

        /// <summary>
        /// 批量执行Sql语句，带事物操作
        /// </summary>
        /// <CreateDate>08-09-15</CreateDate>
        /// <CreateMan>xw</CreateMan>
        /// <param name="arraySql">sql数组</param>
        /// <returns></returns>
        public override bool BatchExecuteNonQuery(string[] arraySql)
        {
            bool blnResult = true;
            try
            {
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = this._connection;
                cmd.CommandType = CommandType.Text;
                this.Open();
                this.BeginTransaction();
                foreach (string strSql in arraySql)
                {
                    cmd.CommandText = strSql;
                    cmd.ExecuteNonQuery();
                }
                this.CommitTransaction();
                this.Close();
            }
            catch
            {
                this.RollBackTransaction();
                this.Close();
                blnResult = false;
            }
            return blnResult;
        }
        #endregion


        public override DBType getDBType()
        {
            return DBType.DB_SQL_Server;
        }

        public override ConnectionState State
        {
            get
            {
                return this._connection.State;
            }
        }

        public override string getDateSqlFormat(DateTime dt)
        {
            // TODO:  添加 SqlDataSource.getDateSqlFormat 实现
            return "'" + dt.ToString() + "'";
        }

        public override string subStrSqlString()
        {
            return "substring";
        }
        public override string getSubStrSql(string str, int start, int length)
        {
            string strtmp = "";
            strtmp = " SUBSTRING(" + str + "," + Convert.ToString(start) + "," + Convert.ToString(length) + ")";
            return strtmp;
        }

        public override string getLenStr()
        {
            return " LEN";
        }
        public override string getLenStrSql(string str)
        {
            string strtmp;
            strtmp = " LEN(" + str + ") ";
            return strtmp;
        }

        public override string getSysTimeStr()
        {
            return "GETDATE()";
        }

        public override string getLikeStr()
        {
            return "%";
        }

        public override string getDateAddStr(string param, int addcount, string fieldName)
        {
            return "DATEADD(" + param + "," + addcount + "," + fieldName + ")";
        }

        public override string getDateStr(string fieldName)
        {
            return "CONVERT(char(10)," + fieldName + ",120)";
        }

        public override string getAddMonthsStr(string fieldName, int count)
        {
            return "DATEADD(Month," + count + "," + fieldName + ")";
        }

        public override string getYearPart(string fieldName)
        {
            return "DATEPART(yyyy," + fieldName + ")";
        }

        public override string getMPart(string fieldName)
        {
            // TODO:  添加 SqlDataSource.getMPart 实现
            return "DATEPART(mm," + fieldName + ")";
        }

        public override string getJDPart(string fieldName)
        {
            // TODO:  添加 SqlDataSource.getRPart 实现
            return "DATEPART(q," + fieldName + ")";
        }

        //返回年-季度
        public override string getYQPart(string fieldName)
        {
            //cast(year(getdate()) as char(4)) + '-' + cast(datepart(q,getdate()) as char(4))
            return "cast(year(" + fieldName + ") as char(4))" + "+'-'+" +
                "cast(datepart(q," + fieldName + ") as char(1))";
        }

        //返回年-月份
        public override string getYMPart(string fieldName)
        {
            return "convert(char(7)," + fieldName + ",120)";
        }
        public override string getParameterVarSqlFormat(string varName)
        {
            // TODO:  添加 SqlDataSource.getParameterVarSqlFormat 实现
            return "@" + varName;
        }

        public override IDbDataParameter[] CreateNTextParameter(string paramenterContent, string varName)
        {
            // TODO:  添加 SqlDataSource.CreateNTextParameter 实现

            SqlParameter[] p = new SqlParameter[1];
            p[0] = new SqlParameter();
            p[0].SqlDbType = SqlDbType.NText;
            p[0].Direction = ParameterDirection.Input;
            p[0].ParameterName = "@" + varName;
            p[0].Value = paramenterContent;
            return p;

        }

        public override IDbDataParameter[] CreateParameterS(string[] paramenterContentS, string[] varNameS, string[] dbTypeS)
        {
            if (paramenterContentS != null && varNameS != null)
            {
                SqlParameter[] p = new SqlParameter[varNameS.Length];
                for (int n = 0; n < p.Length; n++)
                {
                    p[n] = new SqlParameter();
                    p[n].SqlDbType = DbUtility.GetSqlDbType(dbTypeS[n]);
                    p[n].Direction = ParameterDirection.Input;
                    p[n].ParameterName = "@" + varNameS[n];
                    p[n].Value = DbUtility.GetObjectNullHandler(dbTypeS[n], paramenterContentS[n]);
                }
                return p;
            }
            return null;
        }

        public override IDbDataParameter[] CreateBlobParameter(byte[] paramenterContent, string varName)
        {

            SqlParameter[] p = new SqlParameter[1];
            p[0] = new SqlParameter();
            p[0].SqlDbType = SqlDbType.VarBinary;
            p[0].Direction = ParameterDirection.Input;
            p[0].ParameterName = "@" + varName;
            p[0].Value = paramenterContent;
            return p;


        }
        public override string getParameterTag()
        {
            return "@";
        }
        public override string getPriKeySqlString(string priKeyName)
        {
            // TODO:  添加 SqlDataSource.getPriKeySqlString 实现
            return "select @@identity";
        }
        public override string getConStrForJet()
        {
            string tmp = DataSourceFactory.connectionString;
            string[] con = tmp.Split(new char[] { ';' });
            tmp = "[ODBC;Driver=SQL Server;UID=" + con[1].Split(new char[] { '=' })[1].Trim()
                + ";PWD=" + con[2].Split(new char[] { '=' })[1].Trim()
                + ";Server=" + con[0].Split(new char[] { '=' })[1].Trim()
                + "]";
            return tmp;
        }


        public override string TableType()
        {
            return "BASE TABLE";
        }

        public override string ViewType()
        {
            return "VIEW";
        }

        public override string ProcedureType()
        {
            return "SPECIFIC_NAME";
        }

        public override string GetTopcmdText(string cmdText, int Top)
        {
            return "select top " + Top + " * from (" + cmdText + ") as t ";
        }

    }
}
