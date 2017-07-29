using System;
using System.Data;
using System.Data.OracleClient;
using System.Collections;
using System.Collections.Generic;



namespace SklDataProvider
{
    /// <summary>
    /// OracleDataSource 的摘要说明。
    /// </summary>
    public class OracleDataSource : SklDataSource
    {
        private OracleConnection _connection;
        private OracleTransaction _transaction;

        public OracleDataSource(string connectionString)
        {
            //
            // TODO: 在此处添加构造函数逻辑
            //
            this._connection = new OracleConnection(connectionString);
            this._transaction = null;
        }

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
            if (this._connection != null)
            {
                if (this._connection.State != ConnectionState.Closed)
                    this._connection.Close();
                this._connection = null;
            }
        }

        public override void BeginTransaction()
        {
            this._transaction = this._connection.BeginTransaction();
        }

        public override void CommitTransaction()
        {
            this._transaction.Commit();
        }

        public override void RollBackTransaction()
        {
            this._transaction.Rollback();
        }

        public override IDbCommand CreateCommand(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OracleCommand command = new OracleCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, commandParameters);

            return command;
        }


        public override int ExecuteNonQuery(string commandText, CommandType cmdType, IDbDataParameter[] commandParameters)
        {
            OracleCommand cmd = new OracleCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, cmdType, commandText, commandParameters);

            return cmd.ExecuteNonQuery();
        }

        #region FillDataset

        public override void FillDataset(string selectText, DataSet dataSet, string[] tableNames, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OracleDataAdapter oda = new OracleDataAdapter(selectText, this._connection);
            oda.Fill(dataSet);
        }

        public override int FillDataset(string selectText, DataSet dataset, int startRow, int maxRow, string tableName, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            int result = 0;
            //OleDbCommand cmd = new OleDbCommand();
            //this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);
            //using (OleDbDataAdapter adapter = new OleDbDataAdapter(cmd))
            //{
            //    if (string.IsNullOrEmpty(tableName))
            //    {
            //        tableName = "Table";
            //    }
            //    result = adapter.Fill(dataset, startRow, maxRow, tableName);
            //    cmd.Parameters.Clear();
            //}
            throw new Exception("未实现的方法！");
            return result;
        }

        #endregion

        public override string ToString()
        {
            return this.GetType().Name;
        }

        public override void UpdateDataSet(IDbCommand insertCommand, IDbCommand deleteCommand, IDbCommand updateCommand, DataSet dataSet, string tableName)
        {

        }
        public override void UpdateDataSet(DataSet dataSet, string tableName, string sql)
        {

        }

        public override IDataReader ExecuteReader(string selectCommandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OracleCommand command = new OracleCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, selectCommandText, commandParameters);

            return command.ExecuteReader();
        }

        public override object ExecuteScalar(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OracleCommand command = new OracleCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, commandParameters);

            return command.ExecuteScalar();
        }

        public override IDbDataParameter[] CreateParameters(string[] paramNames, object[] paramValues)
        {
            return null;
        }

        public override DataTable GetSchema(string collectionName)
        {
            return null;
        }
        public override DataTable GetOleDbSchemaTable(Guid schema, object[] restrictions)
        {
            return null;
        }

        public override void FillSchema(DataTable dataTable, SchemaType schemaType, string TableName)
        {

        }

        /// <summary>
        /// 根据表名称获取表字段定义信息
        /// add by wusl 2009-2-13
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override string[] GetTableFieldS(string tableName)
        {
            string sql = "SELECT  COLUMN_NAME  FROM USER_TAB_COLUMNS WHERE TABLE_NAME='" + tableName + "'";
            DataSet ds = new DataSet();
            this.FillDataset(sql, ds);
            if (ds != null && ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                string[] fieldS = new string[dt.Rows.Count];
                for (int u = 0; u < dt.Rows.Count; u++)
                {
                    fieldS[u] = dt.Rows[u]["COLUMN_NAME"].ToString();
                }
                return fieldS;
            }
            return new string[0];
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
                OracleCommand cmd = new OracleCommand();
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
                OracleCommand cmd = new OracleCommand();
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
            }
            return blnResult;
        }
        #endregion

        public override DBType getDBType()
        {
            return DBType.DB_Oracle;
        }

        public override ConnectionState State
        {
            get
            {
                return this._connection.State;
            }
        }


        #region IDisposable 成员

        public override void Dispose(bool disposing)
        {
            if (this._transaction != null)
            {
                this._transaction.Dispose();
                this._transaction = null;
            }

            base.Dispose(disposing);
        }


        #endregion

        public override string getDateSqlFormat(DateTime dt)
        {
            // TODO:  添加 OracleDataSource.getDateSqlFormat 实现
            return null;
        }

        public override string getParameterVarSqlFormat(string varName)
        {
            // TODO:  添加 OracleDataSource.getParameterVarSqlFormat 实现
            return null;
        }

        public override IDbDataParameter[] CreateNTextParameter(string paramenterContent, string varName)
        {
            // TODO:  添加 OracleDataSource.CreateNTextParameter 实现
            return null;
        }

        public override IDbDataParameter[] CreateParameterS(string[] paramenterContentS, string[] varNameS, string[] dbTypeS)
        {
            //System.Data.OracleClient.OracleParameter[] = {};
            return null;
        }

        public override IDbDataParameter[] CreateBlobParameter(byte[] paramenterContent, string varName)
        {
            return null;

        }

        public override string getPriKeySqlString(string priKeyName)
        {
            // TODO:  添加 OracleDataSource.getPriKeySqlString 实现
            return null;
        }

        public override string subStrSqlString()
        {
            return "";
        }
        public override string getSubStrSql(string str, int start, int length)
        {
            return null;
        }

        public override string getLenStr()
        {
            return "LENGTH";
        }
        public override string getLenStrSql(string str)
        {
            return null;
        }

        public override string getSysTimeStr()
        {
            return "SYSDATE";
        }

        public override string getLikeStr()
        {
            return "%";
        }

        public override string getDateAddStr(string param, int addcount, string fieldName)
        {
            throw new NotImplementedException();
        }

        public override string getDateStr(string fieldName)
        {
            return "";
        }

        public override string getAddMonthsStr(string fieldName, int count)
        {
            return "ADD_MONTHS(" + fieldName + "," + count + ")";
        }

        public override string getYearPart(string fieldName)
        {
            return "TO_CHAR(" + fieldName + ",'YYYY')";
        }

        public override string getConStrForJet()
        {
            return null;
        }


        public override string getJDPart(string fieldName)
        {
            // TODO:  添加 OracleDataSource.getJDPart 实现
            return null;
        }

        public override string getMPart(string fieldName)
        {
            // TODO:  添加 OracleDataSource.getMPart 实现
            return null;
        }

        public override string getYQPart(string fieldName)
        {
            throw new NotImplementedException();
        }

        public override string getYMPart(string fieldName)
        {
            throw new NotImplementedException();
        }


        public override string getParameterTag()
        {
            return ":";
        }


        public override string TableType()
        {
            return "TABLE";
        }

        public override string ViewType()
        {
            return "VIEW";
        }

        public override string ProcedureType()
        {
            return "PROCEDURE_NAME";
        }

        public override string GetTopcmdText(string cmdText, int Top)
        {
            return "select * from (" + cmdText + ") where ROWNUM <=" + Top;
        }
    }
}
