using System;
using System.Collections;
using System.Data;
using System.Data.OleDb;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;

namespace SklDataProvider
{
	/// <summary>
	/// OraDataSource 的摘要说明。
	/// </summary>
	public class OraOracleDataSource:SklDataSource
	{
		
		private OracleConnection _connection;	
		private OracleTransaction _transaction=null;
        private string _connectionString;

		public OraOracleDataSource(string connectionString)
		{
		    _connectionString = connectionString;
			this._connection=new OracleConnection(connectionString);
            this.m_provider = "OraOLEDB.Oracle.1";
        }

        #region Connection
        public override IDbConnection Connection
        {
            get
            {
                return this._connection;
            }
        }

        public override ConnectionState State
        {
            get
            {
                return this._connection.State;
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
                 //this._connection = null;
            }
        }

        #endregion

        #region Transaction

        public override void BeginTransaction()
        {
            _transaction = this._connection.BeginTransaction();
        }

        public override void CommitTransaction()
        {
            if (_transaction == null)
                throw new Exception("没有有效的事务用于提交!");
            _transaction.Commit();
            _transaction = null;
        }

        public override void RollBackTransaction()
        {
            if (_transaction == null)
                throw new Exception("没有有效的事务用于回滚!");
            _transaction.Rollback();
            _transaction = null;
        }

        #endregion

        #region CreateCommand

        public override IDbCommand CreateCommand(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            OracleCommand command = new OracleCommand(commandText);
            command.CommandType = commandType;
            for (int index = 0; index < commandParameters.Length; index++)
                command.Parameters.Add(commandParameters[index]);
            return command;
        }

        #endregion

        #region CreateParameters

        public override IDbDataParameter[] CreateParameters(string[] paramNames, object[] paramValues)
        {
            if (paramNames == null)
                throw new ArgumentNullException("paramNames", "paramNames can't be null");

            int length = paramNames.Length;
            OracleParameter[] parameters = new OracleParameter[length];

            if (paramValues != null)
            {
                if (paramValues.Length != length)
                    throw new IndexOutOfRangeException("array paramNames' length must equels paramValues's length");

                for (int index = 0; index < length; index++)
                    parameters[index] = new OracleParameter(paramNames[index], paramValues[index]);
            }
            else
            {
                for (int index = 0; index < length; index++)
                {
                    parameters[index] = new OracleParameter();
                    parameters[index].ParameterName = paramNames[index];
                }
            }

            return parameters;
        }

        #endregion

        #region ExecuteNonQuery

        public override int ExecuteNonQuery(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {

            OracleCommand cmd = new OracleCommand();
            //cmd.Parameters.Clear();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, commandText, commandParameters);

            return cmd.ExecuteNonQuery();
        }

        #endregion

        #region FillDataset

        public override void FillDataset(string selectText, DataSet dataSet, string[] tableNames, CommandType commandType, IDbDataParameter[] commandParameters)
        {
       // OracleDataAdapter adapter = new OracleDataAdapter(selectText, this._connection);
            OracleCommand cmd = new OracleCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);
            using (OracleDataAdapter adapter = new OracleDataAdapter(cmd))
            {
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
            OracleCommand cmd = new OracleCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);
            using (OracleDataAdapter adapter = new OracleDataAdapter(cmd))
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

	    /*public void FillDataset(string selectText,DataSet dataSet,int startRecord,int maxRecords,string srcTable){
            
        }*/

        //public override void FillDataset(string selectText, DataSet dataSet, int startRecord, int maxRecords, string srcTable) {
        //    OracleDataAdapter adapter=new OracleDataAdapter(selectText,this._connection);			
        //    adapter.Fill(dataSet,startRecord,maxRecords,srcTable);

        //}

        #endregion

        #region UpdateDataset

        public override void UpdateDataSet(IDbCommand insertCommand, IDbCommand deleteCommand, IDbCommand updateCommand, DataSet dataSet, string tableName)
        {
            if (insertCommand == null) throw new ArgumentNullException("insertCommand");
            if (deleteCommand == null) throw new ArgumentNullException("deleteCommand");
            if (updateCommand == null) throw new ArgumentNullException("updateCommand");
            if (tableName == null || tableName.Length == 0) throw new ArgumentNullException("tableName");

            // Create a SqlDataAdapter, and dispose of it after we are done
            using (OracleDataAdapter dataAdapter = new OracleDataAdapter())
            {
                // Set the data adapter commands
                dataAdapter.UpdateCommand = (OracleCommand)updateCommand;
                dataAdapter.InsertCommand = (OracleCommand)insertCommand;
                dataAdapter.DeleteCommand = (OracleCommand)deleteCommand;

                // Update the dataset changes in the data source
                dataAdapter.Update(dataSet, tableName);

                // Commit all the changes made to the DataSet
                dataSet.AcceptChanges();
            }

        }
        public override void UpdateDataSet(DataSet dataSet, string tableName, string sql)
        {
            OracleDataAdapter m_OracleDataAdapter = new OracleDataAdapter();
            m_OracleDataAdapter.SelectCommand = new OracleCommand(sql, this._connection);
            OracleCommandBuilder Oraclecb = new OracleCommandBuilder(m_OracleDataAdapter);
            m_OracleDataAdapter.Update(dataSet, tableName);
            dataSet.Tables[tableName].AcceptChanges();
        }
        #endregion

        public override DataTable GetOleDbSchemaTable(Guid schema, object[] restrictions) {
			//GisqDataSource conn=GisqDataSource.CreateInstance("Provider=MSDAORA.1;Password="+this.m_password+";User ID="+this.m_username+";Data Source="+this.m_datasource+";Persist Security Info=True","oledb");
			OleDbConnection conn=new OleDbConnection("Provider="+this.m_provider+";Password="+this.m_password+";User ID="+this.m_username+";Data Source="+this.m_datasource+";Persist Security Info=True");
			conn.Open();
			DataTable tablelist=conn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Tables,new object[]{null,this.m_catalog,null,null});
			conn.Close();
			return tablelist;
		}

		public override void Dispose(bool disposing) {
			base.Dispose (disposing);
        }

        #region ExecuteScalar
        public override object ExecuteScalar(string commandText, CommandType commandType, IDbDataParameter[] commandParameters) {
			OracleCommand cmd = new OracleCommand();
			//cmd.Parameters.Clear();
			this.PrepareCommand(cmd,this._connection,this._transaction,commandType,commandText,commandParameters);
			
			return cmd.ExecuteScalar();
        }

        #endregion      
        
        #region ExecuteReader
        public override IDataReader ExecuteReader(string selectCommandText, CommandType commandType, IDbDataParameter[] commandParameters) {
			OracleCommand cmd = new OracleCommand();
			this.PrepareCommand(cmd,this._connection,this._transaction,commandType,selectCommandText,commandParameters);
			cmd.Parameters.Clear();

			return cmd.ExecuteReader();
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
            string sql = "SELECT   *  FROM " + tableName +" WHERE 1=0";
            DataSet ds = new DataSet();
            this.FillDataset(sql, ds);
            if (ds != null && ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                string[] fieldS = new string[dt.Columns.Count];
                for (int u = 0; u < dt.Columns.Count; u++)
                {
                    fieldS[u] = dt.Columns[u].ColumnName;
                }
                return fieldS;
            }
            return new string[0];
        }

        public override void FillSchema(DataTable dataTable, SchemaType schemaType, string TableName) {
			OracleDataAdapter adapter=new OracleDataAdapter("select * from "+TableName,this._connection);
			adapter.FillSchema(dataTable,schemaType);
		}

		public override DBType getDBType() {
			return DBType.DB_OraOracle;
		}


        public override DataTable GetSchema(string collectionName)
        {
            OleDbConnection conn = new OleDbConnection("Provider=OraOLEDB.Oracle.1;" + this._connectionString + ";Persist Security Info=True");
            conn.Open();
            DataTable tablelist = conn.GetSchema(collectionName);
            conn.Close();
            return tablelist;     
            
        }

		public override string getDateSqlFormat(DateTime dt)
		{
			// TODO:  添加 OraOracleDataSource.getDateSqlFormat 实现
			return String.Format("To_Date('{0}','yyyy-mm-dd hh24:mi:ss')",dt);
		}
        
		public override string getSubStrSql(string str, int start, int length)
		{
			string strtmp="";
			strtmp=" SUBSTR("+str+","+Convert.ToString(start)+","+Convert.ToString(length)+")";
			return strtmp;
		}

        public override string getLenStr()
        {
            return "LENGTH";
        }
		public override string getLenStrSql(string str)
		{
			string strtmp;
			strtmp=" LENGTH(" + str + ") ";
			return strtmp;
		}

		public override string getSysTimeStr()
		{
			return "SYSDATE";
		}

		public override string getLikeStr()
		{
			return "%";
		}

		public override string getAddMonthsStr(string fieldName,int count)
		{
			return "ADD_MONTHS(" + fieldName + "," + count + ")";
		}

		public override string getYearPart(string fieldName)
		{
			return "TO_CHAR(" + fieldName + ",'YYYY')";
		}

		public override string getParameterVarSqlFormat(string varName)
		{
			// TODO:  添加 OraOracleDataSource.getParameterVarSqlFormat 实现
			return  ":"+varName;
		}
	
		public override IDbDataParameter[] CreateNTextParameter(string paramenterContent, string varName)
		{
			// TODO:  添加 OraOracleDataSource.CreateNTextParameter 实现
			
			OracleParameter[] p = new OracleParameter[1];
			p[0] = new OracleParameter();
			p[0].OracleDbType = OracleDbType.Clob;
			p[0].Direction = ParameterDirection.Input;
			p[0].ParameterName=varName;
			p[0].Value = paramenterContent;
			return p;
		}

		public override IDbDataParameter[] CreateBlobParameter(byte[] paramenterContent,string varName)
		{
			OracleParameter[] p = new OracleParameter[1];
			p[0] = new OracleParameter();
			p[0].OracleDbType = OracleDbType.Blob;
			p[0].Direction = ParameterDirection.Input;
			p[0].ParameterName=varName;
			p[0].Value = paramenterContent;
			return p;

		}
	
		public override string getPriKeySqlString(string priKeyName)
		{
			// TODO:  添加 OraOracleDataSource.getPriKeySqlString 实现
			return "select SEQ_"+priKeyName+".Currval from dual";
		}
		public override string getConStrForJet()
		{
			return null;
		}

        //add'ed by 4color
        public override string subStrSqlString()
        {
            return "substr";
        }

    
        public override string getMPart(string fieldName)
        {
            return "TO_CHAR(" + fieldName + ",'MM')";
        }
        public override string getJDPart(string fieldName)
        {
            return "TO_CHAR(" + fieldName + ",'Q')";
        }

        public override string getYQPart(string fieldName)
        {
            return "TO_CHAR(" + fieldName + ",'YYYY-Q')";
        }

        public override string getYMPart(string fieldName)
        {
            return "TO_CHAR(" + fieldName + ",'YYYY-MM')";
        }

        public override string getDateAddStr(string param, int addcount, string fieldName)
        {
            string ret = "";
            switch (param.ToLower())
            {
                case "yyyy":
                    ret = "add_months(" + fieldName + "," + addcount * 12 + ")";
                    break;
                case "mm":
                    ret = "add_months(" + fieldName + "," + addcount + ")";
                    break;
                default:
                    ret = fieldName + "+" + addcount;
                    break;
            }
            return ret;
        }

        public override IDbDataParameter[] CreateParameterS(string[] paramenterContentS, string[] varNameS, string[] dbTypeS)
        {
            if (paramenterContentS != null && varNameS != null)
            {        
                OracleParameter[] p = new OracleParameter[varNameS.Length];
                for (int n = 0; n < p.Length; n++)
                {
                    p[n] = new OracleParameter();
                    p[n].OracleDbType= DbUtility.GetOraOracleDbType(dbTypeS[n]);
                    p[n].Direction = ParameterDirection.Input;
                    p[n].ParameterName = ":" + varNameS[n];
                    p[n].Value = DbUtility.GetObjectNullHandler(dbTypeS[n], paramenterContentS[n]);
                }
                return p;
            }
            return null;
        }

        public override string getParameterTag()
        {
            return ":";
        }

        #region 数据库批量事物操作
        public override bool BatchExecuteNonQuery(ArrayList arraySql)
        {
            bool blnResult = true;
            try
            {
                OracleCommand cmd = new OracleCommand();
                this.Open();
                this.BeginTransaction();
                foreach (string strSql in arraySql)
                {
                    cmd.Connection = this._connection;
                //    cmd.Transaction = this._transaction;
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
                blnResult = false;
            }
            return blnResult;
        }
        #endregion

        public override string getDateStr(string fieldName)
        {
            return "TO_CHAR(" + fieldName + ",'YYYY-MM-DD')";
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
