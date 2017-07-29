using System;
using System.Data;
using System.Data.SQLite;
using System.Collections;
using System.Collections.Generic;

namespace SklDataProvider
{

    /// <summary>
    /// MysqlDataSource 的摘要说明。
    /// </summary>
    /// 
    public class SqliteDataSource : SklDataSource
    {

        private SQLiteConnection _connection;
        private SQLiteTransaction _transaction;

        public SqliteDataSource(string connectionString)
        {
            //
            // TODO: 在此处添加构造函数逻辑


            this._connection = new SQLiteConnection(connectionString);
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
            if (this._connection.State == ConnectionState.Open)
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
            //hyf 2006-3-27 add
            this._transaction = null;
            //
        }

        public override void RollBackTransaction()
        {
            this._transaction.Rollback();
            //hyf 2006-3-27 add
            this._transaction = null;
            //
        }

        #endregion

        #region CreateCommand
        public override IDbCommand CreateCommand(string commandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {

            SQLiteCommand command = new SQLiteCommand();
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


            SQLiteParameter[] parameters = new SQLiteParameter[length];

            if (paramValues != null)
            {
                if (paramValues.Length != length)
                    throw new IndexOutOfRangeException("array paramNames' length must equels paramValues's length");

                for (int index = 0; index < length; index++)
                    parameters[index] = new SQLiteParameter(paramNames[index], paramValues[index]);
            }
            else
            {
                for (int index = 0; index < length; index++)
                {
                    parameters[index] = new SQLiteParameter();
                    parameters[index].ParameterName = paramNames[index];
                }
            }

            return parameters;
        }



        #endregion

        #region ExecuteNonQuery
        public override int ExecuteNonQuery(string commandText, CommandType cmdType, IDbDataParameter[] commandParameters)
        {
            SQLiteCommand cmd = new SQLiteCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, cmdType, commandText, commandParameters);

            //hyf 2006-3-27 delete
            //cmd.Parameters.Clear();

            return cmd.ExecuteNonQuery();
        }
        #endregion

        #region FillDataset

        public override void FillDataset(string selectText, DataSet dataSet, string[] tableNames, CommandType commandType, IDbDataParameter[] commandParameters)
        {
            SQLiteCommand cmd = new SQLiteCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);


            using (SQLiteDataAdapter adapter = new SQLiteDataAdapter(cmd))
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
            SQLiteCommand cmd = new SQLiteCommand();
            this.PrepareCommand(cmd, this._connection, this._transaction, commandType, selectText, commandParameters);
            using (SQLiteDataAdapter adapter = new SQLiteDataAdapter(cmd))
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
            using (SQLiteDataAdapter dataAdapter = new SQLiteDataAdapter())
            {
                // Set the data adapter commands
                dataAdapter.UpdateCommand = (SQLiteCommand)updateCommand;
                dataAdapter.InsertCommand = (SQLiteCommand)insertCommand;
                dataAdapter.DeleteCommand = (SQLiteCommand)deleteCommand;

                // Update the dataset changes in the data source
                dataAdapter.Update(dataSet, tableName);

                // Commit all the changes made to the DataSet
                dataSet.AcceptChanges();
            }
        }

        public override void UpdateDataSet(DataSet dataSet, string tableName, string sql)
        {


            using (SQLiteDataAdapter dataAdapter = new SQLiteDataAdapter(sql, this._connection))
            {

                SQLiteCommandBuilder oleDbComandBuilder = new SQLiteCommandBuilder(dataAdapter);


                dataAdapter.Update(dataSet, tableName);


            }


        }


        #endregion

        #region ExecuteReader
        public override IDataReader ExecuteReader(string selectCommandText, CommandType commandType, IDbDataParameter[] commandParameters)
        {

            SQLiteCommand command = new SQLiteCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, selectCommandText, commandParameters);

            return command.ExecuteReader();
        }
        #endregion

        #region ExecuteScalar

        public override object ExecuteScalar(string commandText, CommandType commandType, IDbDataParameter[] parameters)
        {
            SQLiteCommand command = new SQLiteCommand();
            this.PrepareCommand(command, this._connection, this._transaction, commandType, commandText, parameters);

            return command.ExecuteScalar();
        }

        #endregion

        public override void Dispose(bool disposing)
        {
            if (this._transaction != null)
            {
                //?????
                //this._transaction.Dispose();
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
            string[] s = new string[restrictions.Length];
            int i = 0;
            foreach (object obj in restrictions)
            {
                if (obj != null)
                    s[i] = obj.ToString();
                i++;
            }
            return this._connection.GetSchema(schema.ToString(), s);


        }


        /// <summary>
        /// 根据表名称获取表字段定义信息
        /// add by wusl 2009-2-13
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public override string[] GetTableFieldS(string tableName)
        {
            string sql = "SELECT   *  FROM " + tableName + " WHERE 1=0";
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

                SQLiteCommand cmd = new SQLiteCommand();
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
                SQLiteCommand cmd = new SQLiteCommand();
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
            return DBType.DB_OleDb;
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
            // TODO:  添加 OleDataSource.getDateSqlFormat 实现
            return "#" + dt.ToString() + "#";
        }

        public override string getParameterVarSqlFormat(string varName)
        {
            // TODO:  添加 OleDataSource.getParameterVarSqlFormat 实现
            //return "@"+varName;
            return "?";
        }

        public override string getParameterTag()
        {
            return "@";
        }


        public override IDbDataParameter[] CreateNTextParameter(string paramenterContent, string varName)
        {

            SQLiteParameter[] p = new SQLiteParameter[1];
            p[0] = new SQLiteParameter();
            p[0].DbType = DbType.String;
            p[0].Direction = ParameterDirection.Input;
            p[0].ParameterName = varName;
            p[0].Value = paramenterContent;
            return p;
        }
        public override IDbDataParameter[] CreateParameterS(string[] paramenterContentS, string[] varNameS, string[] dbTypeS)
        {
            if (paramenterContentS != null && varNameS != null)
            {
                SQLiteParameter[] p = new SQLiteParameter[paramenterContentS.Length];
                for (int i = 0; i < p.Length; i++)
                {
                    p[i] = new SQLiteParameter();
                    p[i].DbType = DbUtility.GetSqliteDbType(dbTypeS[i]);
                    p[i].Direction = ParameterDirection.Input;
                    p[i].ParameterName = varNameS[i];
                    p[i].Value = paramenterContentS[i];
                }
                return p;
            }
            return null;
        }

        public override IDbDataParameter[] CreateBlobParameter(byte[] paramenterContent, string varName)
        {

            SQLiteParameter[] p = new SQLiteParameter[1];
            p[0] = new SQLiteParameter();
            p[0].DbType = DbType.Binary;
            p[0].Direction = ParameterDirection.Input;
            p[0].ParameterName = varName;
            p[0].Value = paramenterContent;
            return p;

        }

        public override string getPriKeySqlString(string priKeyName)
        {
            // TODO:  添加 OleDataSource.getPriKeySqlString 实现
            return "select @@identity";
        }

        public override string subStrSqlString()
        {
            return "substr";
        }
        public override string getSubStrSql(string str, int start, int length)
        {
            string strtmp = "";
            strtmp = " substr(" + str + "," + Convert.ToString(start) + "," + Convert.ToString(length) + ")";
            return strtmp;
        }

        public override string getLenStrSql(string str)
        {
            string strtmp = "";
            strtmp = " length(" + str + ") ";
            return strtmp;
        }

        public override string getLenStr()
        {
            throw new NotImplementedException();
        }
        public override string getSysTimeStr()
        {
            return "Date()";
        }

        public override string getLikeStr()
        {
            return "*";
        }

        public override string getDateAddStr(string param, int addcount, string fieldName)
        {
            return "FORMAT(DATEADD('" + param + "'," + addcount + "," + fieldName + "),'yyyy-MM-dd')";
        }

        public override string getDateStr(string fieldName)
        {
            return "Format(" + fieldName + ",'yyyy-MM-dd')";
        }

        public override string getAddMonthsStr(string fieldName, int count)
        {
            return "DATEADD('m'," + count + "," + fieldName + ")";
        }

        public override string getYearPart(string fieldName)
        {
            return "Format(" + fieldName + ",'yyyy')";

        }

        public override string getMPart(string fieldName)
        {
            // TODO:  添加 OleDataSource.getMPart 实现
            return "Format(" + fieldName + ",'mm')";
        }

        public override string getJDPart(string fieldName)
        {
            // TODO:  添加 OleDataSource.getRPart 实现
            return "Format(" + fieldName + ",'q')";
        }

        public override string getYQPart(string fieldName)
        {
            throw new NotImplementedException();
        }

        public override string getYMPart(string fieldName)
        {
            throw new NotImplementedException();
        }

        public override string getConStrForJet()
        {
            string tmp = this._connection.ConnectionString;
            string[] con = tmp.Split(new char[] { ';' });
            tmp = con[0];
            con = tmp.Split(new char[] { '=' });
            tmp = con[1];
            return "[;database=" + tmp.Trim() + "]";
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
            return "PROCEDURE_NAME";
        }

        public override string GetTopcmdText(string cmdText, int Top)
        {
            return "select * from (" + cmdText + ") limit 0," + Top;
        }
    }
}
