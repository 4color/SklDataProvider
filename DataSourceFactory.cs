/*******************************************************************
 * Created : hyf
 * Date    : 2006-12-13
 * 
 * ****************************************************************/
using System;

namespace SklDataProvider
{
    /// <summary>
    /// DataSourceFactory 的摘要说明。
    /// </summary>
    public class DataSourceFactory
    {
        public static string connectionString = "";
        public static string databaseType = "sqlserver";

        public DataSourceFactory()
        {
            //
            // TODO: 在此处添加构造函数逻辑

        }

        #region Create Data Source Instance

        public static SklDataSource CreateInstance()
        {
            return DataSourceFactory.CreateInstance(connectionString, databaseType);

        }


        /// <summary>
        /// Create GisqDataSource object.
        /// </summary>
        /// <param name="connectionString">
        /// Connection string.
        /// SqlServer : Server=[NetSDK];Initial Catalog=[pubs];uid=[sa];pwd=123
        /// Oracle    : Data Source = [GisqOA];User Id=[UserID];Password=[Password]
        /// Access    : Provider = Microsoft.Jet.OLEDB.4.0;Data Source=[C:\Northwind.mdb]											  
        /// </param>
        /// <returns>GisqDataSource object.</returns>
        public static SklDataSource CreateInstance(string connectionString)
        {
            return DataSourceFactory.CreateInstance(connectionString, "sqlserver");
        }

        /// <summary>
        /// Create GisqDataSource object.
        /// </summary>
        /// <param name="connectionString">Connection string.</param>
        /// <param name="dbType">Database type.(Sql Server default)</param>
        /// <returns>GisqDataSource object.</returns>
        public static SklDataSource CreateInstance(string connectionString, string databaseType)
        {
            SklDataSource dataSource = null;
            switch (databaseType.ToLower())
            {
                case "sqlserver":
                    dataSource = new SqlDataSource(connectionString);
                    break;
                case "oracle":
                    dataSource = new OracleDataSource(connectionString);
                    break;
                case "oledb":
                    dataSource = new OleDataSource(connectionString);
                    break;
                case "oraoracle":
                    dataSource = new OraOracleDataSource(connectionString);
                    break;
                case "mysql":
                    dataSource = new MysqlDataSource(connectionString);
                    break;
                case "sqlite":
                    dataSource = new SqliteDataSource(connectionString);
                    break;
                default:
                    dataSource = new SqlDataSource(connectionString);
                    break;
            }

            return dataSource;
        }

        public static SklDataSource CreateInstance(string dataSource, string catalog, string username, string password, string databaseType)
        {
            string connectionString = "";
            switch (databaseType.ToLower())
            {
                case "sqlserver":
                    connectionString = "Data Source=" + dataSource + ";Initial Catalog=" + catalog + ";User ID=" + username + ";Password=" + password;
                    break;
                case "oracle":
                    connectionString = "Data Source=" + dataSource + ";User ID=" + username + ";Password=" + password;
                    break;
                case "oraoracle":
                    connectionString = "Data Source=" + dataSource + ";User ID=" + username + ";Password=" + password;
                    break;
            }
            return DataSourceFactory.CreateInstance(connectionString, databaseType);
        }

        #endregion
    }
}
