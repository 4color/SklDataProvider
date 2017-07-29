using System;
using System.Text.RegularExpressions;

namespace SklDataProvider
{
	/// <summary>
	/// Verifier 的摘要说明。
	/// </summary>
	public class Verifier
	{
		public Verifier()
		{
			//
			// TODO: 在此处添加构造函数逻辑
			//
		}
		/// <summary>
		/// 校验是否为日期格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法日期返回传入值,非法日期返回当前日期值</returns>
		static public DateTime VerifyDateTime(object Value)
		{
            if (DBNull.Value.Equals(Value))
                return DateTime.MinValue;
            // return DateTime.Now;
            else
            {
                if (Value != null)
                {
                    if (Value.ToString() == "")
                        //return DateTime.Now;
                        return DateTime.MinValue;
                    return DateTime.Parse(Value.ToString());
                }
                else
                    //return DateTime.Now;
                    return DateTime.MinValue;
            }
		}
		/// <summary>
		/// 校验是否为长整型格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法长整型返回传入值,非法长整型返回零</returns>
		static public long VerifyLong(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return 0;
			else
			{
				if((Value!=null)&&(Value.ToString()!="null"))
				{
					if(Value.ToString()=="")
						return 0;					
					return Int64.Parse(Value.ToString());
				}
				else
					return 0;
			}
		}
		/// <summary>
		/// 校验是否为短整型格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法短整型返回传入值,非法短整型返回零</returns>
		static public short VerifyShort(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return 0;
			else
			{
				if(Value!=null)
				{
					if(Value.ToString()=="")
						return 0;
					return Int16.Parse(Value.ToString());
				}
				else
					return 0;
			}
		}
		/// <summary>
		/// 校验是否为整型格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法整型返回传入值,非法整型返回零</returns>
		static public int VerifyInt(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return 0;
			else
			{
				if(Value!=null)
				{
					if(Value.ToString()=="")
						return 0;
					return Int32.Parse(Value.ToString());
				}
				else
					return 0;
			}
		}
		/// <summary>
		/// 校验是否为字符串
		/// </summary>
		/// <param name="Value"></param>
		/// <returns></returns>
		static public string VerifyString(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return "";
			else
			{
				if(Value!=null)
					return Value.ToString();
				else
					return "";
			}
		}	
		/// <summary>
		/// 校验布尔值
		/// </summary>
		/// <param name="Value"></param>
		/// <returns></returns>
		static public bool VerifyBool(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return false;
			else
			{
				if(Value!=null)
				{
					if(Value.ToString()=="")
						return false;

					if(Value.ToString()=="0")
						return false;
					if(Value.ToString()=="1")
						return true;

					return Boolean.Parse(Value.ToString());
					//					if(Int16.Parse(Value.ToString())!=0)
					//						return true;
					//					else
					//						return false;
				}
				else
					return false;
			}
		}

		/// <summary>
		/// 校验是否为double型格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法double型返回传入值,非法double型返回零</returns>
		static public double VerifyDouble(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return 0;
			else
			{
				if(Value!=null)
				{
					if(Value.ToString()=="")
						return 0;
					return System.Double.Parse(Value.ToString());
				}
				else
					return 0;
			}
		}

		/// <summary>
		/// 校验是否为single型格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法single型返回传入值,非法single型返回零</returns>
		static public float VerifySingle(object Value)
		{
			if(DBNull.Value.Equals(Value))
				return 0;
			else
			{
				if(Value!=null)
				{
					if(Value.ToString()=="")
						return 0;
					return System.Single.Parse(Value.ToString());
				}
				else
					return 0;
			}
		}

		/// <summary>
		/// 校验是否为字节格式
		/// </summary>
		/// <param name="Value">要校验的值</param>
		/// <returns>合法字节返回传入值,非法字节返回零</returns>
		static public byte VerifyByte(object Value)
		{
			if(System.DBNull.Value.Equals(Value))
				return 0;
			else
			{
				if(Value!=null)
				{
					if(Value.ToString()=="")
						return 0;
					return byte.Parse(Value.ToString());
				}
				else
					return 0;
			}
		}


		/// <summary>
		/// 判断是否为数字
		/// </summary>
		/// <param name="str">字符串</param>
		/// <returns></returns>
		static public bool IsNumeric(string str) 
		{
			if(str==null || str.Length==0)return false;
			return Regex.IsMatch(str, @"^[+-]?\d*[.]?\d*$");
		}

		/// <summary>
		/// 判断是否为日期
		/// </summary>
		/// <param name="date">字符串</param>
		/// <returns></returns>
		static public bool IsDate(string date)   
		{
			DateTime dt;
			bool isDate = true;
			try
			{
				dt=DateTime.Parse(date);
			}
			catch(FormatException)
			{
				isDate=false;
			}
    
			return isDate;
		}

	}
}
