using System;
using System.Text.RegularExpressions;

namespace SklDataProvider
{
	/// <summary>
	/// Verifier ��ժҪ˵����
	/// </summary>
	public class Verifier
	{
		public Verifier()
		{
			//
			// TODO: �ڴ˴���ӹ��캯���߼�
			//
		}
		/// <summary>
		/// У���Ƿ�Ϊ���ڸ�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ����ڷ��ش���ֵ,�Ƿ����ڷ��ص�ǰ����ֵ</returns>
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
		/// У���Ƿ�Ϊ�����͸�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ������ͷ��ش���ֵ,�Ƿ������ͷ�����</returns>
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
		/// У���Ƿ�Ϊ�����͸�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ������ͷ��ش���ֵ,�Ƿ������ͷ�����</returns>
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
		/// У���Ƿ�Ϊ���͸�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ����ͷ��ش���ֵ,�Ƿ����ͷ�����</returns>
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
		/// У���Ƿ�Ϊ�ַ���
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
		/// У�鲼��ֵ
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
		/// У���Ƿ�Ϊdouble�͸�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ�double�ͷ��ش���ֵ,�Ƿ�double�ͷ�����</returns>
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
		/// У���Ƿ�Ϊsingle�͸�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ�single�ͷ��ش���ֵ,�Ƿ�single�ͷ�����</returns>
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
		/// У���Ƿ�Ϊ�ֽڸ�ʽ
		/// </summary>
		/// <param name="Value">ҪУ���ֵ</param>
		/// <returns>�Ϸ��ֽڷ��ش���ֵ,�Ƿ��ֽڷ�����</returns>
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
		/// �ж��Ƿ�Ϊ����
		/// </summary>
		/// <param name="str">�ַ���</param>
		/// <returns></returns>
		static public bool IsNumeric(string str) 
		{
			if(str==null || str.Length==0)return false;
			return Regex.IsMatch(str, @"^[+-]?\d*[.]?\d*$");
		}

		/// <summary>
		/// �ж��Ƿ�Ϊ����
		/// </summary>
		/// <param name="date">�ַ���</param>
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
