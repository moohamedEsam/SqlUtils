namespace SqlUtils
{
    public class SqlData
    {
       public SqlData(string data, SqlTypes type)
        {
            this.data = data;
            this.type = type;
        }
        public string data { get; set; }

        public SqlTypes type { get; set; }
    }

    public enum SqlTypes
    {
        NUMBER,
        STRING_AR,
        STRING_EN,
        DATE,
        TIME,
        DATE_TIME
    }
}
