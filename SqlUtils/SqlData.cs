namespace SqlUtils
{
    /// <summary>
    /// class to specify the database value and its type
    /// </summary>
    public class SqlData
    {
       public SqlData(string data, SqlTypes type)
        {
            Data = data;
            Type = type;
        }
        public string Data { get; set; }

        public SqlTypes Type { get; set; }
    }
    /// <summary>
    /// valid database types
    /// </summary>
    public enum SqlTypes
    {
        Number,
        StringAr,
        StringEn,
        Date,
        Time,
        DateTime
    }
}
