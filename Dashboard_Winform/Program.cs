using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using Modbus.Device;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;

namespace Dashboard_Winform
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            //========================================= START ============================================
            log4net.Config.XmlConfigurator.Configure();

            try
            {
                ModbusTcpMasterReadRegisters_SMA_MPPTs();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.ReadKey();
            //============================================ END =====================================

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new Form1());
        }

        //=========================================================== START ===========================
        public class IpInformation
        {
            public IpInformation(string IP_Address, int IP_Port, byte UnitID, int MPPT_Count)
            {
                this.IP_Address = IP_Address;
                this.IP_Port = IP_Port;
                this.UnitID = UnitID;
                this.MPPT_Count = MPPT_Count;
            }
            public string IP_Address { get; set; }
            public int IP_Port { get; set; }
            public byte UnitID { get; set; }
            public int MPPT_Count { get; set; }
        }

        static Regex regex_get_IP = new Regex(@"(?i)IP\s*:\s*(?<ip>\d+\.\d+\.\d+\.\d+)");
        static Regex regex_get_PORT = new Regex(@"(?i)PORT\s*:\s*(?<port>\d+)");
        static Regex regex_get_UNIT_ID = new Regex(@"(?i)UNIT_ID\s*:\s*(?<unitID>\d+)");
        static Regex regex_get_MPPTS = new Regex(@"(?i)MPPTS\s*:\s*(?<MPPTs>\d+)");
        static Regex regex_get_IP_TIMEOUT = new Regex(@"(?i)IP_TIMEOUT\s*:\s*(?<ipTimeout>\d+)\s*$");
        static Regex regex_get_HISTORY_DIR = new Regex(@"(?i)HISTORY_DIR\s*:\s*(?<historyDir>.*)\s*$");

        const string INI_FILE_NAME = @"CollectInverterHistory.ini";
        const int DEFAULT_IP_PORT = 502;
        const int DEFAULT_UNIT_ID = 3;
        const int DEFAULT_MPPT_COUNT = 2;
        const int DEFAULT_IP_TIMEOUT_SECONDS = 5;
        const string DEFAULT_HISTORY_DIRECTORY_PATH = @".\HistoryFiles";

        public static void ModbusTcpMasterReadRegisters_SMA_MPPTs()
        {
            string workingDirectory = Directory.GetCurrentDirectory();

            string[] ini_lines = File.ReadAllLines(INI_FILE_NAME);
            try
            {
                ini_lines = File.ReadAllLines(INI_FILE_NAME);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + '\n');
                return;
            }

            List<IpInformation> IP_Information = new List<IpInformation>();
            int ipTimeout = DEFAULT_IP_TIMEOUT_SECONDS;
            int ipPort = DEFAULT_IP_PORT;
            byte unitID = DEFAULT_UNIT_ID;
            int mpptCount = DEFAULT_MPPT_COUNT;
            string historyDir = DEFAULT_HISTORY_DIRECTORY_PATH;
            foreach (string line in ini_lines)
            {
                if (line.Trim().StartsWith("#"))
                    continue;

                Match match;
                if ((match = regex_get_IP.Match(line)).Success)
                {
                    string ipString = match.Groups["ip"].Value;
                    if ((match = regex_get_PORT.Match(line)).Success)
                        ipPort = Convert.ToInt32(match.Groups["port"].Value);
                    if ((match = regex_get_UNIT_ID.Match(line)).Success)
                        unitID = Convert.ToByte(match.Groups["unitID"].Value);
                    if ((match = regex_get_MPPTS.Match(line)).Success)
                        mpptCount = Convert.ToInt32(match.Groups["MPPTs"].Value);
                    IP_Information.Add(new IpInformation(ipString, ipPort, unitID, mpptCount));
                }
                else if ((match = regex_get_IP_TIMEOUT.Match(line)).Success)
                {
                    ipTimeout = Convert.ToInt32(match.Groups["ipTimeout"].Value);
                }
                else if ((match = regex_get_HISTORY_DIR.Match(line)).Success)
                {
                    historyDir = match.Groups["historyDir"].Value;

                    if (historyDir.StartsWith("\"") && historyDir.EndsWith("\""))
                        historyDir = historyDir.Substring(1, historyDir.Length - 2);

                    if (historyDir.EndsWith("\\") || historyDir.EndsWith("/"))
                        historyDir = historyDir.Substring(0, historyDir.Length - 1);
                }
            }

            for (; ; )
            {
                foreach (IpInformation ipInfo in IP_Information)
                {
                    using (TcpClient client = new TcpClient())
                    {
                        try
                        {
                            //Communicate with inverter
                            Console.WriteLine("\nConnecting to " + ipInfo.IP_Address);
                            IAsyncResult ar = client.BeginConnect(ipInfo.IP_Address, ipInfo.IP_Port, null, null);
                            WaitHandle wh = ar.AsyncWaitHandle;
                            try
                            {
                                if (!ar.AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(ipTimeout), false))
                                {
                                    client.Close();
                                    throw new TimeoutException("Could not connect to " + ipInfo.IP_Address);
                                }
                            }
                            finally
                            {
                                wh.Close();
                            }

                            ModbusIpMaster master = ModbusIpMaster.CreateIp(client);
                            string line = string.Empty;

                            //******************************************DESIRED PARAMETERS*****************************************************
                            //******************************************Refer to the datasheet*************************************************
                            //******************************************2 MPPTS****************************************************************
                            //Get local time from computer.
                            const string DATE_TIME_PATTERN = "HH:mm:ss";
                            const string FILE_NAME_DATE_PATTERN = "yyyyMMdd";
                            DateTime timeNow = DateTime.Now;

                            try
                            {
                                if (timeNow.ToShortTimeString() == "00:00 AM")
                                {
                                    Thread.Sleep(60000);
                                }
                            }
                            catch
                            {
                                Debug.WriteLine("");
                            }

                            string filePath;
                            string filePath_1;
                            filePath = historyDir + "\\SMA-" + timeNow.ToString(FILE_NAME_DATE_PATTERN) + ".txt";
                            filePath_1 = historyDir + "\\SMA-" + timeNow.ToString(FILE_NAME_DATE_PATTERN) + ".csv";

                            string dtComputer = timeNow.ToString(DATE_TIME_PATTERN);
                            line += dtComputer;


                            //Get inverter serial number.
                            const ushort SERIAL_NUMBER_ADR = 30005;
                            const ushort SERIAL_NUMBER_REGISTER_COUNT = 2;
                            // Read SMA serial number registers (U32)
                            ushort[] serialNumberInfo = master.ReadHoldingRegisters(ipInfo.UnitID, SERIAL_NUMBER_ADR, SERIAL_NUMBER_REGISTER_COUNT);
                            // Extract fields from U32
                            UInt32 serialNumber = ((UInt32)serialNumberInfo[0] << 16) | (UInt32)serialNumberInfo[1];
                            line += "," + serialNumber;

                            //MPPT count.
                            line += "," + ipInfo.MPPT_Count;
                            if (ipInfo.MPPT_Count > 0)
                            {
                                // Get inverter DC input 1 parameters.
                                const ushort DC_1_ADR = 30769;
                                const ushort DC_1_REGISTER_COUNT = 6;
                                // Read SMA DC 1 registers (3 x S32)
                                ushort[] DC1Info = master.ReadHoldingRegisters(ipInfo.UnitID, DC_1_ADR, DC_1_REGISTER_COUNT);
                                // Extract fields from DC 1 Current S32 (FIX3)
                                if (DC1Info[0] == 32768)
                                    DC1Info[0] = 0;
                                if (DC1Info[2] == 32768)
                                    DC1Info[2] = 0;
                                if (DC1Info[4] == 32768)
                                    DC1Info[4] = 0;

                                double DC1_Current = (double)(Int32)(((UInt32)DC1Info[0] << 16) | (UInt32)DC1Info[1]) / 1000;
                                double DC1_Voltage = (double)(Int32)(((UInt32)DC1Info[2] << 16) | (UInt32)DC1Info[3]) / 100;
                                double DC1_Power = (double)(Int32)(((UInt32)DC1Info[4] << 16) | (UInt32)DC1Info[5]);

                                line += "," + DC1_Current + "," + DC1_Voltage + "," + DC1_Power;
                            }

                            // Get inverter DC input 2 parameters.
                            if (ipInfo.MPPT_Count > 1)
                            {
                                const ushort DC_2_ADR = 30957;
                                const ushort DC_2_REGISTER_COUNT = 6;
                                // Read SMA DC 2 registers (3 x S32)
                                ushort[] DC2Info = master.ReadHoldingRegisters(ipInfo.UnitID, DC_2_ADR, DC_2_REGISTER_COUNT);
                                // Extract fields from DC 2 Current S32 (FIX3)
                                if (DC2Info[0] == 32768)
                                    DC2Info[0] = 0;
                                if (DC2Info[2] == 32768)
                                    DC2Info[2] = 0;
                                if (DC2Info[4] == 32768)
                                    DC2Info[4] = 0;
                                double DC2_Current = (double)(Int32)(((UInt32)DC2Info[0] << 16) | (UInt32)DC2Info[1]) / 1000;
                                double DC2_Voltage = (double)(Int32)(((UInt32)DC2Info[2] << 16) | (UInt32)DC2Info[3]) / 100;
                                double DC2_Power = (double)(Int32)(((UInt32)DC2Info[4] << 16) | (UInt32)DC2Info[5]);

                                line += "," + DC2_Current + "," + DC2_Voltage + "," + DC2_Power + ",";
                            }

                            if (ipInfo.MPPT_Count > 2)
                            {
                                // Get inverter DC input 3 parameters.
                                const ushort DC_3_ADR = 30963;
                                const ushort DC_3_REGISTER_COUNT = 6;
                                // Read SMA DC 3 registers (3 x S32)
                                ushort[] DC3Info = master.ReadHoldingRegisters(ipInfo.UnitID, DC_3_ADR, DC_3_REGISTER_COUNT);
                                // Extract fields from DC 3 Current S32 (FIX3)
                                if (DC3Info[0] == 32768)
                                    DC3Info[0] = 0;
                                if (DC3Info[2] == 32768)
                                    DC3Info[2] = 0;
                                if (DC3Info[4] == 32768)
                                    DC3Info[4] = 0;
                                double DC3_Current = (double)(Int32)(((UInt32)DC3Info[0] << 16) | (UInt32)DC3Info[1]) / 1000;
                                double DC3_Voltage = (double)(Int32)(((UInt32)DC3Info[2] << 16) | (UInt32)DC3Info[3]) / 100;
                                double DC3_Power = (double)(Int32)(((UInt32)DC3Info[4] << 16) | (UInt32)DC3Info[5]);
                                line += "," + DC3_Current + "," + DC3_Voltage + "," + DC3_Power + ",";
                            }

                            if (ipInfo.MPPT_Count > 3)
                            {
                                // Get inverter DC input 4 parameters.
                                const ushort DC_4_ADR = 30963;
                                const ushort DC_4_REGISTER_COUNT = 6;
                                // Read SMA DC 4 registers (3 x S32)
                                ushort[] DC4Info = master.ReadHoldingRegisters(ipInfo.UnitID, DC_4_ADR, DC_4_REGISTER_COUNT);
                                // Extract fields from DC 4 Current S32 (FIX3)
                                if (DC4Info[0] == 32768)
                                    DC4Info[0] = 0;
                                if (DC4Info[2] == 32768)
                                    DC4Info[2] = 0;
                                if (DC4Info[4] == 32768)
                                    DC4Info[4] = 0;
                                double DC4_Current = (double)(Int32)(((UInt32)DC4Info[0] << 16) | (UInt32)DC4Info[1]) / 1000;
                                double DC4_Voltage = (double)(Int32)(((UInt32)DC4Info[2] << 16) | (UInt32)DC4Info[3]) / 100;
                                double DC4_Power = (double)(Int32)(((UInt32)DC4Info[4] << 16) | (UInt32)DC4Info[5]);
                                line += "," + DC4_Current + "," + DC4_Voltage + "," + DC4_Power + ",";
                            }

                            //Get Total Yield
                            //kWh
                            const ushort DC_totalpower_ADR = 30529;
                            const ushort DC_totalpower_REGISTER_COUNT = 2;
                            ushort[] DCtotalpowerInfo = master.ReadHoldingRegisters(ipInfo.UnitID, DC_totalpower_ADR, DC_totalpower_REGISTER_COUNT);
                            if (DCtotalpowerInfo[0] == 32768)
                                DCtotalpowerInfo[0] = 0;
                            double totalyield = (double)(Int32)(((UInt32)DCtotalpowerInfo[0] << 16) | (UInt32)DCtotalpowerInfo[1]);
                            line += totalyield + ",";

                            //Get current total power
                            //kWh
                            const ushort DC_currentpower_ADR = 30775;
                            const ushort DC_currentpower_REGISTER_COUNT = 2;
                            ushort[] DCcurrentpowerInfo = master.ReadHoldingRegisters(ipInfo.UnitID, DC_currentpower_ADR, DC_currentpower_REGISTER_COUNT);
                            if (DCcurrentpowerInfo[0] == 32768)
                                DCcurrentpowerInfo[0] = 0;
                            double currentyield = (double)(Int32)(((UInt32)DCcurrentpowerInfo[0] << 16) | (UInt32)DCcurrentpowerInfo[1]);
                            line += currentyield + ",";

                            //Get daily power
                            //kWh
                            const ushort DC_dailypower_ADR = 30535;
                            const ushort DC_dailypower_REGISTER_COUNT = 2;
                            ushort[] DCdailypowerInfo = master.ReadHoldingRegisters(ipInfo.UnitID, DC_dailypower_ADR, DC_dailypower_REGISTER_COUNT);
                            if (DCdailypowerInfo[0] == 32768)
                                DCdailypowerInfo[0] = 0;
                            double dailyyield = (double)(Int32)(((UInt32)DCdailypowerInfo[0] << 16) | (UInt32)DCdailypowerInfo[1]);
                            line += dailyyield + ",";

                            //Get Inverter condition
                            const ushort DC_condition_ADR = 30201;
                            const ushort DC_condition_REGISTER_COUNT = 2;
                            ushort[] DCconditionInfo = master.ReadHoldingRegisters(ipInfo.UnitID, DC_condition_ADR, DC_condition_REGISTER_COUNT);
                            if (DCconditionInfo[0] == 32768)
                                DCconditionInfo[0] = 0;
                            double condition = (double)(Int32)(((UInt32)DCconditionInfo[0] << 16) | (UInt32)DCconditionInfo[1]);
                            line += condition + ",";

                            line += "\n";
                            File.AppendAllText(filePath, line);
                            File.AppendAllText(filePath_1, line);
                            Console.WriteLine(line);
                            ConnectionString(line);
                            
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }
                        finally
                        {
                            if (client != null)
                                ((IDisposable)client).Dispose();
                        }
                    }
                }
                Thread.Sleep(300000);       //5 minutes sleep     
            }
        }
        //============================================== END ===========================================

        // SQL START
        public static SqlConnectionStringBuilder ConnectionString(string line)
        {
            SqlConnectionStringBuilder sql = new SqlConnectionStringBuilder();

            sql.DataSource = "sqlsever-ers.database.windows.net";   // Server name from azure
            sql.UserID = "ers"; // ID to access DB
            sql.Password = "testing123#";   //password to access DB
            sql.InitialCatalog = "inverterDB";  //Database name
            StringToSql(line, sql);

            return sql;
        }

        public static void StringToSql(string line, SqlConnectionStringBuilder sql)
        {
            string[] lineList = line.Split(','); // splitting commas in 'line'

            //assigning variable for each element in 'line'
            string date = Convert.ToDateTime(lineList[0]).ToString("yyyy/MM/dd HH:mm:ss");
            int serial = Convert.ToInt32(lineList[1]);
            int mppts = Convert.ToInt32(lineList[2]);
            double DC_c1 = Convert.ToDouble(lineList[3]);
            double DC_v1 = Convert.ToDouble(lineList[4]);
            double DC_p1 = Convert.ToDouble(lineList[5]);
            double DC_c2 = Convert.ToDouble(lineList[6]);
            double DC_v2 = Convert.ToDouble(lineList[7]);
            double DC_p2 = Convert.ToDouble(lineList[8]);
            double total_yield = Convert.ToDouble(lineList[9]);
            double current_yield = Convert.ToDouble(lineList[10]);
            double daily_yield = Convert.ToDouble(lineList[11]);
            var condition = Convert.ToInt32(lineList[12]);

            //formatting strings to meet sql query syntax requirement
            string sendQuery = String.Format(
                "INSERT INTO INVERTER_DATA (_datetime, Serial, MPPTs, DC_c1, DC_v1, DC_p1, DC_c2, DC_v2, DC_p2, total_yield, current_yield, daily_yield, condition) " +
                "VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12});"
                , ("'" + date + "'"), serial, mppts, DC_c1, DC_v1, DC_p1, DC_c2, DC_v2, DC_p2, total_yield, current_yield, daily_yield, condition);

            // Connecting to sql and execute query formed above
            using (SqlConnection sqlconn = new SqlConnection(sql.ConnectionString))
            {
                String sqlquery = sendQuery.ToString();
                SqlCommand sqlCommand = new SqlCommand(sqlquery, sqlconn);
                try
                {
                    sqlconn.Open();
                    sqlCommand.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                

            }
        }

        // SQL END
    }
}
