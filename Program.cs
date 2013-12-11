using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Oracle.DataAccess.Client;
using System.Data;
using System.Threading;


namespace CollectCER
{
    class Program
    {
        /* 
         * This tool is to collect CER files in local file store on CER
         * Frontend servers, and put them in proper location on file server.
         * And submit a CER reprocessing request to reprocess the CERs which
         * status is -18 in the last 7 days
         */

        //Comment this for testing
        //private static string target = @"C:\CER\target\";

        private static string target = @"\\erfs\ERStoreForPrd\errorreportsprd\";
        static void Main(string[] args)
        {
            try
            {
                //Comment this for testing.
                //string[] serverName = { "USSCLSECERAPP01", "USSCLSECERAPP02", "USSCLSECERAPP03" };

                //for (int i = 0; i <= 2; i++)

                string[] serverName = {"USSCLPECERAPP01","USSCLPECERAPP02","USSCLPECERAPP03",
                                        "USSCLPECERAPP04","USSCLPECERAPP05","USSCLPECERAPP06"};
                for (int i=0; i<=5; i++)
                {
                    //Comment this for testing.
                    //string folder = string.Format(@"\\{0}\d$\Tony\", serverName[i]);
                    string folder = string.Format(@"\\{0}\d$\deploy\cer_server\localFileStoreLocation\", serverName[i]);

                    //Collect all CERs received since some day in local store. There are 3 parameters are required for execute this program: year, month and day. e.g. 2013 4 27
                    //so all the *.zip files which was created after this given date under localFileStoreLocation folder will be moved to \\erfs
                    string[] cerFiles = Directory.GetFiles(folder, "*.zip", SearchOption.TopDirectoryOnly);
                    foreach (string file in cerFiles)
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        DateTime dateTime = fileInfo.CreationTime;

                        int year = int.Parse(args[0]);
                        int month = int.Parse(args[1]);
                        int day = int.Parse(args[2]);
                        if (dateTime.Year >= year && dateTime.Month >= month && dateTime.Day >= day)
                        {
                            string path = string.Format(@"{0}-{1}\{2}\{3}\", dateTime.Month, dateTime.Year, dateTime.Day, dateTime.Hour);


                            bool success = false;
                            try
                            {
                                string targetFile = target + path + fileInfo.Name;
                                Console.WriteLine("Copy " + fileInfo.Name + " to " + targetFile);

                                if (!Directory.Exists(target + path))
                                    Directory.CreateDirectory(target + path);
                                File.Copy(file, targetFile, true);
                                success = true;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Error copy: " + fileInfo.Name);
                                throw;
                            }
                            finally
                            {
                                if (success)
                                    try
                                    {
                                        Console.WriteLine("Delete " + file);
                                        File.Delete(file);
                                    }
                                    catch (Exception)
                                    { Console.WriteLine("Error delete: " + file); throw; }
                            }
                        }
                    }
                }
                // Start a new Thread to invoke the DBProcess method.
                Thread t = new Thread(new ThreadStart(DBProcess));
                t.Start();
                t.Join();
            }
            finally
            {
                
            }
        }


        private static void DBProcess()
        {
            int CERCount = 0;
            CERCount = QueryCERNumber();

            try
            {
                while (CERCount > 0)
                {
                    SubmitReprocessing();

                    int reprocessCount = 0;
                    reprocessCount = QueryReprocessingNumber();

                    while (reprocessCount > 0)
                    {
                        //Unit: millisecond   --  600000 millisecond == 10 minutes
                        Thread.Sleep(600000);
                        reprocessCount = QueryReprocessingNumber();
                    }

                    if (reprocessCount < 0)
                    {
                        Console.WriteLine("Error! ");
                    }
                    else
                    {
                        CERCount -= 5000;
                    }

                }

                Console.WriteLine("There is no -18 CERs in the last 7 days.");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e);
            }

        }


        //Query the number of CER reprocessing requests which submitted by Tony-Wei Feng 
        private static int QueryReprocessingNumber()
        {
            string connStr = "Data Source=CERPRD.AUTODESK.COM;USER ID=CER_REPROCESSING;Password=admin;";
            OracleConnection conn = new OracleConnection(connStr);

            try
            {
                int count;
                conn.Open();
                OracleCommand cmd = conn.CreateCommand();
                string queryCode = "SELECT COUNT(*) FROM CER_REPROCESSING_REQUESTS t WHERE t.submitted_by = 'Tony-Wei Feng' and t.status in ('Reprocessing','ONREPROCESSQUEUE')";
                cmd.CommandText = queryCode;
                count = Convert.ToInt32(cmd.ExecuteScalar());

                return count;
            }

            catch (Exception e)
            {
                Console.WriteLine("Error: " + e);
                return -100;
            }

            finally
            {
                conn.Close();
            }
        }

        //Query the count of -26 CERs in CER PRD DB
        private static int QueryCERNumber()
        {
            string connStr = "Data Source=CERPRD.AUTODESK.COM;USER ID=cer;Password=certeixeira;";
            OracleConnection conn = new OracleConnection(connStr);

            try
            {
                conn.Open();
                OracleCommand cmd = conn.CreateCommand();
                string queryCode = "SELECT COUNT(*) FROM CER_ERROR_REPORT t WHERE t.status = -18 and (sysdate - daterecieved) <7";
                cmd.CommandText = queryCode;
                int count = Convert.ToInt32(cmd.ExecuteScalar());

                return count;
            }

            catch(Exception e)
            {
                Console.WriteLine("Error: " + e);
                return -99;
            }

            finally
            {
                conn.Close();
            }
        }


        //Submit a CER reprocessing request in to INTO CER_REPROCESSING_REQUESTS table in CER PRD DB
        private static void SubmitReprocessing()
        {
            string connStr = "Data Source=CERPRD.AUTODESK.COM;USER ID=CER_REPROCESSING;Password=admin;";
            OracleConnection conn = new OracleConnection(connStr);

            try
            {
                conn.Open();
                OracleCommand cmd = conn.CreateCommand();

                string insertCode = "INSERT INTO CER_REPROCESSING_REQUESTS " +
                                        "(" +
                                            "ID, " +
                                            "SUBMITTED_BY, " +
                                            "DATE_SUBMITTED, " +
                                            "STATUS, " +
                                            "TYPE, " +
                                            "EXTRACT_ARCHIVE, " +
                                            "CREATE_DMP_INFO, " +
                                            "PARSE_DMP_INFO, " +
                                            "PARSE_USER_XML_INFO, " +
                                            "BUCKET, " +
                                            "ROUNDTRIP, " +
                                            "UPDATE_SUCESSFUL_STATUS, " +
                                            "CRITERIA, " +
                                            "CYCLE_ID" +
                                        ") " +
                                    "VALUES " +
                                        "(" +
                                            "CER_REPROCESSING_REQUEST_SEQ.NEXTVAL, " +
                                            "'" + "Tony-Wei Feng" + "', " +
                                            "TO_DATE('" + System.DateTime.Now.ToString() + "', 'mm/dd/yyyy HH:MI:SS PM'), " +
                                            "'" + "ONREPROCESSQUEUE" + "', " +
                                            "'" + "QUERY" + "', " +
                                            "1, " +
                                            "1, " +
                                            "1, " +
                                            "1, " +
                                            "1, " +
                                            "1, " +
                                            "1, " +
                                            //Comment this for testing.
                                            //"'" + "select ... " + "', " +
                                            "'" + "select id from cer_error_report where status=-18 and (sysdate - daterecieved) <7" + "', " +
                                            "1" +
                                        ")";

                cmd.CommandText = insertCode;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e);
            }

            finally
            {
                conn.Close();
            }
        }
    }
}
