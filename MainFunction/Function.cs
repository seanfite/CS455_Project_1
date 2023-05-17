using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using System.Text.Json;
using System.Xml;
using Npgsql;
using System.Data;
using System.Text.Json.Nodes;
using System.Reflection.Emit;
using System.Xml.Linq;

/*
Sean Fite
CS 455 Cloud Computing
Last Updated 5/16/23

This program is an aws lambda function that works as a trigger anytime an s3 bucket receives a new file. This program
will parse the s3 bucket file and pass those objects to a pgadmin4 database to store the data. It currently processes and 
stores covid infection relation data.
*/

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace MainFunction;

public class Function
{
    private string siteId = "";                             // objects set to use throughout project
    private string name = "";
    private string zipCode = "";
    private string month = "";
    private string day = "";
    private string year = "";
    private string date = "";
    private int totalFirstShots = 0;
    private int totalSecondShots = 0;
    private string tagValue = "";

    IAmazonS3 S3Client { get; set; }                        // set up interaction with s3 service

    public Function()                                       // Function class constructor
    {
        S3Client = new AmazonS3Client();
    }

    public Function(IAmazonS3 s3Client)                     // Second constructor to pass in IAmazonS3 object
    {
        this.S3Client = s3Client;
    }
    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)               // Lambda function entry point
    {
        var eventRecords = evnt.Records ?? new List<S3Event.S3EventNotificationRecord>();
        foreach (var record in eventRecords)                                              // retrieve s3 event records
        {
            var s3Event = record.S3;
            if (s3Event == null)
            {
                continue;
            }

            try
            {
                var response = await this.S3Client.GetObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key);

                GetObjectTaggingRequest getTagsRequest = new GetObjectTaggingRequest            // new instance for tagging request
                {
                    BucketName = s3Event.Bucket.Name,
                    Key = s3Event.Object.Key,                
                    
                };
                GetObjectTaggingResponse objectTags = await this.S3Client.GetObjectTaggingAsync(getTagsRequest);
                string tags = objectTags.GetType().Name;
                string tag = objectTags.Tagging.Count > 0 ? objectTags.Tagging[0].Key : string.Empty;  // get tag from s3 object
                tagValue = objectTags.Tagging.Count > 0 ? objectTags.Tagging[0].Value : string.Empty;
                if (tag == "xml")           
                {
                    // Read the XML content from the response stream
                    using (var reader = new StreamReader(response.ResponseStream))
                    {
                        var xmlContent = reader.ReadToEnd();
                        var xmlDoc = new XmlDocument();
                        xmlDoc.LoadXml(xmlContent);

                        // Extract the data using XPath queries

                        siteId = xmlDoc.SelectSingleNode("/data/site/@id")?.Value;
                        name = xmlDoc.SelectSingleNode("/data/site/name")?.InnerText;
                        zipCode = xmlDoc.SelectSingleNode("/data/site/zipCode")?.InnerText;
                        month = xmlDoc.SelectSingleNode("/data/@month")?.Value;
                        day = xmlDoc.SelectSingleNode("/data/@day")?.Value;
                        year = xmlDoc.SelectSingleNode("/data/@year")?.Value;
                        date = month + "/" + day + "/" + year;
                        var firstShots = xmlDoc.SelectNodes("/data/vaccines/brand/firstShot");
                        var secondShots = xmlDoc.SelectNodes("/data/vaccines/brand/secondtShot");
                        totalFirstShots = 0;
                        totalSecondShots = 0;

                        foreach (XmlNode node in firstShots)                                // sum first shots
                        {
                            if (int.TryParse(node.InnerText, out int firstShotValue))
                            {
                                totalFirstShots += firstShotValue;
                            }
                        }
                        foreach (XmlNode node in secondShots)                               // sum second shots
                        {
                            if (int.TryParse(node.InnerText, out int secondShotValue))
                            {
                                totalSecondShots += secondShotValue;
                            }
                        }
                    }
                }
                else if (tag == "json")
                {
                    {
                        using (var reader = new StreamReader(response.ResponseStream))
                        {
                            var jsonContent = reader.ReadToEnd();
                            var jsonObject = JsonDocument.Parse(jsonContent).RootElement;

                            // JSON parsing logic...
                            siteId = jsonObject.GetProperty("site").GetProperty("id").GetString();
                            name = jsonObject.GetProperty("site").GetProperty("name").GetString();
                            zipCode = jsonObject.GetProperty("site").GetProperty("zipCode").GetString();
                            month = jsonObject.GetProperty("date").GetProperty("month").GetInt32().ToString();
                            day = jsonObject.GetProperty("date").GetProperty("day").GetInt32().ToString();
                            year = jsonObject.GetProperty("date").GetProperty("year").GetInt32().ToString();
                            date = $"{month}/{day}/{year}";

                            var vaccines = jsonObject.GetProperty("vaccines");
                            totalFirstShots = 0;
                            totalSecondShots = 0;
                            foreach (var vaccine in vaccines.EnumerateArray())                  // sum first and second shots
                            {
                                var firstShot = vaccine.GetProperty("firstShot").GetInt32();
                                var secondShot = vaccine.GetProperty("secondShot").GetInt32();
                                totalFirstShots += firstShot;
                                totalSecondShots += secondShot;
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Error Reading Tag From Bucket: {0}", s3Event.Bucket.Name);
                }
            }
            catch (Exception e)
            {
                context.Logger.LogError($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogError(e.Message);
                context.Logger.LogError(e.StackTrace);
                throw;
            }
        }
        DataBaseConnector();
    }

    public string DataBaseConnector()                       // connect to pgadmin4 database
    {
        try
        {
            NpgsqlConnection conn = OpenConnection();
            if (conn.State == ConnectionState.Open)
            {
                Console.WriteLine("Successfully opened a connection to the database");

                string selectQuery = "SELECT COUNT(*) FROM Data WHERE SiteID = @SiteID";
                using (var selectCommand = new NpgsqlCommand(selectQuery, conn))
                {
                    selectCommand.Parameters.AddWithValue("@SiteID", siteId); // Replace siteId with the actual value for SiteID

                    int count = Convert.ToInt32(selectCommand.ExecuteScalar());
                    if (count > 0)
                    {
                        if (tagValue == "") // If tag value is empty, send an error message
                        {
                            Console.WriteLine("Entry already exists.");
                            return String.Empty;
                        }
                        else // If tag value is "correction", perform an UPDATE
                        {
                            Console.WriteLine("we made it to correction");
                            string updateQuery = "UPDATE Data SET Date = @Date, FirstShot = @FirstShot, SecondShot = @SecondShot WHERE SiteID = @SiteID";
                            using (var updateCommand = new NpgsqlCommand(updateQuery, conn))
                            {
                                updateCommand.Parameters.AddWithValue("@SiteID", siteId); // Replace siteId with the actual value for SiteID
                                updateCommand.Parameters.AddWithValue("@Date", date); // Replace name with the actual value for Date
                                updateCommand.Parameters.AddWithValue("@FirstShot", totalFirstShots); // Replace FirstShots with the actual value for FirstShots
                                updateCommand.Parameters.AddWithValue("@SecondShot", totalSecondShots); // Replace SecondShots with the actual value for SecondShots
                                updateCommand.ExecuteNonQuery();
                            }
                        }
                    }
                    else
                    {
                        string insertQuery = "INSERT INTO Data (SiteID, Date, FirstShot, SecondShot) VALUES (@SiteID, @Date, @FirstShot, @SecondShot)";
                        using (var command = new NpgsqlCommand(insertQuery, conn))
                        {
                            command.Parameters.AddWithValue("@SiteID", siteId); // Replace siteId with the actual value for SiteID
                            command.Parameters.AddWithValue("@Date", date); // Replace name with the actual value for Date
                            command.Parameters.AddWithValue("@FirstShot", totalFirstShots); // Replace FirstShots with the actual value for FirstShots
                            command.Parameters.AddWithValue("@SecondShot", totalSecondShots); // Replace SecondShots with the actual value for SecondShots
                            command.ExecuteNonQuery();
                        }
                        insertQuery = "INSERT INTO Site (SiteID, Name, ZipCode) VALUES (@SiteID, @Name, @ZipCode)";
                        using (var command = new NpgsqlCommand(insertQuery, conn))
                        {
                            command.Parameters.AddWithValue("@SiteID", siteId); // Replace siteId with the actual value for SiteID
                            command.Parameters.AddWithValue("@Name", name); // Replace name with the actual value for Name
                            command.Parameters.AddWithValue("@ZipCode", zipCode); // Replace zipCode with the actual value for ZipCode
                            command.ExecuteNonQuery();
                        }

                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            else
            {
                Console.WriteLine("Failed to open a connection to the database. Connection state {0}", Enum.GetName(typeof(ConnectionState), conn.State));
            }
        }
        catch (NpgsqlException ex)
        {
            Console.WriteLine("Npgsql ERROR: {0}", ex.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine("ERROR: {0}", ex.Message);
        }
        return String.Empty;
    }

    private NpgsqlConnection OpenConnection()           // use credentials to open pgadmi4 database connection
    {
        string endpoint = "project1database.c7s65u3srtdk.us-east-1.rds.amazonaws.com";
        string connString = "Server=" + endpoint + ";port=5432;Database=CovidInfectionDatabase;User ID=postgres;password=cs455pass;Timeout=15";
        NpgsqlConnection conn = new NpgsqlConnection(connString);
        conn.Open();
        return conn;
    }
}

