using Amazon.S3.Model;
using Amazon.S3;
using System;
using System.Reflection.Metadata.Ecma335;
using Amazon.Runtime;
using Amazon;
using Amazon.Runtime.CredentialManagement;

/*
Sean Fite
CS 455 Cloud Computing
Last Updated 5/16/23
This class passes a json and/or xml file to an s3 bucket project-test-001, this class produces an executable file Inpute.exe, 
this file is structured to be called from a terminal along with a file path and tag 
*/

namespace UploadDataConsoleApp
{
    class Program
    {
        public static string bucketName = "project-test-001";       // s3 bucket name

        public static async Task Main(string[] args)
        {
            if (args.Length < 2)                                    // if exe is called without file path and tag
            {
                Console.WriteLine("Command input error, please edit and try again");
                return;
            }
            string filePath = args[0];                                        // split terminal call into filePath and tag
            string tagKey = args[1];  
            int position = filePath.LastIndexOf("\\");                        // parse filename from filePath
            string key = filePath.Substring(position + 1);
            string tagValue = "";
            if(key.Contains("Correction") || key.Contains("correction"))
            {
                tagValue = "correction";
            }
            AWSCredentials credentials = GetAWSCredentialsByName("default");                    // retrieve aws credentials
            AmazonS3Client s3Client = new AmazonS3Client(credentials, RegionEndpoint.USEast1);  // create aws instance
            await UploadFileToS3(s3Client, filePath, bucketName, key, tagKey, tagValue);                  // call method to upload file to s3 bucket
            s3Client.Dispose();
        }

        static AWSCredentials GetAWSCredentialsByName(string profileName)               // retrieve aws credentials
        {
            if (String.IsNullOrEmpty(profileName))
            {
                throw new ArgumentException("profileName cannot be null or empty");
            }
            SharedCredentialsFile credFile = new SharedCredentialsFile();
            CredentialProfile profile = credFile.ListProfiles().Find(p => p.Name.Equals(profileName));
            if (profile == null)
            {
                throw new Exception(String.Format("Profile name {0} not found", profileName));
            }
            return AWSCredentialsFactory.GetAWSCredentials(profile, new SharedCredentialsFile());
        }
                                                                                        // method to upload file to s3
                                                                                        // passing in terminal command objects 
        static async Task UploadFileToS3(AmazonS3Client s3Client, string filePath, string bucketName, string key, string tagKey, string tagValue)
        {
            try                                                                  
            {
                var tags = new List<Tag>                                // add tag to s3 bucket file upload
                {
                    new Tag { Key = tagKey, Value = tagValue }
                };
                await s3Client.PutObjectAsync(new PutObjectRequest      // send request to upload file to s3 bucket
                {
                    BucketName = bucketName,
                    Key = key,
                    FilePath = filePath,
                    TagSet = tags
                });
                Console.WriteLine($"File uploaded: {key} to bucket: {bucketName}");
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine($"Amazon S3 Error: {e.Message}");
                Console.WriteLine($"Error Code: {e.ErrorCode}");
                Console.WriteLine($"Request ID: {e.RequestId}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error uploading file: {e.Message}");
                Console.WriteLine(e.Message);
            }
        }
    }
}
