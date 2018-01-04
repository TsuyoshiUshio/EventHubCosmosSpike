using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.Configuration;


namespace EventHubAndCosmosSpike
{
    public class Trace<T>
    {
        /// <summary>
        /// EventRootId 
        /// </summary>
        public string EventRootID { get; set; }
        /// <summary>
        /// EventTrace is trace attribute which includes FunctionNames
        /// </summary>
        public string[] EventTrace { get; set; }
        /// <summary>
        /// Azure Function Name
        /// </summary>
        public string FunctionName { get; set; }

        private T tracerContext;
        /// <summary>
        /// Store the Context object. You can store Poco object if it is JSON convertable.
        /// When you Set the Context object, TracerContextBase64 is automatically generated.
        /// If you modify the context object, you need to call GenerateTracerContextBase64() 
        /// method or ToJsonString method to re-generate TracerContextBase64
        /// </summary>
        public T TracerContext
        {
            get
            {
                return this.tracerContext;
            }

            set
            {
                this.tracerContext = value;
                GenerateTracerContextBase64();
            }
        }
        /// <summary>
        /// Base64 representation of TracerContext
        /// This is used for serialize/deserialize this object from LogAnalytics
        /// </summary>
        /// <returns></returns>
        public string TracerContextBase64 { get; set; }
        /// <summary>
        /// Generate Base64 value from TracerContext and set to TracerContextBase64
        /// This method mainly used for testing. 
        /// </summary>
        public void GenerateTracerContextBase64()
        {
            var tracerContextString = JsonConvert.SerializeObject(TracerContext);
            TracerContextBase64 = Base64Encode(tracerContextString);
        }

        /// <summary>
        /// Create a JSON string representation. Before generating JSON
        /// This method call GenerateTracerContextBase64() method to re-generate
        /// TracerContextBase64
        /// </summary>
        /// <returns>JSON string representation of this instance</returns>
        public string ToJSONString()
        {
            GenerateTracerContextBase64();
            return JsonConvert.SerializeObject(this);
        }

        private static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        private static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = System.Convert.FromBase64String(base64EncodedData);
            return System.Text.Encoding.UTF8.GetString(base64EncodedBytes);
        }
        /// <summary>
        /// Deserialize the TracerContext object from TracerContextBase64 
        /// </summary>
        public void DecodeTraceContext()
        {
            tracerContext = JsonConvert.DeserializeObject<T>(Base64Decode(TracerContextBase64));
        }
    }

    class SomeContext
    {
        public string EventRootID { get; set; }
        public string[] EventTrace { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

       

            private static async Task MainAsync()
            {
                var EhsConnectionString = ConfigurationManager.AppSettings.Get("EhsConnectionString");
            var EhEntityPath = "tracehub"; // EventHub name
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhsConnectionString)
                {
                    EntityPath = EhEntityPath
                };
                var guid = Guid.NewGuid();
                var trace = new Trace<SomeContext>()
                {
                    EventRootID = guid.ToString(),
                    EventTrace = new string[] { "RequestHandler" },
                    FunctionName = "RequestHandler",
                    TracerContext = new SomeContext()
                    {
                        EventRootID = guid.ToString(),
                        EventTrace = new string[] { "RequestHandler" },
                        Key = "Foo",
                        Value = "Bar"
                    }

                };
                var eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                try
                {
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(trace.ToJSONString())));
                Console.WriteLine("Message has sent!");
            }
                catch (Exception e)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {e.Message}");
                }


            // Wait for the execution of the Azure Functions
            await Task.Delay(TimeSpan.FromSeconds(10));

            // document db query 
            var databaseName = "tracedb";
            var collectionName = "tracelog";
            var EndpointUrl = "https://printserverlessdb.documents.azure.com:443/";
            var PrimaryKey = ConfigurationManager.AppSettings.Get("PrimaryKey");
            var client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
            var query = client.CreateDocumentQuery<Trace<SomeContext>>(
                UriFactory.CreateDocumentCollectionUri(databaseName, collectionName))
                .Where(f => f.EventRootID == guid.ToString());
            
            foreach (var traces in query)
            {
                Console.WriteLine(JsonConvert.SerializeObject(traces));
            }

            Console.ReadLine();

            Console.WriteLine("--------------------------------");
            var sql = client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                $"SELECT * FROM c WHERE c.EventRootID = '{guid.ToString()}'");

            foreach (var traces in sql)
            {
                Console.WriteLine(traces);
            }
            Console.ReadLine();
            
        }
    }
}
