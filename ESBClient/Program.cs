using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ESBClient;
using Newtonsoft.Json;
using System.Collections;

namespace ESBClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Out.WriteLine("ESBClient start");

            var calls = 0;
            ESBClient.Register("/math/plus", 1, (ht, cb) =>
            {
                calls++;
                //var payload = System.Text.Encoding.UTF8.GetString(data);
                //var ht = JsonConvert.DeserializeObject<Hashtable>(payload, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Include, FloatParseHandling = FloatParseHandling.Decimal });
                var sum = Convert.ToInt32(ht["a"]) + Convert.ToInt32(ht["b"]);
                cb(null, ESBClient.stringToByteArray(String.Format("{0}", sum)));
            });

            var requests = 0;
            var responses = 0;
            var errors = 0;
            var chuck = 50000;
            var complete = chuck;
            var start = DateTime.Now;
            while (true)
            {
                if (ESBClient.isReady)
                {
                    //if (complete == chuck)
                    //{
                    //    if (requests > 0)
                    //    {
                    //        var diff = (DateTime.Now - start).TotalMilliseconds;
                    //        Console.Out.WriteLine("{0} successes {1} errors, complete in {2}ms, speed: {3} ops/sec", responses, errors, diff, Math.Round(chuck/diff*1000, 2));
                    //    }

                    //    start = DateTime.Now;
                    //    requests = 0;
                    //    responses = 0;
                    //    errors = 0;
                    //    calls = 0;

                    //    complete = 0;
                    //    var cache = ESBClient.stringToByteArray("{\"a\":2,\"b\":3}");
                    //    for (var i = 0; i < chuck; i++)
                    //    {
                    //        requests++;
                    //        ESBClient.Invoke("/math/plus", 1, cache, (errCode, data, err) =>
                    //        {
                    //            complete++;
                    //            if (errCode == ErrorCodes.OK)
                    //            {
                    //                responses++;
                    //                //var resp = System.Text.Encoding.UTF8.GetString(data);
                    //                //Console.Out.WriteLine("2+3={0}", resp);
                    //            }
                    //            else
                    //            {
                    //                //Console.Out.WriteLine("error response: {0}", err);
                    //                errors++;
                    //            }
                    //        });
                    //    }
                    //}
                }
                Thread.Sleep(500);
            }
        }
    }
}
