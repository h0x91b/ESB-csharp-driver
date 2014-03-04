using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ESBClient;

namespace ESBClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Out.WriteLine("ESBClient start");

            var calls = 0;
            ESBClient.Register("/math/plus", 1, (data, cb) =>
            {
                calls++;
                var payload = System.Text.Encoding.UTF8.GetString(data);
                //Console.Out.WriteLine("method math plus invoked");
                cb(null, ESBClient.stringToByteArray("5"));
            });

            var requests = 0;
            var responses = 0;
            var now = DateTime.Now;
            while (true)
            {
                if (ESBClient.isReady)
                {
                    for (var i = 0; i < 15000; i++)
                    {
                        requests++;
                        ESBClient.Invoke("/math/plus", 1, ESBClient.stringToByteArray("{\"a\":2,\"b\":3}"), (errCode, data, err) =>
                        {
                            responses++;
                            if (errCode == ErrorCodes.OK)
                            {
                                var resp = System.Text.Encoding.UTF8.GetString(data);
                                //Console.Out.WriteLine("2+3={0}", resp);
                            }
                            else
                            {
                                Console.Out.WriteLine("error response: {0}", err);
                            }
                        });
                    }
                    if ((DateTime.Now - now).TotalMilliseconds >= 1000)
                    {
                        now = DateTime.Now;
                        Console.Out.WriteLine("{0} requests and {1} responses and {2} calls per second", requests, responses, calls);
                        requests = 0;
                        responses = 0;
                        calls = 0;
                    }
                }
                Thread.Sleep(500);
            }
        }
    }
}
