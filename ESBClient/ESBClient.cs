using ProtoBuf;
using ServiceStack.Redis;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace ESBClient
{
    [ProtoContract]
    public class RegistryEntry
    {
        public enum RegistryEntryType
        {
            INVOKE_METHOD = 1
        };
        [ProtoMember(1, IsRequired = true)]
        public RegistryEntryType type { get; set; }
        [ProtoMember(2, IsRequired=true)]
        public string identifier { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public string method_guid { get; set; }
        [ProtoMember(4, IsRequired = true)]
        public string proxy_guid { get; set; }
    }
    [ProtoContract]
    public class Message
    {
        public enum Cmd { 
            ERROR = 1,
		    RESPONSE = 2,
		    ERROR_RESPONSE = 3,
		    NODE_HELLO = 4,
		    PROXY_HELLO = 5,
		    PING = 6,
		    PONG = 7,
		    INVOKE = 8,
		    REGISTER_INVOKE = 9,
		    REGISTER_INVOKE_OK = 10,
		    REGISTRY_EXCHANGE_REQUEST = 11,
		    REGISTRY_EXCHANGE_RESPONSE = 12,
		    PUBLISH = 13,
		    SUBSCRIBE = 14 
        }

        [ProtoMember(1, IsRequired = true)]
        public Cmd cmd { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string source_proxy_guid { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public byte[] payload { get; set; }
        [ProtoMember(4)]
        public string target_proxy_guid { get; set; }
        [ProtoMember(5)]
        public string identifier { get; set; }
        [ProtoMember(6)]
        public string guid_from { get; set; }
        [ProtoMember(7)]
        public string guid_to { get; set; }
        [ProtoMember(8)]
        public Int32 recursion { get; set; }
        [ProtoMember(9)]
        public UInt32 start_time { get; set; }
        [ProtoMember(10)]
        public Int32 timeout_ms { get; set; }
        [ProtoMember(11)]
        public List<RegistryEntry> reg_entry { get; set; }
    }

    public enum ErrorCodes
    {
        OK = 0,
        INTERNAL_SERVER_ERROR = 1,
        SERVICE_ERROR = 2,
        SERVICE_TIMEOUT = 3
    }

    internal class Publisher
    {
        public string connectionString { get; internal set; }
        public string hostName { get; internal set; }
        public int port { get; internal set; }
        public int traffic = 0;
        ZmqContext ctx = null;
        ZmqSocket socket = null;
        private static ConcurrentBag<byte[]> PublishBag;

        public Publisher(string _hostName, int _port)
        {
            hostName = _hostName;
            port = _port;
            PublishBag = new ConcurrentBag<byte[]>();
            connectionString = String.Format("tcp://{0}:{1}", hostName, port);
            ctx = ZmqContext.Create();
            socket = ctx.CreateSocket(SocketType.PUB);
            var str = String.Format("tcp://*:{0}", port);
            socket.Bind(str);
            socket.SendHighWatermark = 1000000;
            socket.SendBufferSize = 512 * 1024;
        }

        public void Publish(string channel, ref Message msg)
        {
            var uCBytes = Encoding.Unicode.GetBytes(channel);
            byte[] c = Encoding.Convert(Encoding.Unicode, Encoding.ASCII, uCBytes);
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, msg);
                byte[] data = ms.ToArray();
                Publish(c, data);
            }
        }

        public void Publish(byte[] channel, byte[] msg)
        {
            byte[] buf = new byte[channel.Length + 1 + msg.Length];

            Array.Copy(channel, buf, channel.Length);
            Array.Copy(ASCIIEncoding.ASCII.GetBytes("\t"), 0, buf, channel.Length, 1);
            Array.Copy(msg, 0, buf, channel.Length + 1, msg.Length);

            PublishBag.Add(buf);            
        }

        public void Flush()
        {
            byte[] buf;
            for (var i = 0; i < 100000; i++)
            {
                if (!PublishBag.TryTake(out buf))
                {
                    if (PublishBag.Count < 100)
                    {
                        return;
                    }
                    continue;
                }
                int rc = socket.Send(buf, buf.Length, SocketFlags.DontWait);
                traffic += buf.Length;
            }

        }
    }

    internal class Requester
    {
        public IPEndPoint ep { get; internal set; }
        public string guid { get; internal set; }
        public Requester(string _guid, string connectionString)
        {
            var t = connectionString.Split(':');
            ep = Parse(connectionString);
            guid = _guid;
        }

        public string GetSubscriberConnectionString()
        {
            var socket = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            socket.Connect(ep);

            var buf = new byte[1024];
            var uBytes = Encoding.Unicode.GetBytes(String.Format("{0}#{1}", ESBClient.publisherPort, ESBClient.guid));
            byte[] payload = Encoding.Convert(Encoding.Unicode, Encoding.ASCII, uBytes);
            socket.Send(payload);
            int len = socket.Receive(buf);
            var portStr = Encoding.ASCII.GetString(buf, 0, len);
            socket.Close();
            return String.Format("tcp://{0}:{1}",ep.Address, portStr);
        }

        public static IPEndPoint Parse(string endpointstring)
        {
            return Parse(endpointstring, -1);
        }

        public static IPEndPoint Parse(string endpointstring, int defaultport)
        {
            if (string.IsNullOrEmpty(endpointstring)
                || endpointstring.Trim().Length == 0)
            {
                throw new ArgumentException("Endpoint descriptor may not be empty.");
            }

            if (defaultport != -1 &&
                (defaultport < IPEndPoint.MinPort
                || defaultport > IPEndPoint.MaxPort))
            {
                throw new ArgumentException(string.Format("Invalid default port '{0}'", defaultport));
            }

            string[] values = endpointstring.Split(new char[] { ':' });
            IPAddress ipaddy;
            int port = -1;

            //check if we have an IPv6 or ports
            if (values.Length <= 2) // ipv4 or hostname
            {
                if (values.Length == 1)
                    //no port is specified, default
                    port = defaultport;
                else
                    port = getPort(values[1]);

                //try to use the address as IPv4, otherwise get hostname
                if (!IPAddress.TryParse(values[0], out ipaddy))
                    ipaddy = getIPfromHost(values[0]);
            }
            else if (values.Length > 2) //ipv6
            {
                //could [a:b:c]:d
                if (values[0].StartsWith("[") && values[values.Length - 2].EndsWith("]"))
                {
                    string ipaddressstring = string.Join(":", values.Take(values.Length - 1).ToArray());
                    ipaddy = IPAddress.Parse(ipaddressstring);
                    port = getPort(values[values.Length - 1]);
                }
                else //[a:b:c] or a:b:c
                {
                    ipaddy = IPAddress.Parse(endpointstring);
                    port = defaultport;
                }
            }
            else
            {
                throw new FormatException(string.Format("Invalid endpoint ipaddress '{0}'", endpointstring));
            }

            if (port == -1)
                throw new ArgumentException(string.Format("No port specified: '{0}'", endpointstring));

            return new IPEndPoint(ipaddy, port);
        }

        private static int getPort(string p)
        {
            int port;

            if (!int.TryParse(p, out port)
             || port < IPEndPoint.MinPort
             || port > IPEndPoint.MaxPort)
            {
                throw new FormatException(string.Format("Invalid end point port '{0}'", p));
            }

            return port;
        }

        private static IPAddress getIPfromHost(string p)
        {
            var hosts = Dns.GetHostAddresses(p);

            if (hosts == null || hosts.Length == 0)
                throw new ArgumentException(string.Format("Host not found: {0}", p));

            return hosts[0];
        }

    }

    internal class Subscriber
    {
        public string connectionString { get; internal set; }
        public string proxyGuid { get; internal set; }
        ZmqContext ctx;
        ZmqSocket socket;
        byte[] buf;
        public int traffic = 0;
        public Subscriber(string _connectionString, string _proxyGuid) 
        {
            connectionString = _connectionString;
            buf = new byte[65536];

            ctx = ZmqContext.Create();
            socket = ctx.CreateSocket(SocketType.SUB);
            var binGuid = ESBClient.stringToByteArray(ESBClient.guid);
            //socket.SubscribeAll();
            //socket.Subscribe(ASCIIEncoding.ASCII.GetBytes(""));
            socket.Subscribe(binGuid);
            socket.Connect(connectionString);
            socket.ReceiveHighWatermark = 1000000;
            socket.ReceiveBufferSize = 512 * 1024;
        }

        public Message Poll()
        {
            var size = socket.Receive(buf, SocketFlags.DontWait);
            var status = socket.ReceiveStatus;
            if (status == ReceiveStatus.TryAgain)
            {
                return null;
            }
            traffic += size;
            var start = Array.IndexOf(buf, (byte)9);
            if (start == -1) throw new Exception("Can not find the Delimiter \\t");
            start++;
            MemoryStream stream = new MemoryStream(buf, start, size-start, false);
            var respMsg = Serializer.Deserialize<Message>(stream);
            return respMsg;
        }
    }

    delegate void InternalInvokeCallback(Message msg);
    public delegate void InvokeCallback(ErrorCodes errCode, byte[] payload, string err);
    public delegate void InvokeResponderCallback(string err, byte[] data);
    public delegate void InvokeResponder(byte[] data, InvokeResponderCallback cb);

    internal class ResponseStruct
    {
        public DateTime reqTime;
        public InvokeCallback callback;
    }

    internal class LocalInvokeMethod
    {
        public string methodGuid;
        public string identifier;
        public InvokeResponder method;
    }

    public static class ESBClient
    {
        public static bool isReady { get; internal set; }
        public static bool isConnecting { get; internal set; }
        public static string guid { get; internal set; }
        public static int publisherPort { get; internal set; }
        public static string proxyGuid { get; internal set; }
        private static BlockingCollection<Message> InvokeCallBag;
        readonly static RedisClient redis = new RedisClient("esb-redis", 6379);
        static Requester requester = null;
        static Publisher publisher = null;
        static Subscriber subscriber = null;
        static ConcurrentDictionary<string, ResponseStruct> responses = null;
        static Dictionary<string, LocalInvokeMethod> localMethods = null;
        static Random random;
        static DateTime lastESBServerActiveTime;
        static ESBClient()
        {
            random = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
            lastESBServerActiveTime = DateTime.Now;
            isReady = false;
            guid = genGuid();
            Console.Out.WriteLine("new ESBClient {0}", guid);
            responses = new ConcurrentDictionary<string, ResponseStruct>();
            InvokeCallBag = new BlockingCollection<Message>(new ConcurrentBag<Message>());
            localMethods = new Dictionary<string, LocalInvokeMethod>();
            publisherPort = 7777;
            publisher = new Publisher("Arseny-PC02.Toyga.local", publisherPort);
            
            while (isConnecting || !Connect())
            {
                Thread.Sleep(250);
            }
            Console.Out.WriteLine("Connected");
            (new Thread(new ThreadStart(MainLoop))).Start();

            (new Thread(new ThreadStart(InvokeCallWorker))).Start();
            (new Thread(new ThreadStart(InvokeCallWorker))).Start();
            (new Thread(new ThreadStart(InvokeCallWorker))).Start();
            (new Thread(new ThreadStart(InvokeCallWorker))).Start();
            (new Thread(new ThreadStart(InvokeCallWorker))).Start();

        }

        static void Ping()
        {
            var cmdGuid = genGuid();
            var msgReq = new Message
            {
                cmd = Message.Cmd.PING,
                guid_from = cmdGuid,
                payload = stringToByteArray("Ping!!!"),
                source_proxy_guid = guid
            };
            var s = new ResponseStruct
            {
                callback = (errCode, data, err) =>
                {
                    if (data != null)
                    {
                        isReady = true;
                        string response = System.Text.Encoding.UTF8.GetString(data);
                    }
                    //Console.Out.WriteLine("response {0}", response);
                },
                reqTime = DateTime.Now.AddMilliseconds(1000)
            };
            //responsesMutex.WaitOne();
            while (!responses.TryAdd(cmdGuid, s))
                Thread.Sleep(1);
            //responsesMutex.ReleaseMutex();
            publisher.Publish(proxyGuid, ref msgReq);
        }

        public static string Register(string identifier, UInt32 version, InvokeResponder callback)
        {
            var methodGuid = genGuid();
            localMethods[methodGuid] = new LocalInvokeMethod
            {
                identifier = identifier+"/v"+version,
                method = callback,
                methodGuid = methodGuid
            };

            var msg = new Message {
                cmd = Message.Cmd.REGISTER_INVOKE,
                payload = stringToByteArray(methodGuid),
                source_proxy_guid = guid,
                identifier = identifier + "/v" + version
            };

            publisher.Publish(proxyGuid, ref msg);

            return methodGuid;
        }

        public static string Invoke(string identifier, UInt32 version, byte[] payload, InvokeCallback cb)
        {
            string cmdGuid = genGuid();

            var s = new ResponseStruct
            {
                callback = (errCode, data, err) =>
                {
                    //if (data != null)
                    //{
                    //    string response = System.Text.Encoding.UTF8.GetString(data);
                    //    //Console.Out.WriteLine("response data: {0}", response);
                    //}
                    //else
                    //{
                    //    Console.Out.WriteLine("response errCode:{0}, err: {1}", errCode, err);
                    //}
                    try
                    {
                        cb(errCode, data, err);
                    }
                    catch (Exception e)
                    {
                        Console.Out.WriteLine("Exception in invoke callback: {0}", e.ToString());
                    }
                },
                reqTime = DateTime.Now.AddMilliseconds(30000)
            };
            var msgReq = new Message
            {
                cmd = Message.Cmd.INVOKE,
                guid_from = cmdGuid,
                identifier = identifier + "/v" + version,
                payload = payload,
                source_proxy_guid = guid
            };

            //responsesMutex.WaitOne();
            while (!responses.TryAdd(cmdGuid, s))
                Thread.Sleep(1);
            //responsesMutex.ReleaseMutex();
            publisher.Publish(proxyGuid, ref msgReq);
            return cmdGuid;
        }

        private static void Response(Message respMsg)
        {
            try
            {
                //responsesMutex.WaitOne();
                if(!responses.ContainsKey(respMsg.guid_to))
                {
                    Console.Out.WriteLine("Requested callback not found: {0} {1}", respMsg.guid_to, respMsg);
                    //responsesMutex.ReleaseMutex();
                    return;
                }
                var resp = responses[respMsg.guid_to];
                
                var cb = resp.callback;
                ResponseStruct tmp;
                while (!responses.TryRemove(respMsg.guid_to,out tmp))
                {
                    Thread.Sleep(1);
                }
                
                //responsesMutex.ReleaseMutex();
                if (respMsg.cmd == Message.Cmd.RESPONSE)
                {
                    cb(ErrorCodes.OK, respMsg.payload, "");
                }
                else if (respMsg.cmd == Message.Cmd.ERROR)
                {
                    cb(ErrorCodes.INTERNAL_SERVER_ERROR, null, System.Text.Encoding.UTF8.GetString(respMsg.payload));
                }
                else if (respMsg.cmd == Message.Cmd.ERROR_RESPONSE)
                {
                    cb(ErrorCodes.SERVICE_ERROR, null, System.Text.Encoding.UTF8.GetString(respMsg.payload));
                }
                else
                {
                    throw new Exception("Unknown type");
                }
            }
            catch (Exception e)
            {
                Console.Out.WriteLine("Error on callback", e.ToString());
            }
        }

        private static void SendLocalMethods()
        {
            foreach (var m in localMethods)
            {
                var msg = new Message
                {
                    cmd = Message.Cmd.REGISTER_INVOKE,
                    payload = stringToByteArray(m.Key),
                    source_proxy_guid = guid,
                    identifier = m.Value.identifier
                };

                publisher.Publish(proxyGuid, ref msg);
            }
        }

        private static void InvokeCallWorker()
        {
            Message msg;
            while (true)
            {


                msg = InvokeCallBag.Take();                
                InvokeCall(msg);
                
                
            }
        }

        private static void InvokeCall(Message msg)
        {
            try
            {
                localMethods[msg.guid_to].method(msg.payload, (err, resp) => {
                    //Console.Out.WriteLine("response from local method");
                    if (err != null)
                    {
                        var errRespMsg = new Message
                        {
                            cmd = Message.Cmd.ERROR_RESPONSE,
                            source_proxy_guid = guid,
                            guid_to = msg.guid_from,
                            payload = stringToByteArray(err)
                        };
                        publisher.Publish(msg.source_proxy_guid, ref errRespMsg);
                        return;
                    }

                    var respMsg = new Message
                    {
                        cmd = Message.Cmd.RESPONSE,
                        source_proxy_guid = guid,
                        guid_to = msg.guid_from,
                        payload = resp
                    };
                    publisher.Publish(msg.source_proxy_guid, ref respMsg);
                    return;
                });
            }
            catch (Exception e)
            {
                var respMsg = new Message
                {
                    cmd = Message.Cmd.ERROR_RESPONSE,
                    source_proxy_guid = guid,
                    guid_to = msg.guid_from,
                    payload = stringToByteArray(String.Format("Exception: {0}", e.ToString()))
                };
                publisher.Publish(msg.source_proxy_guid, ref respMsg);
            }
        }

        private static void MainLoop()
        {
            int messages = 0;
            int totalMessages = 0;
            int cycles = 0;
            var now = DateTime.Now;
            var lastCleanUpTime = DateTime.Now;
            while (true)
            {
                cycles++;
                try
                {
                    var isSomethingHappen = false;
                    var msg = subscriber.Poll();
                    while (msg != null)
                    {
                        lastESBServerActiveTime = DateTime.Now;
                        isSomethingHappen = true;
                        messages++;
                        totalMessages++;
                        switch (msg.cmd)
                        {
                            case Message.Cmd.INVOKE:
                                //InvokeCall(msg);
                                InvokeCallBag.Add(msg);
                                break;
                            case Message.Cmd.RESPONSE:
                                Response(msg);
                                break;
                            case Message.Cmd.ERROR:
                                Response(msg);
                                break;
                            case Message.Cmd.ERROR_RESPONSE:
                                Response(msg);
                                break;
                            case Message.Cmd.REGISTER_INVOKE_OK:
                                break;
                            default:
                                throw new Exception("Unknown command received!");
                        }
                        msg = subscriber.Poll();
                    }

                    publisher.Flush();
                    if ((cycles % 250 == 0) && (DateTime.Now - now).TotalMilliseconds > 1000)
                    {
                        //Console.Out.WriteLine("1 sec passed, received {0} responses ({1} KB out / {2} KB in) {3} requests", messages, publisher.traffic / 1024, subscriber.traffic / 1024, requests);
                        messages = 0;
                        publisher.traffic = 0;
                        subscriber.traffic = 0;
                        now = DateTime.Now;
                        Ping();
                        SendLocalMethods();
                    }
                    if ((cycles % 250 == 0) && (DateTime.Now - lastCleanUpTime).TotalSeconds >= 30)
                    {
                        lastCleanUpTime = DateTime.Now;
                        CleanUpDeadCallbacks();
                    }

                    if ((cycles % 250 == 0) && (DateTime.Now - lastESBServerActiveTime).TotalMilliseconds >= 3000)
                    {
                        Console.Out.WriteLine("More then 3 second there is no activity from ESB server, probaly it is dead...");
                        isConnecting = false;
                        while (isConnecting || !Connect())
                        {
                            Thread.Sleep(50);
                        }
                        Console.Out.WriteLine("Connected");
                        lastESBServerActiveTime = DateTime.Now;
                    }

                    if(!isSomethingHappen) Thread.Sleep(1);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine("Exception in MainLoop: {0}", e.ToString());
                    Thread.Sleep(250);
                }
            }
        }

        private static void CleanUpDeadCallbacks()
        {
            //responsesMutex.WaitOne();
            var l = new List<string>();
            var now = DateTime.Now;
            foreach (var g in responses)
            {
                if (now > g.Value.reqTime)
                {
                    l.Add(g.Key);
                }
            }
            if (l.Count > 0)
            {
                foreach (var g in l)
                {
                    var cb = responses[g].callback;
                    try
                    {
                        cb(ErrorCodes.SERVICE_TIMEOUT, null, "Timeout on service call");
                    } catch(Exception e)
                    {
                        Console.Out.WriteLine("Error while executing callback: {0}", e.ToString());
                    }
                    ResponseStruct tmp;
                    while (!responses.TryRemove(g,out tmp))
                    {
                        Thread.Sleep(1);
                    }
                }
                Console.Out.WriteLine("Removed {0} dead callbacks", l.Count);
            }
            //responsesMutex.ReleaseMutex();
        }

        private static bool Connect()
        {
            if (isConnecting) return false;
            isConnecting = true;
            try
            {
                var ret = redis.ZRevRange("ZSET:PROXIES", 0, 0);
                if (ret.Length < 1)
                {
                    return false;
                }
                string buf = System.Text.Encoding.UTF8.GetString(ret[0]);
                var r = buf.Split('#');
                string _proxyGuid = r[0];
                proxyGuid = _proxyGuid;
                string connectionString = r[1];
                requester = new Requester(guid, connectionString);
                var sub = requester.GetSubscriberConnectionString();
                if (sub == String.Empty) return false;
                subscriber = new Subscriber(sub, _proxyGuid);
                isConnecting = false;
                return true;
            }
            catch (System.Exception e)
            {
                Console.Out.WriteLine("Esception on ESBClient.Connect(): {0}", e.ToString());
                isConnecting = false;
                return false;
            }
        }

        public static byte[] stringToByteArray(string str) 
        {
            var uBytes = Encoding.Unicode.GetBytes(str);
            byte[] bytes = Encoding.Convert(Encoding.Unicode, Encoding.ASCII, uBytes);
            return bytes;
        }

        public static string genGuid()
        {
            //var g = Guid.NewGuid();
            //return g.ToString().Replace("-", string.Empty).ToUpper().Substring(0, 16);
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var stringChars = new char[16];

            for (int i = 0; i < stringChars.Length; i++)
            {
                stringChars[i] = chars[random.Next(1000000) % chars.Length];
            }

            return new String(stringChars);
        }
    }
}
