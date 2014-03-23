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

namespace ESB
{
    [ProtoContract]
    public class RegistryEntry
    {
        public enum RegistryEntryType
        {
            INVOKE_METHOD = 1,
            QUEUE = 2
        };
        [ProtoMember(1, IsRequired = true)]
        public RegistryEntryType type { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string identifier { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public string method_guid { get; set; }
        [ProtoMember(4, IsRequired = true)]
        public string proxy_guid { get; set; }
        [ProtoMember(5, IsRequired = false)]
        public string queue_name { get; set; }
    }
    [ProtoContract]
    public class Message
    {
        public enum Cmd
        {
            ERROR = 1,
            RESPONSE = 2,
            ERROR_RESPONSE = 3,
            NODE_HELLO = 4,
            PING = 5,
            PONG = 6,
            INVOKE = 7,
            REGISTER_INVOKE = 8,
            REGISTER_INVOKE_OK = 9,
            REGISTRY_EXCHANGE_REQUEST = 10,
            REGISTRY_EXCHANGE_RESPONSE = 11,
            PUBLISH = 12,
            SUBSCRIBE = 13,
            REG_QUEUE = 14,
            UNREG_QUEUE = 15,
            PEEK_QUEUE = 16,
            DEQUEUE_QUEUE = 17,
            LEN_QUEUE = 18
        }

        [ProtoMember(1, IsRequired = true)]
        public Cmd cmd { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string source_component_guid { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public byte[] payload { get; set; }
        [ProtoMember(4)]
        public string target_component_guid { get; set; }
        [ProtoMember(5)]
        public string identifier { get; set; }
        [ProtoMember(6)]
        public string source_operation_guid { get; set; }
        [ProtoMember(7)]
        public string target_operation_guid { get; set; }
        [ProtoMember(8)]
        public Int32 recursion { get; set; }
        [ProtoMember(9)]
        public List<RegistryEntry> reg_entry { get; set; }
    }

    public enum ErrorCodes
    {
        OK = 0,
        INTERNAL_SERVER_ERROR = 1,
        SERVICE_ERROR = 2,
        SERVICE_TIMEOUT = 3
    }

    internal class Publisher : IDisposable
    {
        public string connectionString { get; internal set; }
        public string hostName { get; internal set; }
        public int port { get; internal set; }
        public int traffic = 0;
        ZmqContext ctx = null;
        ZmqSocket socket = null;
        private ConcurrentBag<byte[]> PublishBag;

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
            socket.SendHighWatermark = 100000;
            socket.Backlog = 100000;
            socket.SendBufferSize = 128 * 1024;
        }

        public void Dispose()
        {
            socket.Close();
            socket.Dispose();
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
        int publisherPort;
        public Requester(string _guid, string connectionString, int _publisherPort)
        {
            var t = connectionString.Split(':');
            ep = Parse(connectionString);
            guid = _guid;
            publisherPort = _publisherPort;
        }

        public string GetSubscriberConnectionString()
        {
            var socket = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            socket.Connect(ep);

            var buf = new byte[1024];
            var uBytes = Encoding.Unicode.GetBytes(String.Format("{0}#{1}", publisherPort, guid));
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

    internal class Subscriber : IDisposable
    {
        public string connectionString { get; internal set; }
        public string proxyGuid { get; internal set; }
        ZmqContext ctx;
        ZmqSocket socket;
        byte[] buf;
        public int traffic = 0;
        List<string> channels;
        public Subscriber(string _connectionString, string _proxyGuid, string ESBClientGuid) 
        {
            connectionString = _connectionString;
            buf = new byte[65536];

            ctx = ZmqContext.Create();
            socket = ctx.CreateSocket(SocketType.SUB);
            var binGuid = ESBClient.stringToByteArray(ESBClientGuid);
            channels = new List<string>();
            //socket.SubscribeAll();
            //socket.Subscribe(ASCIIEncoding.ASCII.GetBytes(""));
            socket.Subscribe(binGuid);
            socket.Connect(connectionString);
            socket.ReceiveHighWatermark = 100000;
            socket.ReceiveBufferSize = 128 * 1024;
        }

        public void Dispose()
        {
            socket.Close();
            socket.Dispose();
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

        public void Subscribe(string channel)
        {
            if (channels.Contains(channel)) return;
            channels.Add(channel);
            socket.Subscribe(ESBClient.stringToByteArray(channel+"\t"));
        }
    }

    delegate void InternalInvokeCallback(Message msg);
    public delegate void InvokeCallback(ErrorCodes errCode, byte[] payload, string err);
    public delegate void InvokeResponderCallback(string err, byte[] data);
    public delegate void InvokeResponder(Dictionary<string, object> ht, InvokeResponderCallback cb);
    public delegate void SubscribeCallback(string channel, byte[] data);

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

    public class ESBOptions
    {
       public int redisPort;
       public int publisherPort;
       public int maxInactiveTimeInMsBeforeReconnect;
    }

    public class ESBClient : IDisposable
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public bool isReady { get; internal set; }
        public bool isConnecting { get; internal set; }
        public string guid { get; internal set; }
        public int publisherPort { get; internal set; }
        public int maxInactiveTimeInMsBeforeReconnect { get; internal set; }
        public string proxyGuid { get; internal set; }
        public string registryRedisAddr { get; internal set; }
        private BlockingCollection<Message> InvokeCallBag;
        RedisClient redis;
        Requester requester = null;
        Publisher publisher = null;
        Subscriber subscriber = null;
        ConcurrentDictionary<string, ResponseStruct> responses = null;
        Dictionary<string, LocalInvokeMethod> localMethods = null;
        Dictionary<string, Dictionary<string, SubscribeCallback>> subscribeCallbacks;
        List<Thread> workerList;
        static Random random;
        DateTime lastESBServerActiveTime;
        List<string> channels;
        bool isWork;
        static ESBClient()
        {
            random = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
        }

        public ESBClient()
        {
            Init("plt-esbredis01.toyga.local", new ESBOptions {
                redisPort = 6379,
                publisherPort = random.Next(7000, 8000),
                maxInactiveTimeInMsBeforeReconnect = 15000
            });
        }

        public ESBClient(string _registryRedisAddr)
        {
            Init(_registryRedisAddr, new ESBOptions
            {
                redisPort = 6379,
                publisherPort = random.Next(7000, 8000),
                maxInactiveTimeInMsBeforeReconnect = 15000
            });
        }

        public ESBClient(string _registryRedisAddr, ESBOptions options)
        {
            options.redisPort = options.redisPort > 0 ? options.redisPort : 6379;
            options.publisherPort = options.redisPort > 0 ? options.publisherPort : random.Next(7000, 8000);
            options.maxInactiveTimeInMsBeforeReconnect = options.maxInactiveTimeInMsBeforeReconnect > 0 ? options.maxInactiveTimeInMsBeforeReconnect : 15000;
            Init(_registryRedisAddr, options);
        }

        void Init(string _registryRedisAddr, ESBOptions options)
        {
            lastESBServerActiveTime = DateTime.Now;
            isReady = false;
            guid = genGuid();
            log.InfoFormat("new ESBClient {0}", guid);
            registryRedisAddr = _registryRedisAddr;
            redis = new RedisClient(registryRedisAddr, options.redisPort);
            responses = new ConcurrentDictionary<string, ResponseStruct>();
            InvokeCallBag = new BlockingCollection<Message>(new ConcurrentBag<Message>());
            localMethods = new Dictionary<string, LocalInvokeMethod>();
            subscribeCallbacks = new Dictionary<string, Dictionary<string, SubscribeCallback>>();
            channels = new List<string>();
            publisherPort = options.publisherPort;
            publisher = new Publisher(GetFQDN(), publisherPort);
            isWork = true;
            maxInactiveTimeInMsBeforeReconnect = options.maxInactiveTimeInMsBeforeReconnect;

            while (isConnecting || !Connect())
            {
                Thread.Sleep(250);
            }
            isReady = true;
            log.InfoFormat("Connected");
            (new Thread(new ThreadStart(MainLoop))).Start();

            var cpus = Environment.ProcessorCount;
            var workers = cpus * 1;
            log.InfoFormat("Machine have {0} cores, using {1} workers", cpus, workers);
            workerList = new List<Thread>();
            for (var i = 0; i < workers; i++)
            {
                var t = new Thread(new ThreadStart(InvokeCallWorker));
                workerList.Add(t);
                t.Start();
            }
        }

        public void Dispose()
        {
            log.InfoFormat("ESBClient {0} dispose", guid);
            isWork = false;
            foreach (var w in workerList)
            {
                w.Abort();
            }
            publisher.Dispose();
            publisher = null;
            subscriber.Dispose();
            subscriber = null;
            redis.Dispose();
            redis = null;
        }


        void GotPublish(Message msg)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Got publish on channel {0}", msg.identifier);
            var channel = msg.identifier;
            if (!subscribeCallbacks.ContainsKey(channel) || subscribeCallbacks[channel].Count < 1)
            {
                log.ErrorFormat("We are get message on channel `{0}` but no listeners here!", channel);
                return;
            }

            var dict = subscribeCallbacks[channel];

            foreach (var c in dict)
            {
                try
                {
                    c.Value(channel, msg.payload);
                }
                catch (Exception e)
                {
                    log.ErrorFormat("Exception on subscriber callback: {0}", e);
                }
            }
        }

        public string Subscribe(string channel, SubscribeCallback cb)
        {
            subscriber.Subscribe(channel);

            var callbackGuid = genGuid();

            if (!subscribeCallbacks.ContainsKey(channel))
                subscribeCallbacks[channel] = new Dictionary<string, SubscribeCallback>();

            subscribeCallbacks[channel][callbackGuid] = cb;

            if (channels.Contains(channel)) return callbackGuid;
            
            channels.Add(channel);
            var msg = new Message
            {
                cmd = Message.Cmd.SUBSCRIBE,
                source_component_guid = guid,
                identifier = channel,
                payload = stringToByteArray(".")
            };

            publisher.Publish(proxyGuid, ref msg);
            return callbackGuid;
        }

        void Ping()
        {
            var msgReq = new Message
            {
                cmd = Message.Cmd.PING,
                source_operation_guid = "",
                payload = stringToByteArray("Ping!!!"),
                source_component_guid = guid
            };
            publisher.Publish(proxyGuid, ref msgReq);
        }

        public string Register(string identifier, UInt32 version, InvokeResponder callback)
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
                source_component_guid = guid,
                identifier = identifier + "/v" + version
            };

            publisher.Publish(proxyGuid, ref msg);

            return methodGuid;
        }

        public void Publish(string channel, byte[] payload)
        {
            var msg = new Message
            {
                cmd = Message.Cmd.PUBLISH,
                payload = payload,
                source_component_guid = guid,
                identifier = channel
            };

            publisher.Publish(channel, ref msg);
        }

        public string Invoke(string identifier, UInt32 version, byte[] payload, InvokeCallback cb)
        {
            return Invoke(identifier, version, payload, cb, 30000);
        }

        public string Invoke(string identifier, UInt32 version, byte[] payload, InvokeCallback cb, int timeoutMs)
        {
            if (isConnecting || !isReady)
            {
                try
                {
                    cb(ErrorCodes.INTERNAL_SERVER_ERROR, null, "ESB client not connected");
                }
                catch (Exception e)
                {
                    log.ErrorFormat("Exception in invoke callback: {0}", e.ToString());
                }
                return string.Empty;
            }

            string cmdGuid = genGuid();

            var s = new ResponseStruct
            {
                callback = (errCode, data, err) =>
                {
                    try
                    {
                        cb(errCode, data, err);
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("Exception in invoke callback: {0}", e.ToString());
                    }
                },
                reqTime = DateTime.Now.AddMilliseconds(timeoutMs)
            };
            var msgReq = new Message
            {
                cmd = Message.Cmd.INVOKE,
                source_operation_guid = cmdGuid,
                identifier = identifier + "/v" + version,
                payload = payload,
                source_component_guid = guid
            };

            //responsesMutex.WaitOne();
            while (!responses.TryAdd(cmdGuid, s))
                Thread.Sleep(1);
            //responsesMutex.ReleaseMutex();
            publisher.Publish(proxyGuid, ref msgReq);
            return cmdGuid;
        }

        private void Response(Message respMsg)
        {
            try
            {
                //responsesMutex.WaitOne();
                if(!responses.ContainsKey(respMsg.target_operation_guid))
                {
                    log.WarnFormat("Requested callback not found: {0} {1}", respMsg.target_operation_guid, respMsg);
                    //responsesMutex.ReleaseMutex();
                    return;
                }
                var resp = responses[respMsg.target_operation_guid];
                
                var cb = resp.callback;
                ResponseStruct tmp;
                while (!responses.TryRemove(respMsg.target_operation_guid,out tmp))
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
                log.ErrorFormat("Error on callback", e.ToString());
            }
        }

        private void SendLocalMethods()
        {
            foreach (var m in localMethods)
            {
                var msg = new Message
                {
                    cmd = Message.Cmd.REGISTER_INVOKE,
                    payload = stringToByteArray(m.Key),
                    source_component_guid = guid,
                    identifier = m.Value.identifier
                };

                publisher.Publish(proxyGuid, ref msg);
            }
        }

        private void InvokeCallWorker()
        {
            Message msg;
            var calls = 0;
            Thread.CurrentThread.Name = "Invoke Worker";
            while (isWork)
            {
                msg = InvokeCallBag.Take();
                InvokeCall(msg);
                calls++;
            }
        }

        private void InvokeCall(Message msg)
        {
            try
            {
                var payload = System.Text.Encoding.UTF8.GetString(msg.payload);
                var ser = new ServiceStack.Text.JsonSerializer<Dictionary<string, object>>();
                var ht = ser.DeserializeFromString(payload);
                //var ht = JsonConvert.DeserializeObject<Hashtable>(payload, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Include, FloatParseHandling = FloatParseHandling.Decimal });
                localMethods[msg.target_operation_guid].method(ht, (err, resp) => {
                    if (err != null)
                    {
                        var errRespMsg = new Message
                        {
                            cmd = Message.Cmd.ERROR_RESPONSE,
                            source_component_guid = guid,
                            target_operation_guid = msg.source_operation_guid,
                            payload = stringToByteArray(err)
                        };
                        publisher.Publish(msg.source_component_guid, ref errRespMsg);
                        return;
                    }

                    var respMsg = new Message
                    {
                        cmd = Message.Cmd.RESPONSE,
                        source_component_guid = guid,
                        target_operation_guid = msg.source_operation_guid,
                        payload = resp
                    };
                    publisher.Publish(msg.source_component_guid, ref respMsg);
                    return;
                });
            }
            catch (Exception e)
            {
                var respMsg = new Message
                {
                    cmd = Message.Cmd.ERROR_RESPONSE,
                    source_component_guid = guid,
                    target_operation_guid = msg.source_operation_guid,
                    payload = stringToByteArray(String.Format("Exception: {0}", e.ToString()))
                };
                publisher.Publish(msg.source_component_guid, ref respMsg);
            }
        }

        private void MainLoop()
        {
            int messages = 0;
            int totalMessages = 0;
            int cycles = 0;
            var now = DateTime.Now;
            var isSomethingHappen = false;
            while (isWork)
            {
                cycles++;
                try
                {
                    isSomethingHappen = false;
                    var msg = subscriber.Poll();
                    while (msg != null && isWork)
                    {
                        lastESBServerActiveTime = DateTime.Now;
                        isSomethingHappen = true;
                        messages++;
                        totalMessages++;
                        switch (msg.cmd)
                        {
                            case Message.Cmd.INVOKE:
                                InvokeCallBag.Add(msg);
                                break;
                            case Message.Cmd.PING:
                                Pong(msg);
                                break;
                            case Message.Cmd.PONG:
                                lastESBServerActiveTime = DateTime.Now;
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
                            case Message.Cmd.PUBLISH:
                                GotPublish(msg);
                                break;
                            default:
                                throw new Exception("Unknown command received!");
                        }
                        msg = subscriber.Poll();
                    }

                    publisher.Flush();
                    if ((DateTime.Now - now).TotalMilliseconds > 1000)
                    {
                        messages = 0;
                        publisher.traffic = 0;
                        subscriber.traffic = 0;
                        now = DateTime.Now;
                        Ping();
                        SendLocalMethods();
                    }

                    if ((DateTime.Now - lastESBServerActiveTime).TotalMilliseconds >= maxInactiveTimeInMsBeforeReconnect)
                    {
                        log.ErrorFormat("More then {0} second there is no activity from ESB server, probaly it is dead...", maxInactiveTimeInMsBeforeReconnect);
                        isReady = false;
                        FlushAllCallbacks();
                        isConnecting = false;
                        while (isConnecting || !Connect())
                        {
                            Thread.Sleep(50);
                        }
                        isReady = true;
                        log.InfoFormat("Connected");
                        lastESBServerActiveTime = DateTime.Now;
                    }

                    CleanUpDeadCallbacks();
                    if(!isSomethingHappen)
                        Thread.Sleep(1);
                }
                catch (Exception e)
                {
                    log.ErrorFormat("Exception in MainLoop: {0}", e.ToString());
                    Thread.Sleep(250);
                }
            }
        }

        private void Pong(Message cmdReq)
        {
            var respMsg = new Message
            {
                cmd = Message.Cmd.PONG,
                payload = stringToByteArray("\"pong!\""), //this is JSON encoded string
                source_component_guid = guid,
                target_operation_guid = cmdReq.source_operation_guid
            };
            publisher.Publish(cmdReq.source_component_guid, ref respMsg);
        }

        private void CleanUpDeadCallbacks()
        {
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
                        log.ErrorFormat("Error while executing callback: {0}", e.ToString());
                    }
                    ResponseStruct tmp;
                    while (!responses.TryRemove(g,out tmp))
                    {
                        Thread.Sleep(1);
                    }
                }
                log.WarnFormat("Removed {0} dead callbacks", l.Count);
            }
        }

        private void FlushAllCallbacks()
        {
            foreach (var g in responses.Keys)
            {
                var cb = responses[g].callback;
                try
                {
                    cb(ErrorCodes.INTERNAL_SERVER_ERROR, null, "Connection to server have been lost");
                }
                catch (Exception e)
                {
                    log.ErrorFormat("Error while executing callback: {0}", e.ToString());
                }
                ResponseStruct tmp;
                while (!responses.TryRemove(g, out tmp))
                {
                    Thread.Sleep(1);
                }
            }
        }

        private bool Connect()
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
                requester = new Requester(guid, connectionString, publisherPort);
                var sub = requester.GetSubscriberConnectionString();
                if (sub == String.Empty) return false;
                subscriber = new Subscriber(sub, _proxyGuid, guid);
                isConnecting = false;

                foreach (var c in channels)
                {
                    subscriber.Subscribe(c);
                }

                return true;
            }
            catch (System.Exception e)
            {
                log.ErrorFormat("Exception on ESBClient.Connect(): {0}", e.ToString());
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

        public static string byteArrayToString(byte[] data) 
        {
            return System.Text.Encoding.UTF8.GetString(data);
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

        public static string GetFQDN()
        {
            string domainName = System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().DomainName;
            string hostName = Dns.GetHostName();
            string fqdn = "";
            if (!hostName.Contains(domainName))
                fqdn = hostName + "." + domainName;
            else
                fqdn = hostName;

            log.DebugFormat("GetFQDN returns - `{0}`", fqdn);

            return fqdn;
        }
    }
}
