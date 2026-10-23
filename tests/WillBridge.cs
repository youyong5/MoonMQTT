using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

// Test-only TCP bridge. Closing both sides simulates a network failure while
// leaving the broker and the automatic MoonBit client processes alive.
public sealed class WillBridge : IDisposable
{
    private readonly TcpListener listener;
    private readonly string upstreamHost;
    private readonly int upstreamPort;
    private readonly object gate = new object();
    private Thread acceptThread;
    private volatile bool running;
    private TcpClient downstream;
    private TcpClient upstream;

    public WillBridge(int listenPort, string upstreamHost, int upstreamPort)
    {
        listener = new TcpListener(IPAddress.Loopback, listenPort);
        this.upstreamHost = upstreamHost;
        this.upstreamPort = upstreamPort;
    }

    public void Start()
    {
        listener.Start();
        running = true;
        acceptThread = new Thread(AcceptLoop) { IsBackground = true };
        acceptThread.Start();
    }

    private void AcceptLoop()
    {
        while (running)
        {
            TcpClient client = null;
            TcpClient broker = null;
            try
            {
                client = listener.AcceptTcpClient();
                broker = new TcpClient(upstreamHost, upstreamPort);
                lock (gate)
                {
                    downstream = client;
                    upstream = broker;
                }
                TcpClient left = client;
                TcpClient right = broker;
                new Thread(() => Pump(left, right)) { IsBackground = true }.Start();
                new Thread(() => Pump(right, left)) { IsBackground = true }.Start();
            }
            catch (SocketException)
            {
                if (client != null) client.Close();
                if (broker != null) broker.Close();
            }
            catch (ObjectDisposedException)
            {
                if (client != null) client.Close();
                if (broker != null) broker.Close();
            }
        }
    }

    private static void Pump(TcpClient source, TcpClient target)
    {
        try
        {
            byte[] buffer = new byte[8192];
            NetworkStream input = source.GetStream();
            NetworkStream output = target.GetStream();
            int count;
            while ((count = input.Read(buffer, 0, buffer.Length)) > 0)
            {
                output.Write(buffer, 0, count);
            }
        }
        catch (IOException) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            source.Close();
            target.Close();
        }
    }

    public void Drop()
    {
        lock (gate)
        {
            if (downstream == null || upstream == null)
                throw new InvalidOperationException("No active bridge connection to drop");
            try { downstream.Client.Shutdown(SocketShutdown.Both); }
            catch (SocketException) { }
            try { upstream.Client.Shutdown(SocketShutdown.Both); }
            catch (SocketException) { }
            downstream.Close();
            upstream.Close();
            downstream = null;
            upstream = null;
        }
    }

    public void Dispose()
    {
        running = false;
        listener.Stop();
        lock (gate)
        {
            if (downstream != null) downstream.Close();
            if (upstream != null) upstream.Close();
        }
        if (acceptThread != null) acceptThread.Join(1000);
    }
}
